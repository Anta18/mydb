// tx/lock_manager.rs

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Mutex,
};

/// A transaction identifier.
pub type TxId = u64;
/// A resource to lock (e.g. table name, page number).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Resource {
    Table(String),
    Page(u64),
    // add Row((String, RID)) if row-level locking desired
}

/// Lock modes.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum LockMode {
    Shared,    // S
    Exclusive, // X
}

/// A lock request by a transaction.
#[derive(Debug)]
struct LockRequest {
    tx: TxId,
    mode: LockMode,
    /// notified when granted
    waker: tokio::sync::oneshot::Sender<()>,
}

/// The lock state for one resource.
#[derive(Debug)]
struct LockState {
    /// currently granted locks (tx, mode)
    holders: Vec<(TxId, LockMode)>,
    /// queue of waiting requests
    queue: VecDeque<LockRequest>,
}

impl LockState {
    fn new() -> Self {
        LockState {
            holders: Vec::new(),
            queue: VecDeque::new(),
        }
    }

    /// Can `req` be granted immediately?
    fn can_grant(&self, req: &LockRequest) -> bool {
        if self.holders.is_empty() {
            return true;
        }
        match req.mode {
            LockMode::Shared => {
                // shared can coexist with other shared, but not with X
                self.holders.iter().all(|&(_, m)| m == LockMode::Shared)
            }
            LockMode::Exclusive => false, // any existing holder blocks X
        }
    }
}

/// Central lock manager with global lock table.
pub struct LockManager {
    /// Map resource → lock state
    table: Mutex<HashMap<Resource, LockState>>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            table: Mutex::new(HashMap::new()),
        }
    }

    /// Acquire a lock in the given mode on a resource for a transaction.
    /// Returns when the lock is granted.
    pub async fn lock(&self, tx: TxId, res: Resource, mode: LockMode) -> anyhow::Result<()> {
        // Create one-shot channel to await grant
        let (tx_wake, rx_wake) = tokio::sync::oneshot::channel();

        // Enqueue or grant immediately - CRITICAL: Drop the MutexGuard before await
        let should_wait = {
            let mut tbl = self.table.lock().unwrap();
            let state = tbl.entry(res.clone()).or_insert_with(LockState::new);

            let req = LockRequest {
                tx,
                mode,
                waker: tx_wake,
            };

            if state.can_grant(&req) {
                // grant immediately
                state.holders.push((tx, mode));
                // notify immediately (drop sender)
                // but channel needs to fire, so send
                let _ = req.waker.send(());
                false // don't need to wait
            } else {
                // enqueue
                state.queue.push_back(req);
                true // need to wait
            }
        }; // MutexGuard is dropped here!

        // Only await if we need to wait - this ensures no MutexGuard across await
        if should_wait {
            let _ = rx_wake.await;
        }

        Ok(())
    }

    /// Release all locks held by `tx` on any resource.
    /// Also wakes up queue for each resource to grant next waiting transactions.
    pub fn unlock_all(&self, tx: TxId) {
        let mut tbl = self.table.lock().unwrap();
        let resources: Vec<_> = tbl.keys().cloned().collect();

        for res in resources {
            if let Some(state) = tbl.get_mut(&res) {
                // remove from holders
                state.holders.retain(|&(holder_tx, _)| holder_tx != tx);

                // scan queue, grant in order if possible
                let mut to_wake = Vec::new();
                let i = 0;
                while i < state.queue.len() {
                    // check if head can be granted now
                    if state.holders.is_empty()
                        || (state.queue[i].mode == LockMode::Shared
                            && state.holders.iter().all(|&(_, m)| m == LockMode::Shared))
                    {
                        let req = state.queue.remove(i).unwrap();
                        state.holders.push((req.tx, req.mode));
                        to_wake.push(req.waker);
                        // if an exclusive lock was granted, stop granting further
                        if req.mode == LockMode::Exclusive {
                            break;
                        }
                        // continue to next queue entry
                    } else {
                        // cannot grant head; since queued, later ones also blocked
                        break;
                    }
                }

                // Wake up outside the table lock to avoid holding lock during async operations
                if !to_wake.is_empty() {
                    drop(tbl);
                    for w in to_wake {
                        let _ = w.send(());
                    }
                    tbl = self.table.lock().unwrap();
                }
            }
        }
    }

    /// Build a wait‐for graph and detect cycles → simple deadlock detection.
    /// Returns Some(cycle) if a deadlock is detected, where `cycle` is the list of TxIds in the cycle.
    pub fn detect_deadlock(&self) -> Option<Vec<TxId>> {
        let tbl = self.table.lock().unwrap();
        // build adjacency: waiting_tx → holder_tx
        let mut graph: HashMap<TxId, HashSet<TxId>> = HashMap::new();
        for (res, state) in tbl.iter() {
            for req in state.queue.iter() {
                let waiting = req.tx;
                let holders: Vec<_> = state.holders.iter().map(|&(t, _)| t).collect();
                let entry = graph.entry(waiting).or_default();
                for h in holders {
                    entry.insert(h);
                }
            }
        }
        drop(tbl);

        // detect cycle with DFS
        let mut visited = HashSet::new();
        let mut stack = Vec::new();
        let mut on_stack = HashSet::new();

        fn dfs(
            u: TxId,
            graph: &HashMap<TxId, HashSet<TxId>>,
            visited: &mut HashSet<TxId>,
            on_stack: &mut HashSet<TxId>,
            stack: &mut Vec<TxId>,
        ) -> Option<Vec<TxId>> {
            visited.insert(u);
            on_stack.insert(u);
            stack.push(u);
            if let Some(neighs) = graph.get(&u) {
                for &v in neighs {
                    if !visited.contains(&v) {
                        if let Some(cycle) = dfs(v, graph, visited, on_stack, stack) {
                            return Some(cycle);
                        }
                    } else if on_stack.contains(&v) {
                        // found a cycle: collect from v..end
                        let idx = stack.iter().position(|&x| x == v).unwrap();
                        return Some(stack[idx..].to_vec());
                    }
                }
            }
            on_stack.remove(&u);
            stack.pop();
            None
        }

        for &u in graph.keys() {
            if !visited.contains(&u) {
                if let Some(cycle) = dfs(u, &graph, &mut visited, &mut on_stack, &mut stack) {
                    return Some(cycle);
                }
            }
        }
        None
    }
}
