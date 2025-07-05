

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Mutex,
};


pub type TxId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Resource {
    Table(String),
    Page(u64),
    
}


#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum LockMode {
    Shared,    
    Exclusive, 
}


#[derive(Debug)]
struct LockRequest {
    tx: TxId,
    mode: LockMode,
    
    waker: tokio::sync::oneshot::Sender<()>,
}


#[derive(Debug)]
struct LockState {
    
    holders: Vec<(TxId, LockMode)>,
    
    queue: VecDeque<LockRequest>,
}

impl LockState {
    fn new() -> Self {
        LockState {
            holders: Vec::new(),
            queue: VecDeque::new(),
        }
    }

    
    fn can_grant(&self, req: &LockRequest) -> bool {
        if self.holders.is_empty() {
            return true;
        }
        match req.mode {
            LockMode::Shared => {
                
                self.holders.iter().all(|&(_, m)| m == LockMode::Shared)
            }
            LockMode::Exclusive => false, 
        }
    }
}


pub struct LockManager {
    
    table: Mutex<HashMap<Resource, LockState>>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            table: Mutex::new(HashMap::new()),
        }
    }

    
    
    pub async fn lock(&self, tx: TxId, res: Resource, mode: LockMode) -> anyhow::Result<()> {
        
        let (tx_wake, rx_wake) = tokio::sync::oneshot::channel();

        
        let should_wait = {
            let mut tbl = self.table.lock().unwrap();
            let state = tbl.entry(res.clone()).or_insert_with(LockState::new);

            let req = LockRequest {
                tx,
                mode,
                waker: tx_wake,
            };

            if state.can_grant(&req) {
                
                state.holders.push((tx, mode));
                
                
                let _ = req.waker.send(());
                false 
            } else {
                
                state.queue.push_back(req);
                true 
            }
        }; 

        
        if should_wait {
            let _ = rx_wake.await;
        }

        Ok(())
    }

    
    
    pub fn unlock_all(&self, tx: TxId) {
        let mut tbl = self.table.lock().unwrap();
        let resources: Vec<_> = tbl.keys().cloned().collect();

        for res in resources {
            if let Some(state) = tbl.get_mut(&res) {
                
                state.holders.retain(|&(holder_tx, _)| holder_tx != tx);

                
                let mut to_wake = Vec::new();
                let i = 0;
                while i < state.queue.len() {
                    
                    if state.holders.is_empty()
                        || (state.queue[i].mode == LockMode::Shared
                            && state.holders.iter().all(|&(_, m)| m == LockMode::Shared))
                    {
                        let req = state.queue.remove(i).unwrap();
                        state.holders.push((req.tx, req.mode));
                        to_wake.push(req.waker);
                        
                        if req.mode == LockMode::Exclusive {
                            break;
                        }
                        
                    } else {
                        
                        break;
                    }
                }

                
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

    
    
    pub fn detect_deadlock(&self) -> Option<Vec<TxId>> {
        let tbl = self.table.lock().unwrap();
        
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
