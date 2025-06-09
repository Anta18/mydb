// src/query/executor.rs

use crate::index::bplustree::BPlusTree;
use crate::index::node_serializer::NodeType;
use crate::query::binder::{BoundExpr, Value};
use crate::query::physical_planner::PhysicalPlan;
use crate::storage::storage::Storage;
use anyhow::{Result, anyhow};
use std::collections::VecDeque;

pub type Tuple = Vec<Value>;

/// The iterator interface for all physical operators.
pub trait PhysicalOp {
    /// Prepare any state (e.g. open files, build hash tables).
    fn open(&mut self) -> Result<()>;
    /// Return the next tuple, or `None` if done.
    fn next(&mut self) -> Result<Option<Tuple>>;
    /// Clean up resources.
    fn close(&mut self) -> Result<()>;
}

/// Executor: materializes all tuples from a plan.
pub struct Executor<'a> {
    root: Box<dyn PhysicalOp + 'a>,
}

impl<'a> Executor<'a> {
    pub fn new(root: Box<dyn PhysicalOp + 'a>) -> Self {
        Executor { root }
    }

    /// Run the plan to completion, collecting all tuples.
    pub fn execute(&mut self) -> Result<Vec<Tuple>> {
        self.root.open()?;
        let mut rows = Vec::new();
        while let Some(row) = self.root.next()? {
            rows.push(row);
        }
        self.root.close()?;
        Ok(rows)
    }
}

////////////////////////////////////////////////////////////////////////////////
// SeqScan + Filter + Projection implementations
////////////////////////////////////////////////////////////////////////////////

/// Full table scan: reads all RIDs from storage and fetches tuples.
pub struct SeqScanOp<'a> {
    storage: &'a mut Storage,
    table: String,
    predicate: Option<BoundExpr>,
    // queue of pending RIDs
    rids: VecDeque<(u64, u16)>,
}

impl<'a> SeqScanOp<'a> {
    pub fn new(storage: &'a mut Storage, table: String, predicate: Option<BoundExpr>) -> Self {
        SeqScanOp {
            storage,
            table,
            predicate,
            rids: VecDeque::new(),
        }
    }
}

impl<'a> PhysicalOp for SeqScanOp<'a> {
    fn open(&mut self) -> Result<()> {
        // gather all RIDs: scan the table’s pages in storage
        let table_meta = self.storage.catalog.get_table(&self.table)?;
        for page_no in 0..self.storage.pagefile.num_pages()? {
            let page = self.storage.fetch_page_raw(page_no)?;
            for (slot_no, _) in page.iter_slots() {
                self.rids.push_back((page_no, slot_no));
            }
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        while let Some(rid) = self.rids.pop_front() {
            let tuple = self.storage.fetch_tuple(&self.table, rid)?;
            // apply predicate if any
            if let Some(pred) = &self.predicate {
                if !crate::query::executor::eval_predicate(pred, &tuple)? {
                    continue; // skip non-matching
                }
            }
            return Ok(Some(tuple));
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.rids.clear();
        Ok(())
    }
}

/// Index scan: lookup matching RIDs using a B+ tree, then fetch tuples.
pub struct IndexScanOp<'a> {
    storage: &'a mut Storage,
    bptree: BPlusTree<'a>,
    predicate: BoundExpr,
    pending: VecDeque<(u64, u16)>,
}

impl<'a> IndexScanOp<'a> {
    pub fn new(
        storage: &'a mut Storage,
        bptree: BPlusTree<'a>,
        predicate: BoundExpr,
    ) -> Result<Self> {
        Ok(IndexScanOp {
            storage,
            bptree,
            predicate,
            pending: VecDeque::new(),
        })
    }
}

impl<'a> PhysicalOp for IndexScanOp<'a> {
    fn open(&mut self) -> Result<()> {
        // assume predicate is column = literal
        let matches = self.bptree.range_scan(&self.predicate)?;
        for rid in matches {
            self.pending.push_back(rid);
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if let Some(rid) = self.pending.pop_front() {
            let tuple = self.storage.fetch_tuple(&self.bptree.table_name(), rid)?;
            return Ok(Some(tuple));
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.pending.clear();
        Ok(())
    }
}

/// Filter operator: wraps any child and applies a predicate.
pub struct FilterOp<'a> {
    child: Box<dyn PhysicalOp + 'a>,
    predicate: BoundExpr,
}

impl<'a> FilterOp<'a> {
    pub fn new(child: Box<dyn PhysicalOp + 'a>, predicate: BoundExpr) -> Self {
        FilterOp { child, predicate }
    }
}

impl<'a> PhysicalOp for FilterOp<'a> {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        while let Some(row) = self.child.next()? {
            if crate::query::executor::eval_predicate(&self.predicate, &row)? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

/// Projection operator: evaluates expressions against each tuple.
pub struct ProjectionOp<'a> {
    child: Box<dyn PhysicalOp + 'a>,
    exprs: Vec<BoundExpr>,
}

impl<'a> ProjectionOp<'a> {
    pub fn new(child: Box<dyn PhysicalOp + 'a>, exprs: Vec<BoundExpr>) -> Self {
        ProjectionOp { child, exprs }
    }
}

impl<'a> PhysicalOp for ProjectionOp<'a> {
    fn open(&mut self) -> Result<()> {
        self.child.open()
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if let Some(row) = self.child.next()? {
            let mut out = Vec::with_capacity(self.exprs.len());
            for expr in &self.exprs {
                out.push(crate::query::executor::eval_expr(expr, &row)?);
            }
            return Ok(Some(out));
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Expression evaluation
////////////////////////////////////////////////////////////////////////////////

/// Evaluate an expression against a tuple (column ordinal = position).
pub fn eval_expr(expr: &BoundExpr, row: &Tuple) -> Result<Value> {
    Ok(match expr {
        BoundExpr::Literal(v) => v.clone(),
        BoundExpr::Column { ordinal, .. } => row[*ordinal].clone(),
        BoundExpr::BinaryOp {
            left, op, right, ..
        } => {
            let l = eval_expr(left, row)?;
            let r = eval_expr(right, row)?;
            eval_binop(&l, *op, &r)?
        }
    })
}

/// Evaluate a boolean predicate (returns true if tuple matches).
fn eval_predicate(pred: &BoundExpr, row: &Tuple) -> Result<bool> {
    match eval_expr(pred, row)? {
        Value::Int(i) => Ok(i != 0),
        Value::String(s) => Ok(!s.is_empty()),
    }
}

/// Evaluate a binary operator on two literal values.
fn eval_binop(left: &Value, op: BoundExpr, right: &Value) -> Result<Value> {
    use BoundExpr::*;
    match (left, right, op) {
        (
            Value::Int(l),
            Value::Int(r),
            BoundExpr::BinaryOp {
                op: crate::query::binder::BinaryOp::Eq,
                ..
            },
        ) => Ok(Value::Int((l == r) as i64)),
        // ... handle other ops similarly ...
        _ => Err(anyhow!("Unsupported binary op or mismatched types")),
    }
}
