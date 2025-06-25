// query/executor.rs

use crate::index::bplustree::BPlusTree;
use crate::query::binder::{BoundExpr, Catalog, Value};
use crate::query::parser::BinaryOp; // Import BinaryOp from parser
use crate::storage::record::RID;
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
    catalog: &'a Catalog,
    table: String,
    predicate: Option<BoundExpr>,
    // queue of pending RIDs
    rids: VecDeque<RID>,
}

impl<'a> SeqScanOp<'a> {
    pub fn new(
        storage: &'a mut Storage,
        catalog: &'a Catalog,
        table: String,
        predicate: Option<BoundExpr>,
    ) -> Self {
        SeqScanOp {
            storage,
            catalog,
            table,
            predicate,
            rids: VecDeque::new(),
        }
    }
}

impl<'a> PhysicalOp for SeqScanOp<'a> {
    fn open(&mut self) -> Result<()> {
        // gather all RIDs: scan the table's pages in storage
        let table_meta = self.catalog.get_table(&self.table)?;

        // Scan through all pages in the buffer pool's pagefile
        for page_no in 0..self.storage.buffer_pool.pagefile.num_pages()? {
            let frame = self.storage.buffer_pool.fetch_page(page_no)?;
            let page = crate::storage::record::Page::from_bytes(
                frame.data.clone(),
                self.storage.page_size,
            );

            // Iterate through all slots in the page using the iter_slots method
            for (slot_no, _slot_data) in page.iter_slots() {
                self.rids.push_back((page_no, slot_no));
            }

            self.storage.buffer_pool.unpin_page(page_no, false);
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        while let Some(rid) = self.rids.pop_front() {
            let tuple_data = self.storage.fetch(rid)?;
            let tuple = self.deserialize_tuple(&tuple_data)?;

            // apply predicate if any
            if let Some(pred) = &self.predicate {
                if !eval_predicate(pred, &tuple)? {
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

impl<'a> SeqScanOp<'a> {
    /// Convert raw bytes to a tuple of Values based on table schema
    fn deserialize_tuple(&self, data: &[u8]) -> Result<Tuple> {
        let table_meta = self.catalog.get_table(&self.table)?;
        let mut tuple = Vec::with_capacity(table_meta.columns.len());
        let mut offset = 0;

        for col in &table_meta.columns {
            match col.data_type {
                crate::query::binder::DataType::Int => {
                    if offset + 8 > data.len() {
                        return Err(anyhow!("Insufficient data for int column"));
                    }
                    let bytes: [u8; 8] = data[offset..offset + 8]
                        .try_into()
                        .map_err(|_| anyhow!("Failed to read int"))?;
                    let val = i64::from_le_bytes(bytes);
                    tuple.push(Value::Int(val));
                    offset += 8;
                }
                crate::query::binder::DataType::Varchar => {
                    if offset + 4 > data.len() {
                        return Err(anyhow!("Insufficient data for varchar length"));
                    }
                    let len_bytes: [u8; 4] = data[offset..offset + 4]
                        .try_into()
                        .map_err(|_| anyhow!("Failed to read varchar length"))?;
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    offset += 4;

                    if offset + len > data.len() {
                        return Err(anyhow!("Insufficient data for varchar content"));
                    }
                    let str_data = &data[offset..offset + len];
                    let val = String::from_utf8(str_data.to_vec())
                        .map_err(|_| anyhow!("Invalid UTF-8 in varchar"))?;
                    tuple.push(Value::String(val));
                    offset += len;
                }
            }
        }

        Ok(tuple)
    }
}

/// Index scan: lookup matching RIDs using a B+ tree, then fetch tuples.
pub struct IndexScanOp<'a> {
    storage: &'a mut Storage,
    catalog: &'a Catalog,
    bptree: BPlusTree,
    predicate: BoundExpr,
    pending: VecDeque<RID>,
}

impl<'a> IndexScanOp<'a> {
    pub fn new(
        storage: &'a mut Storage,
        catalog: &'a Catalog,
        bptree: BPlusTree,
        predicate: BoundExpr,
    ) -> Result<Self> {
        Ok(IndexScanOp {
            storage,
            catalog,
            bptree,
            predicate,
            pending: VecDeque::new(),
        })
    }
}

impl<'a> PhysicalOp for IndexScanOp<'a> {
    fn open(&mut self) -> Result<()> {
        // Extract key value from predicate for range scan
        let rids = self.bptree.range_scan(&self.predicate)?;

        for rid in rids {
            self.pending.push_back(rid);
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if let Some(rid) = self.pending.pop_front() {
            let tuple_data = self.storage.fetch(rid)?;
            let tuple = self.deserialize_tuple(&tuple_data)?;
            return Ok(Some(tuple));
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.pending.clear();
        Ok(())
    }
}

impl<'a> IndexScanOp<'a> {
    /// Convert raw bytes to a tuple of Values based on table schema
    fn deserialize_tuple(&self, data: &[u8]) -> Result<Tuple> {
        // This would need to know which table the B+ tree corresponds to
        // For now, assume it's stored in the B+ tree or passed separately
        let table_name = self.bptree.table_name();
        let table_meta = self.catalog.get_table(table_name)?;
        let mut tuple = Vec::with_capacity(table_meta.columns.len());
        let mut offset = 0;

        for col in &table_meta.columns {
            match col.data_type {
                crate::query::binder::DataType::Int => {
                    if offset + 8 > data.len() {
                        return Err(anyhow!("Insufficient data for int column"));
                    }
                    let bytes: [u8; 8] = data[offset..offset + 8]
                        .try_into()
                        .map_err(|_| anyhow!("Failed to read int"))?;
                    let val = i64::from_le_bytes(bytes);
                    tuple.push(Value::Int(val));
                    offset += 8;
                }
                crate::query::binder::DataType::Varchar => {
                    if offset + 4 > data.len() {
                        return Err(anyhow!("Insufficient data for varchar length"));
                    }
                    let len_bytes: [u8; 4] = data[offset..offset + 4]
                        .try_into()
                        .map_err(|_| anyhow!("Failed to read varchar length"))?;
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    offset += 4;

                    if offset + len > data.len() {
                        return Err(anyhow!("Insufficient data for varchar content"));
                    }
                    let str_data = &data[offset..offset + len];
                    let val = String::from_utf8(str_data.to_vec())
                        .map_err(|_| anyhow!("Invalid UTF-8 in varchar"))?;
                    tuple.push(Value::String(val));
                    offset += len;
                }
            }
        }

        Ok(tuple)
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
            if eval_predicate(&self.predicate, &row)? {
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
                out.push(eval_expr(expr, &row)?);
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
fn eval_binop(left: &Value, op: BinaryOp, right: &Value) -> Result<Value> {
    match (left, right, op) {
        (Value::Int(l), Value::Int(r), BinaryOp::Eq) => Ok(Value::Int((*l == *r) as i64)),
        (Value::Int(l), Value::Int(r), BinaryOp::Lt) => Ok(Value::Int((*l < *r) as i64)),
        (Value::Int(l), Value::Int(r), BinaryOp::Gt) => Ok(Value::Int((*l > *r) as i64)),
        (Value::String(l), Value::String(r), BinaryOp::Eq) => Ok(Value::Int((l == r) as i64)),
        _ => Err(anyhow!("Unsupported binary op or mismatched types")),
    }
}
