
use crate::index::bplustree_search::BPlusTreeSearch;
use crate::index::node_modifier::NodeModifier;
use crate::index::node_serializer::{LeafNodeSerializer, NodeHeader, NodeType};
use crate::query::binder::BoundExpr;
use crate::storage::record::RID;
use crate::storage::storage::Storage;
use anyhow::{Context, Result, anyhow};


pub struct BPlusTree {
    storage: Storage,
    order: usize,
    root_page: u64,
    table_name: String, 
}

impl BPlusTree {
    
    pub fn new(
        path: &str,
        page_size: usize,
        pool_size: usize,
        order: usize,
        table_name: String,
    ) -> Result<Self> {
        let mut storage =
            Storage::new(path, page_size, pool_size).context("Initializing storage failed")?;
        
        let root_page = storage.buffer_pool.pagefile.allocate_page()?;
        
        let header = NodeHeader {
            node_type: NodeType::Leaf,
            key_count: 0,
            parent: 0,
        };
        let buf = LeafNodeSerializer { order }.serialize(&header, &[], &[], 0, page_size);
        let frame = storage.buffer_pool.fetch_page(root_page)?;
        frame.data = buf;
        storage.buffer_pool.unpin_page(root_page, true);

        Ok(Self {
            storage,
            order,
            root_page,
            table_name,
        })
    }

    
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    
    pub fn insert(&mut self, key: u64, rid: RID) -> Result<()> {
        let mut modifier = NodeModifier::new(&mut self.storage, self.order);
        let new_root = modifier.insert(self.root_page, key, rid)?;
        self.root_page = new_root;
        Ok(())
    }

    
    pub fn get(&mut self, key: u64) -> Result<Option<RID>> {
        let mut searcher = BPlusTreeSearch::new(&mut self.storage, self.order);
        let leaf = searcher.locate_leaf(self.root_page, key)?;
        let frame = self.storage.buffer_pool.fetch_page(leaf)?;
        let (_hdr, keys, rids, _) =
            LeafNodeSerializer { order: self.order }.deserialize(&frame.data)?;
        self.storage.buffer_pool.unpin_page(leaf, false);
        if let Some(idx) = keys.iter().position(|&k| k == key) {
            Ok(Some(rids[idx]))
        } else {
            Ok(None)
        }
    }

    
    pub fn range_scan_keys(&mut self, lo: u64, hi: u64) -> Result<Vec<(u64, RID)>> {
        let mut results = Vec::new();
        let mut searcher = BPlusTreeSearch::new(&mut self.storage, self.order);
        let mut leaf = searcher.locate_leaf(self.root_page, lo)?;
        loop {
            let frame = self.storage.buffer_pool.fetch_page(leaf)?;
            let (_hdr, keys, rids, next_leaf) =
                LeafNodeSerializer { order: self.order }.deserialize(&frame.data)?;
            for (&k, &rid) in keys.iter().zip(rids.iter()) {
                if k > hi {
                    break;
                }
                if k >= lo {
                    results.push((k, rid));
                }
            }
            self.storage.buffer_pool.unpin_page(leaf, false);
            if next_leaf == 0 {
                break;
            }
            leaf = next_leaf;
        }
        Ok(results)
    }

    
    
    pub fn range_scan(&mut self, predicate: &BoundExpr) -> Result<Vec<RID>> {
        match predicate {
            BoundExpr::BinaryOp {
                left, op, right, ..
            } => {
                let key = match (left.as_ref(), right.as_ref()) {
                    (_, BoundExpr::Literal(crate::query::binder::Value::Int(val))) => *val as u64,
                    (BoundExpr::Literal(crate::query::binder::Value::Int(val)), _) => *val as u64,
                    _ => return Err(anyhow!("Cannot extract key from predicate")),
                };

                match op {
                    crate::query::parser::BinaryOp::Eq => {
                        
                        if let Some(rid) = self.get(key)? {
                            Ok(vec![rid])
                        } else {
                            Ok(vec![])
                        }
                    }
                    crate::query::parser::BinaryOp::Lt => {
                        
                        let results = self.range_scan_keys(0, key.saturating_sub(1))?;
                        Ok(results.into_iter().map(|(_, rid)| rid).collect())
                    }
                    crate::query::parser::BinaryOp::Gt => {
                        
                        let results = self.range_scan_keys(key + 1, u64::MAX)?;
                        Ok(results.into_iter().map(|(_, rid)| rid).collect())
                    }
                    _ => Err(anyhow!("Unsupported operator for index scan")),
                }
            }
            _ => Err(anyhow!("Invalid predicate for index scan")),
        }
    }
}
