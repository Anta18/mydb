use crate::index::bplustree_search::BPlusTreeSearch;
use crate::index::node_modifier::NodeModifier;
use crate::index::node_serializer::{LeafNodeSerializer, NodeHeader, NodeType};
use crate::storage::storage::Storage;
use anyhow::{Context, Result};

/// High-level B+ Tree controller
pub struct BPlusTree {
    storage: Storage,
    order: usize,
    root_page: u64,
}

impl BPlusTree {
    /// Initialize a new B+ tree: creates a root leaf node.
    pub fn new(path: &str, page_size: usize, pool_size: usize, order: usize) -> Result<Self> {
        let mut storage =
            Storage::new(path, page_size, pool_size).context("Initializing storage failed")?;
        // Allocate root
        let root_page = storage.buffer_pool.pagefile.allocate_page()?;
        // Write empty leaf node header
        let header = NodeHeader {
            node_type: NodeType::Leaf,
            key_count: 0,
            parent: 0,
        };
        let buf = LeafNodeSerializer { order }.serialize(&header, &[], &[], 0, page_size);
        let mut frame = storage.buffer_pool.fetch_page(root_page)?;
        frame.data = buf;
        storage.buffer_pool.unpin_page(root_page, true);

        Ok(Self {
            storage,
            order,
            root_page,
        })
    }

    /// Insert a key and RID, updating root if split.
    pub fn insert(&mut self, key: u64, rid: (u64, u16)) -> Result<()> {
        let mut modifier = NodeModifier::new(&mut self.storage, self.order);
        let new_root = modifier.insert(self.root_page, key, rid)?;
        self.root_page = new_root;
        Ok(())
    }

    /// Get the RID associated with a key.
    pub fn get(&mut self, key: u64) -> Result<Option<(u64, u16)>> {
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

    /// Perform a range scan between lo and hi (inclusive).
    pub fn range_scan(&mut self, lo: u64, hi: u64) -> Result<Vec<(u64, (u64, u16))>> {
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
}
