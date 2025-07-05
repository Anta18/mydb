
use crate::index::node_serializer::{
    InternalNodeSerializer, LeafNodeSerializer, NodeHeader, NodeType,
};
use crate::storage::storage::Storage;
use anyhow::{Context, Result};


pub struct BPlusTreeSearch<'a> {
    storage: &'a mut Storage,
    internal_serializer: InternalNodeSerializer,
    leaf_serializer: LeafNodeSerializer,
}

impl<'a> BPlusTreeSearch<'a> {
    
    pub fn new(storage: &'a mut Storage, order: usize) -> Self {
        BPlusTreeSearch {
            storage,
            internal_serializer: InternalNodeSerializer { order },
            leaf_serializer: LeafNodeSerializer { order },
        }
    }

    
    pub fn search_path(&mut self, root_page: u64, key: u64) -> Result<Vec<u64>> {
        let mut path = Vec::new();
        let mut current = root_page;

        loop {
            path.push(current);
            
            let frame = self
                .storage
                .buffer_pool
                .fetch_page(current)
                .context("Failed to fetch page for search")?;
            let buf = &frame.data;

            
            let header = NodeHeader::deserialize(&buf[0..NodeHeader::SIZE])
                .context("Failed to deserialize node header")?;

            match header.node_type {
                NodeType::Internal => {
                    
                    let (_hdr, keys, children) = self
                        .internal_serializer
                        .deserialize(buf)
                        .context("Internal node deserialization failed")?;
                    
                    let idx = match keys.binary_search(&key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    
                    let next_page = children[idx];
                    
                    self.storage.buffer_pool.unpin_page(current, false);
                    current = next_page;
                }
                NodeType::Leaf => {
                    
                    self.storage.buffer_pool.unpin_page(current, false);
                    break;
                }
            }
        }

        Ok(path)
    }

    
    pub fn locate_leaf(&mut self, root_page: u64, key: u64) -> Result<u64> {
        let path = self.search_path(root_page, key)?;
        
        path.last()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Search path is empty"))
    }
}
