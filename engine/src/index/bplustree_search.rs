use crate::index::node_serializer::{
    InternalNodeSerializer, LeafNodeSerializer, NodeHeader, NodeType,
};
use crate::storage::storage::Storage;
use anyhow::{Context, Result};

/// Provides search path traversal for a B+Tree.
pub struct BPlusTreeSearch<'a> {
    storage: &'a mut Storage,
    internal_serializer: InternalNodeSerializer,
    leaf_serializer: LeafNodeSerializer,
}

impl<'a> BPlusTreeSearch<'a> {
    /// Initialize a search helper with references to storage and tree order.
    pub fn new(storage: &'a mut Storage, order: usize) -> Self {
        BPlusTreeSearch {
            storage,
            internal_serializer: InternalNodeSerializer { order },
            leaf_serializer: LeafNodeSerializer { order },
        }
    }

    /// Traverse from root to leaf, returning the sequence of page numbers visited.
    pub fn search_path(&mut self, root_page: u64, key: u64) -> Result<Vec<u64>> {
        let mut path = Vec::new();
        let mut current = root_page;

        loop {
            path.push(current);
            // Fetch raw page bytes
            let frame = self
                .storage
                .buffer_pool
                .fetch_page(current)
                .context("Failed to fetch page for search")?;
            let buf = &frame.data;

            // Deserialize header to determine node type
            let header = NodeHeader::deserialize(&buf[0..NodeHeader::SIZE])
                .context("Failed to deserialize node header")?;

            match header.node_type {
                NodeType::Internal => {
                    // Deserialize keys and children
                    let (_hdr, keys, children) = self
                        .internal_serializer
                        .deserialize(buf)
                        .context("Internal node deserialization failed")?;
                    // Binary search to find child index
                    let idx = match keys.binary_search(&key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    // Move to child page
                    let next_page = children[idx];
                    // Unpin current frame
                    self.storage.buffer_pool.unpin_page(current, false);
                    current = next_page;
                }
                NodeType::Leaf => {
                    // Leaf reached; unpin and break
                    self.storage.buffer_pool.unpin_page(current, false);
                    break;
                }
            }
        }

        Ok(path)
    }

    /// Find the leaf page that should contain the given key.
    pub fn locate_leaf(&mut self, root_page: u64, key: u64) -> Result<u64> {
        let path = self.search_path(root_page, key)?;
        // Last element is the leaf
        path.last()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("Search path is empty"))
    }
}
