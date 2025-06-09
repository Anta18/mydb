// src/index/node_modifier.rs

use crate::index::bplustree_search::BPlusTreeSearch;
use crate::index::node_serializer::{
    InternalNodeSerializer, LeafNodeSerializer, NodeHeader, NodeType,
};
use crate::storage::record::RID;
use crate::storage::storage::Storage;
use anyhow::{Context, Result};

/// Handles insertion logic and node splits in a B+ Tree with maintained parent pointers,
/// free-space registration, duplicate-key checks, and optimized split propagation.
pub struct NodeModifier<'a> {
    storage: &'a mut Storage,
    order: usize,
    internal_serializer: InternalNodeSerializer,
    leaf_serializer: LeafNodeSerializer,
    searcher: BPlusTreeSearch<'a>,
    /// Cached path during initial locate to avoid re-traversal
    path_cache: Vec<u64>,
}

impl<'a> NodeModifier<'a> {
    pub fn new(storage: &'a mut Storage, order: usize) -> Self {
        Self {
            storage,
            order,
            internal_serializer: InternalNodeSerializer { order },
            leaf_serializer: LeafNodeSerializer { order },
            searcher: BPlusTreeSearch::new(storage, order),
            path_cache: Vec::new(),
        }
    }

    /// Insert a key -> RID into the B+ tree starting at root_page.
    /// Returns new root page if split propagates to root.
    pub fn insert(&mut self, root_page: u64, key: u64, rid: RID) -> Result<u64> {
        // Locate leaf and cache path
        self.path_cache = self.searcher.search_path(root_page, key)?;
        let leaf_page = *self.path_cache.last().unwrap();
        // Insert and handle splits
        let (new_root, _, _) = self.insert_into_leaf(leaf_page, key, rid, root_page)?;
        Ok(new_root)
    }

    fn insert_into_leaf(
        &mut self,
        leaf_page: u64,
        key: u64,
        rid: RID,
        root_page: u64,
    ) -> Result<(u64, Option<u64>, Option<u64>)> {
        // Fetch leaf
        let frame = self.storage.buffer_pool.fetch_page(leaf_page)?;
        let buf = &frame.data;
        let (mut header, mut keys, mut rids, mut next_leaf) = self
            .leaf_serializer
            .deserialize(buf)
            .context("Leaf deserialize failed")?;
        // Duplicate-key check
        if keys.binary_search(&key).is_ok() {
            return Err(anyhow::anyhow!("Duplicate key insertion not allowed"));
        }
        // Insert
        let idx = keys.binary_search(&key).unwrap_or_else(|i| i);
        keys.insert(idx, key);
        rids.insert(idx, rid);
        header.key_count += 1;

        // Update parent pointer on leaf header
        header.parent = *self
            .path_cache
            .get(self.path_cache.len().saturating_sub(2))
            .unwrap_or(&0);

        if (header.key_count as usize) <= self.order {
            // No split: serialize and write back
            let new_buf = self.leaf_serializer.serialize(
                &header,
                &keys,
                &rids,
                next_leaf,
                self.storage.page_size,
            );
            frame.data.copy_from_slice(&new_buf);
            self.storage.buffer_pool.unpin_page(leaf_page, true);
            // register free space for leaf page
            self.storage
                .free_list
                .register(leaf_page, self.leaf_serializer.compute_free_space(&new_buf));
            Ok((root_page, None, None))
        } else {
            // Split leaf
            let mid = (header.key_count as usize + 1) / 2;
            let right_keys = keys.split_off(mid);
            let right_rids = rids.split_off(mid);
            header.key_count = keys.len() as u16;
            let split_key = right_keys[0];
            // Allocate right page
            let right_page = self.storage.pagefile.allocate_page()?;
            // New headers with parent
            let right_header = NodeHeader {
                node_type: NodeType::Leaf,
                key_count: right_keys.len() as u16,
                parent: header.parent,
            };
            // Serialize buffers
            let left_buf = self.leaf_serializer.serialize(
                &header,
                &keys,
                &rids,
                right_page,
                self.storage.page_size,
            );
            let right_buf = self.leaf_serializer.serialize(
                &right_header,
                &right_keys,
                &right_rids,
                next_leaf,
                self.storage.page_size,
            );
            // Write left and register
            frame.data.copy_from_slice(&left_buf);
            self.storage.buffer_pool.unpin_page(leaf_page, true);
            self.storage.free_list.register(
                leaf_page,
                self.leaf_serializer.compute_free_space(&left_buf),
            );
            // Write right and register
            let mut right_frame = self.storage.buffer_pool.fetch_page(right_page)?;
            right_frame.data.copy_from_slice(&right_buf);
            self.storage.buffer_pool.unpin_page(right_page, true);
            self.storage.free_list.register(
                right_page,
                self.leaf_serializer.compute_free_space(&right_buf),
            );
            // Link siblings parent pointers
            // Propagate to parent without re-traversal using path_cache
            let (new_root, _, _) =
                self.insert_into_parent(root_page, leaf_page, split_key, right_page)?;
            Ok((new_root, Some(split_key), Some(right_page)))
        }
    }

    fn insert_into_parent(
        &mut self,
        root_page: u64,
        left_page: u64,
        split_key: u64,
        right_page: u64,
    ) -> Result<(u64, Option<u64>, Option<u64>)> {
        // If left was root, create new root
        if left_page == root_page {
            let new_root = self.storage.pagefile.allocate_page()?;
            let header = NodeHeader {
                node_type: NodeType::Internal,
                key_count: 1,
                parent: 0,
            };
            let buf = self.internal_serializer.serialize(
                &header,
                &[split_key],
                &[left_page, right_page],
                self.storage.page_size,
            );
            let frame = self.storage.buffer_pool.fetch_page(new_root)?;
            frame.data.copy_from_slice(&buf);
            self.storage.buffer_pool.unpin_page(new_root, true);
            // register free space
            self.storage
                .free_list
                .register(new_root, self.internal_serializer.compute_free_space(&buf));
            Ok((new_root, Some(split_key), Some(right_page)))
        } else {
            // Use cached path to find parent
            let parent_page = *self.path_cache.get(self.path_cache.len() - 2).unwrap();
            let frame = self.storage.buffer_pool.fetch_page(parent_page)?;
            let buf = &frame.data;
            let (mut header, mut keys, mut children) = self
                .internal_serializer
                .deserialize(buf)
                .context("Internal deserialize failed")?;
            // Duplicate-key check
            if keys.iter().any(|&k| k == split_key) {
                return Err(anyhow::anyhow!(
                    "Duplicate key insertion not allowed in internal node"
                ));
            }
            // Insert
            let idx = children.iter().position(|&c| c == left_page).unwrap() + 1;
            keys.insert(idx - 1, split_key);
            children.insert(idx, right_page);
            header.key_count += 1;
            header.parent = *self
                .path_cache
                .get(self.path_cache.len().saturating_sub(3))
                .unwrap_or(&0);

            if (header.key_count as usize) <= self.order {
                let new_buf = self.internal_serializer.serialize(
                    &header,
                    &keys,
                    &children,
                    self.storage.page_size,
                );
                frame.data.copy_from_slice(&new_buf);
                self.storage.buffer_pool.unpin_page(parent_page, true);
                self.storage.free_list.register(
                    parent_page,
                    self.internal_serializer.compute_free_space(&new_buf),
                );
                Ok((root_page, None, None))
            } else {
                // Split internal
                let mid = header.key_count as usize / 2;
                let promote_key = keys[mid];
                let right_keys = keys.split_off(mid + 1);
                let right_children = children.split_off(mid + 1);
                header.key_count = mid as u16;
                children.truncate(mid + 1);
                // Serialize left
                let left_buf = self.internal_serializer.serialize(
                    &header,
                    &keys,
                    &children,
                    self.storage.page_size,
                );
                frame.data.copy_from_slice(&left_buf);
                self.storage.buffer_pool.unpin_page(parent_page, true);
                self.storage.free_list.register(
                    parent_page,
                    self.internal_serializer.compute_free_space(&left_buf),
                );
                // Prepare right node
                let right_header = NodeHeader {
                    node_type: NodeType::Internal,
                    key_count: right_keys.len() as u16,
                    parent: header.parent,
                };
                let right_page = self.storage.pagefile.allocate_page()?;
                let right_buf = self.internal_serializer.serialize(
                    &right_header,
                    &right_keys,
                    &right_children,
                    self.storage.page_size,
                );
                let right_frame = self.storage.buffer_pool.fetch_page(right_page)?;
                right_frame.data.copy_from_slice(&right_buf);
                self.storage.buffer_pool.unpin_page(right_page, true);
                self.storage.free_list.register(
                    right_page,
                    self.internal_serializer.compute_free_space(&right_buf),
                );
                // Recursively propagate up
                self.insert_into_parent(root_page, parent_page, promote_key, right_page)
            }
        }
    }
}
