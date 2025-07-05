

use crate::index::bplustree_search::BPlusTreeSearch;
use crate::index::node_serializer::{
    InternalNodeSerializer, LeafNodeSerializer, NodeHeader, NodeType,
};
use crate::storage::record::RID;
use crate::storage::storage::Storage;
use anyhow::{Context, Result};



pub struct NodeModifier<'a> {
    storage: &'a mut Storage,
    order: usize,
    internal_serializer: InternalNodeSerializer,
    leaf_serializer: LeafNodeSerializer,
    
    path_cache: Vec<u64>,
}

impl<'a> NodeModifier<'a> {
    pub fn new(storage: &'a mut Storage, order: usize) -> Self {
        Self {
            storage,
            order,
            internal_serializer: InternalNodeSerializer { order },
            leaf_serializer: LeafNodeSerializer { order },
            path_cache: Vec::new(),
        }
    }

    
    
    pub fn insert(&mut self, root_page: u64, key: u64, rid: RID) -> Result<u64> {
        
        let mut searcher = BPlusTreeSearch::new(self.storage, self.order);
        self.path_cache = searcher.search_path(root_page, key)?;
        let leaf_page = *self.path_cache.last().unwrap();
        
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
        
        let frame = self.storage.buffer_pool.fetch_page(leaf_page)?;
        let buf = &frame.data;
        let (mut header, mut keys, mut rids, next_leaf) = self
            .leaf_serializer
            .deserialize(buf)
            .context("Leaf deserialize failed")?;
        
        if keys.binary_search(&key).is_ok() {
            return Err(anyhow::anyhow!("Duplicate key insertion not allowed"));
        }
        
        let idx = keys.binary_search(&key).unwrap_or_else(|i| i);
        keys.insert(idx, key);
        rids.insert(idx, rid);
        header.key_count += 1;

        
        header.parent = *self
            .path_cache
            .get(self.path_cache.len().saturating_sub(2))
            .unwrap_or(&0);

        if (header.key_count as usize) <= self.order {
            
            let new_buf = self.leaf_serializer.serialize(
                &header,
                &keys,
                &rids,
                next_leaf,
                self.storage.page_size,
            );
            frame.data.copy_from_slice(&new_buf);
            self.storage.buffer_pool.unpin_page(leaf_page, true);
            
            let used_space = new_buf.len();
            let free_space = self.storage.page_size.saturating_sub(used_space);
            self.storage.free_list.register(leaf_page, free_space);
            Ok((root_page, None, None))
        } else {
            
            self.storage.buffer_pool.unpin_page(leaf_page, false);

            let mid = (header.key_count as usize + 1) / 2;
            let right_keys = keys.split_off(mid);
            let right_rids = rids.split_off(mid);
            header.key_count = keys.len() as u16;
            let split_key = right_keys[0];

            
            let right_page = self.storage.buffer_pool.pagefile.allocate_page()?;

            
            let right_header = NodeHeader {
                node_type: NodeType::Leaf,
                key_count: right_keys.len() as u16,
                parent: header.parent,
            };
            
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

            
            let left_frame = self.storage.buffer_pool.fetch_page(leaf_page)?;
            left_frame.data.copy_from_slice(&left_buf);
            self.storage.buffer_pool.unpin_page(leaf_page, true);
            let left_free_space = self.storage.page_size.saturating_sub(left_buf.len());
            self.storage.free_list.register(leaf_page, left_free_space);

            
            let right_frame = self.storage.buffer_pool.fetch_page(right_page)?;
            right_frame.data.copy_from_slice(&right_buf);
            self.storage.buffer_pool.unpin_page(right_page, true);
            let right_free_space = self.storage.page_size.saturating_sub(right_buf.len());
            self.storage
                .free_list
                .register(right_page, right_free_space);

            
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
        
        if left_page == root_page {
            let new_root = self.storage.buffer_pool.pagefile.allocate_page()?;
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
            
            let free_space = self.storage.page_size.saturating_sub(buf.len());
            self.storage.free_list.register(new_root, free_space);
            Ok((new_root, Some(split_key), Some(right_page)))
        } else {
            
            let parent_page = *self.path_cache.get(self.path_cache.len() - 2).unwrap();
            let frame = self.storage.buffer_pool.fetch_page(parent_page)?;
            let buf = &frame.data;
            let (mut header, mut keys, mut children) = self
                .internal_serializer
                .deserialize(buf)
                .context("Internal deserialize failed")?;
            
            if keys.iter().any(|&k| k == split_key) {
                return Err(anyhow::anyhow!(
                    "Duplicate key insertion not allowed in internal node"
                ));
            }
            
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
                let free_space = self.storage.page_size.saturating_sub(new_buf.len());
                self.storage.free_list.register(parent_page, free_space);
                Ok((root_page, None, None))
            } else {
                
                self.storage.buffer_pool.unpin_page(parent_page, false);

                let mid = header.key_count as usize / 2;
                let promote_key = keys[mid];
                let right_keys = keys.split_off(mid + 1);
                let right_children = children.split_off(mid + 1);
                header.key_count = mid as u16;
                children.truncate(mid + 1);

                
                let new_right_page = self.storage.buffer_pool.pagefile.allocate_page()?;

                
                let left_buf = self.internal_serializer.serialize(
                    &header,
                    &keys,
                    &children,
                    self.storage.page_size,
                );

                
                let left_frame = self.storage.buffer_pool.fetch_page(parent_page)?;
                left_frame.data.copy_from_slice(&left_buf);
                self.storage.buffer_pool.unpin_page(parent_page, true);
                let left_free_space = self.storage.page_size.saturating_sub(left_buf.len());
                self.storage
                    .free_list
                    .register(parent_page, left_free_space);

                
                let right_header = NodeHeader {
                    node_type: NodeType::Internal,
                    key_count: right_keys.len() as u16,
                    parent: header.parent,
                };
                let right_buf = self.internal_serializer.serialize(
                    &right_header,
                    &right_keys,
                    &right_children,
                    self.storage.page_size,
                );
                let right_frame = self.storage.buffer_pool.fetch_page(new_right_page)?;
                right_frame.data.copy_from_slice(&right_buf);
                self.storage.buffer_pool.unpin_page(new_right_page, true);
                let right_free_space = self.storage.page_size.saturating_sub(right_buf.len());
                self.storage
                    .free_list
                    .register(new_right_page, right_free_space);

                
                self.insert_into_parent(root_page, parent_page, promote_key, new_right_page)
            }
        }
    }
}
