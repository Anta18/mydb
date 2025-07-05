
use std::collections::HashMap;


pub struct FreeList {
    
    free_map: HashMap<u64, usize>,
    
    pages: Vec<u64>,
}

impl FreeList {
    
    pub fn new() -> Self {
        FreeList {
            free_map: HashMap::new(),
            pages: Vec::new(),
        }
    }

    
    pub fn register(&mut self, page_no: u64, free_bytes: usize) {
        if !self.free_map.contains_key(&page_no) {
            self.pages.push(page_no);
        }
        self.free_map.insert(page_no, free_bytes);
    }

    
    pub fn remove(&mut self, page_no: u64) {
        self.free_map.remove(&page_no);
        if let Some(idx) = self.pages.iter().position(|&p| p == page_no) {
            self.pages.swap_remove(idx);
        }
    }

    
    pub fn choose_page(&self, min_bytes: usize) -> Option<u64> {
        for &page_no in &self.pages {
            if let Some(&free) = self.free_map.get(&page_no) {
                if free >= min_bytes {
                    return Some(page_no);
                }
            }
        }
        None
    }
}
