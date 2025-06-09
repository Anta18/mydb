use std::collections::HashMap;

/// Tracks free space for pages and provides first-fit allocation.
pub struct FreeList {
    /// Maps page_no to its current free byte count.
    free_map: HashMap<u64, usize>,
    /// Ordered list of pages to scan for first-fit.
    pages: Vec<u64>,
}

impl FreeList {
    /// Create an empty FreeList.
    pub fn new() -> Self {
        FreeList {
            free_map: HashMap::new(),
            pages: Vec::new(),
        }
    }

    /// Register or update the free space for a page.
    pub fn register(&mut self, page_no: u64, free_bytes: usize) {
        if !self.free_map.contains_key(&page_no) {
            self.pages.push(page_no);
        }
        self.free_map.insert(page_no, free_bytes);
    }

    /// Remove a page from tracking (e.g., when it's full or evicted).
    pub fn remove(&mut self, page_no: u64) {
        self.free_map.remove(&page_no);
        if let Some(idx) = self.pages.iter().position(|&p| p == page_no) {
            self.pages.swap_remove(idx);
        }
    }

    /// Choose the first page with at least `min_bytes` free, or allocate new if none found.
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
