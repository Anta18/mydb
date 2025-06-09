use crate::storage::{
    buffer_pool::BufferPool,
    free_list::FreeList,
    pagefile::PageFile,
    record::{Page as RecordPage, RID},
};
use anyhow::Result;

/// High-level storage engine facade
pub struct Storage {
    pagefile: PageFile,
    buffer_pool: BufferPool,
    free_list: FreeList,
    page_size: usize,
}

impl Storage {
    /// Initialize storage with a data file path, page size, and buffer pool capacity.
    pub fn new(path: &str, page_size: usize, pool_size: usize) -> Result<Self> {
        let mut pf = PageFile::open(path, page_size)?;
        let bp = BufferPool::new(pf, pool_size)?;
        let fl = FreeList::new();
        Ok(Storage {
            pagefile: bp.pagefile,
            buffer_pool: bp,
            free_list: fl,
            page_size,
        })
    }

    /// Insert a new record, returning its RID
    pub fn insert(&mut self, data: &[u8]) -> Result<RID> {
        let needed = data.len() + RecordPage::SLOT_ENTRY_SIZE;
        // choose existing page or allocate new
        let page_no = if let Some(pn) = self.free_list.choose_page(needed) {
            pn
        } else {
            let pn = self.pagefile.allocate_page()?;
            // register fresh page
            let mut page = RecordPage::new(pn, self.page_size);
            self.free_list.register(pn, page.free_space());
            pn
        };

        // fetch into buffer
        let frame = self.buffer_pool.fetch_page(page_no)?;
        // wrap raw bytes into RecordPage
        let mut page = RecordPage::from_bytes(frame.data.clone(), self.page_size);
        // insert tuple
        let rid = page.insert_tuple(data)?;
        // write back
        frame.data = page.to_bytes();
        self.buffer_pool.unpin_page(page_no, true);
        // update free list
        let free = page.free_space();
        self.free_list.register(page_no, free);
        Ok(rid)
    }

    /// Fetch a record by RID
    pub fn fetch(&mut self, rid: RID) -> Result<Vec<u8>> {
        let (page_no, slot_no) = rid;
        let frame = self.buffer_pool.fetch_page(page_no)?;
        let page = RecordPage::from_bytes(frame.data.clone(), self.page_size);
        let rec = page
            .get_tuple(slot_no)
            .ok_or_else(|| anyhow::anyhow!("Record not found"))?;
        self.buffer_pool.unpin_page(page_no, false);
        Ok(rec.to_vec())
    }

    /// Flush all pending writes to disk
    pub fn flush(&mut self) -> Result<()> {
        self.buffer_pool.flush_all()?;
        Ok(())
    }
}
