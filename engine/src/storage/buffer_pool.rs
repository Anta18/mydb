use crate::storage::pagefile::PageFile;
use std::collections::{HashMap, VecDeque};
use std::io;

/// A single in-memory frame representing one page.
pub struct Frame {
    pub page_no: u64,
    pub data: Vec<u8>,
    pub is_dirty: bool,
    pub pin_count: usize,
    /// Reference bit for CLOCK eviction
    pub ref_bit: bool,
}

/// CLOCK-based buffer pool
pub struct BufferPool {
    pool: HashMap<u64, Frame>,
    capacity: usize,
    eviction_queue: VecDeque<u64>,
    clock_hand: usize,
    pub pagefile: PageFile,
}

impl BufferPool {
    /// Create a new BufferPool with given capacity and underlying PageFile.
    pub fn new(pagefile: PageFile, capacity: usize) -> io::Result<Self> {
        Ok(BufferPool {
            pool: HashMap::new(),
            capacity,
            eviction_queue: VecDeque::new(),
            clock_hand: 0,
            pagefile,
        })
    }

    /// Fetch a page into the buffer pool, pin it, and return a mutable reference.
    pub fn fetch_page(&mut self, page_no: u64) -> io::Result<&mut Frame> {
        // If not already loaded, read (and possibly evict) then insert
        if !self.pool.contains_key(&page_no) {
            if self.pool.len() == self.capacity {
                self.evict_one()?;
            }
            let buf = self.pagefile.read_page(page_no)?;
            let frame = Frame {
                page_no,
                data: buf,
                is_dirty: false,
                pin_count: 0,
                ref_bit: false,
            };
            self.pool.insert(page_no, frame);
            self.eviction_queue.push_back(page_no);
        }

        // Safe: entry exists
        let frame = self.pool.get_mut(&page_no).unwrap();
        frame.pin_count += 1;
        frame.ref_bit = true;
        Ok(frame)
    }

    /// Unpin a page, optionally marking it dirty.
    pub fn unpin_page(&mut self, page_no: u64, is_dirty: bool) {
        if let Some(frame) = self.pool.get_mut(&page_no) {
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
            if is_dirty {
                frame.is_dirty = true;
            }
        }
    }

    /// Flush all dirty pages back to disk.
    pub fn flush_all(&mut self) -> io::Result<()> {
        for frame in self.pool.values_mut() {
            if frame.is_dirty {
                self.pagefile.write_page(frame.page_no, &frame.data)?;
                frame.is_dirty = false;
            }
        }
        self.pagefile.sync_all()?;
        Ok(())
    }

    /// Evict one unpinned page using the CLOCK algorithm.
    fn evict_one(&mut self) -> io::Result<()> {
        let len = self.eviction_queue.len();
        for _ in 0..len {
            let page_no = self.eviction_queue[self.clock_hand];
            let frame = self.pool.get_mut(&page_no).unwrap();
            if frame.pin_count == 0 {
                if frame.ref_bit {
                    // give second chance
                    frame.ref_bit = false;
                    self.clock_hand = (self.clock_hand + 1) % len;
                } else {
                    // evict
                    if frame.is_dirty {
                        self.pagefile.write_page(page_no, &frame.data)?;
                    }
                    self.pool.remove(&page_no);
                    self.eviction_queue.remove(self.clock_hand);
                    return Ok(());
                }
            } else {
                // skip pinned
                self.clock_hand = (self.clock_hand + 1) % len;
            }
        }
        Err(io::Error::new(
            io::ErrorKind::Other,
            "No page available for eviction",
        ))
    }
}
