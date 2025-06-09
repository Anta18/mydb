use std::fs::remove_file;
use storage::{buffer_pool::BufferPool, pagefile::PageFile};

/// Test that fetching a page loads data from disk and pins it correctly.
#[test]
fn test_fetch_page_and_unpin() {
    let path = "test_bufpool.db";
    let mut pf = PageFile::open(path, 4096).unwrap();
    // prepare page 0
    let data = vec![7u8; 4096];
    pf.write_page(0, &data).unwrap();
    let mut bp = BufferPool::new(pf, 1).unwrap();

    {
        let frame = bp.fetch_page(0).unwrap();
        assert_eq!(frame.data, data);
        assert_eq!(frame.pin_count, 1);
    }
    bp.unpin_page(0, false);
    {
        let frame = bp.pool.get(&0).unwrap();
        assert_eq!(frame.pin_count, 0);
        assert!(!frame.is_dirty);
    }
    remove_file(path).unwrap();
}

/// Test eviction: capacity=1, fetching second page evicts the first.
#[test]
fn test_eviction_and_flush() {
    let path = "test_bufpool.db";
    let mut pf = PageFile::open(path, 4096).unwrap();
    // setup pages
    let d0 = vec![0u8; 4096];
    let d1 = vec![1u8; 4096];
    pf.write_page(0, &d0).unwrap();
    pf.allocate_page();
    pf.write_page(1, &d1).unwrap();

    let mut bp = BufferPool::new(pf, 1).unwrap();
    // fetch page 0 and unpin
    let f0 = bp.fetch_page(0).unwrap().page_no;
    bp.unpin_page(0, false);
    // fetch page 1 triggers eviction of page 0
    let frame1 = bp.fetch_page(1).unwrap();
    assert_eq!(frame1.page_no, 1);
    assert!(bp.pool.get(&0).is_none());
    remove_file(path).unwrap();
}

/// Test that dirty pages are flushed on flush_all.
#[test]
fn test_dirty_write_back() {
    let path = "test_bufpool.db";
    let mut pf = PageFile::open(path, 4096).unwrap();
    pf.write_page(0, &vec![0u8; 4096]).unwrap();
    let mut bp = BufferPool::new(pf, 2).unwrap();
    {
        let frame = bp.fetch_page(0).unwrap();
        frame.data[0] = 0xFF;
        // keep pin_count > 0
    }
    bp.unpin_page(0, true);
    bp.flush_all().unwrap();
    // reopen file to verify
    let mut pf2 = PageFile::open(path, 4096).unwrap();
    let buf = pf2.read_page(0).unwrap();
    assert_eq!(buf[0], 0xFF);
    remove_file(path).unwrap();
}
