use std::fs::remove_file;
use std::path::Path;
use storage::pagefile::PageFile;

#[test]
fn test_open_create_file() {
    let path = "test_pagefile.db";
    if Path::new(path).exists() {
        remove_file(path).unwrap();
    }
    let pf = PageFile::open(path, 4096).expect("open/create failed");
    assert!(Path::new(path).exists());
    remove_file(path).unwrap();
}

#[test]
fn test_read_write_single_page() {
    let path = "test_pagefile.db";
    let mut pf = PageFile::open(path, 4096).unwrap();
    let data = vec![0xABu8; 4096];
    pf.write_page(0, &data).unwrap();
    let buf = pf.read_page(0).unwrap();
    assert_eq!(buf, data);
    remove_file(path).unwrap();
}

#[test]
fn test_write_page_overflow() {
    let path = "test_pagefile.db";
    let mut pf = PageFile::open(path, 4096).unwrap();
    let data = vec![0u8; 5000];
    assert!(pf.write_page(0, &data).is_err());
    remove_file(path).unwrap();
}

#[test]
fn test_allocate_and_count_pages() {
    let path = "test_pagefile.db";
    let mut pf = PageFile::open(path, 4096).unwrap();
    let initial = pf.num_pages().unwrap();
    let new_page = pf.allocate_page().unwrap();
    assert_eq!(new_page, initial);
    assert_eq!(pf.num_pages().unwrap(), initial + 1);
    remove_file(path).unwrap();
}

#[test]
fn test_sync_all() {
    let path = "test_pagefile.db";
    let mut pf = PageFile::open(path, 4096).unwrap();
    let data = vec![0xCDu8; 4096];
    pf.write_page(0, &data).unwrap();
    pf.sync_all().unwrap();
    let buf = pf.read_page(0).unwrap();
    assert_eq!(buf, data);
    remove_file(path).unwrap();
}
