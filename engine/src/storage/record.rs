// storage/record.rs
use anyhow::{Result, anyhow};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

pub type RID = (u64, u16);

pub struct Page {
    data: Vec<u8>,
    pub page_size: usize,
}

impl Page {
    const HEADER_SIZE: usize = 8 + 2 + 2; // page_id + slot_count + free_space_off
    pub const SLOT_ENTRY_SIZE: usize = 2 + 2; // offset + length

    pub fn new(page_id: u64, page_size: usize) -> Self {
        let mut data = vec![0; page_size];
        // write page_id
        (&mut data[0..8])
            .write_u64::<LittleEndian>(page_id)
            .unwrap();
        // slot_count = 0
        (&mut data[8..10]).write_u16::<LittleEndian>(0).unwrap();
        // free_space_off = page_size
        (&mut data[10..12])
            .write_u16::<LittleEndian>(page_size as u16)
            .unwrap();
        Page { data, page_size }
    }

    pub fn from_bytes(data: Vec<u8>, page_size: usize) -> Self {
        assert_eq!(data.len(), page_size);
        Page { data, page_size }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        self.data
    }

    fn page_id(&self) -> u64 {
        let mut rdr = Cursor::new(&self.data[0..8]);
        rdr.read_u64::<LittleEndian>().unwrap()
    }

    fn slot_count(&self) -> u16 {
        let mut rdr = Cursor::new(&self.data[8..10]);
        rdr.read_u16::<LittleEndian>().unwrap()
    }

    fn free_space_off(&self) -> u16 {
        let mut rdr = Cursor::new(&self.data[10..12]);
        rdr.read_u16::<LittleEndian>().unwrap()
    }

    fn set_slot_count(&mut self, cnt: u16) {
        (&mut self.data[8..10])
            .write_u16::<LittleEndian>(cnt)
            .unwrap();
    }

    fn set_free_space_off(&mut self, off: u16) {
        (&mut self.data[10..12])
            .write_u16::<LittleEndian>(off)
            .unwrap();
    }

    pub fn slot_dir_offset(&self) -> usize {
        Self::HEADER_SIZE
    }

    pub fn payload_start(&self) -> usize {
        self.slot_dir_offset() + (self.slot_count() as usize) * Self::SLOT_ENTRY_SIZE
    }

    pub fn free_space(&self) -> usize {
        let free_off = self.free_space_off() as usize;
        free_off - self.payload_start()
    }

    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Result<RID> {
        let tuple_len = tuple.len();
        let needed = tuple_len + Self::SLOT_ENTRY_SIZE;
        if needed > self.free_space() {
            return Err(anyhow!("Not enough free space"));
        }
        // compute new free offset
        let free_off = self.free_space_off() as usize;
        let new_free_off = free_off - tuple_len;
        // write payload
        let start = new_free_off;
        let end = free_off;
        self.data[start..end].copy_from_slice(tuple);
        // write slot entry
        let slot_no = self.slot_count();
        let entry_off = self.slot_dir_offset() + (slot_no as usize) * Self::SLOT_ENTRY_SIZE;
        (&mut self.data[entry_off..entry_off + 2]).write_u16::<LittleEndian>(start as u16)?;
        (&mut self.data[entry_off + 2..entry_off + 4])
            .write_u16::<LittleEndian>(tuple_len as u16)?;
        // update header
        self.set_slot_count(slot_no + 1);
        self.set_free_space_off(new_free_off as u16);
        Ok((self.page_id(), slot_no))
    }

    pub fn get_tuple(&self, slot_no: u16) -> Option<&[u8]> {
        if slot_no >= self.slot_count() {
            return None;
        }
        let entry_off = self.slot_dir_offset() + (slot_no as usize) * Self::SLOT_ENTRY_SIZE;
        let mut rdr = Cursor::new(&self.data[entry_off..entry_off + 4]);
        let off = rdr.read_u16::<LittleEndian>().unwrap() as usize;
        let len = rdr.read_u16::<LittleEndian>().unwrap() as usize;
        Some(&self.data[off..off + len])
    }

    pub fn delete_tuple(&mut self, slot_no: u16) -> Result<()> {
        if slot_no >= self.slot_count() {
            return Err(anyhow!("Invalid slot number"));
        }
        // zero length to mark deleted (lazy)
        let entry_off = self.slot_dir_offset() + (slot_no as usize) * Self::SLOT_ENTRY_SIZE;
        (&mut self.data[entry_off + 2..entry_off + 4]).write_u16::<LittleEndian>(0)?;
        Ok(())
    }

    pub fn iter_slots(&self) -> impl Iterator<Item = (u16, &[u8])> + '_ {
        (0..self.slot_count()).filter_map(move |slot_no| {
            if let Some(tuple_data) = self.get_tuple(slot_no) {
                // Only return slots that aren't deleted (have non-zero length)
                if !tuple_data.is_empty() {
                    Some((slot_no, tuple_data))
                } else {
                    None
                }
            } else {
                None
            }
        })
    }
}
