
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;





pub struct PageFile {
    file: File,
    page_size: usize,
}

impl PageFile {
    
    pub fn open<P: AsRef<Path>>(path: P, page_size: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(PageFile { file, page_size })
    }

    
    pub fn read_page(&mut self, page_no: u64) -> io::Result<Vec<u8>> {
        let offset = page_no
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Page number overflow"))?;

        let mut buf = vec![0u8; self.page_size];
        self.file.seek(SeekFrom::Start(offset))?;
        let n = self.file.read(&mut buf)?;
        if n != self.page_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("Expected {} bytes, read {} bytes", self.page_size, n),
            ));
        }
        Ok(buf)
    }

    
    
    pub fn write_page(&mut self, page_no: u64, buf: &[u8]) -> io::Result<()> {
        if buf.len() != self.page_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Buffer size {} does not match page size {}",
                    buf.len(),
                    self.page_size
                ),
            ));
        }

        let offset = page_no
            .checked_mul(self.page_size as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Page number overflow"))?;

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        self.file.sync_data()?; 
        Ok(())
    }

    
    pub fn num_pages(&mut self) -> io::Result<u64> {
        let metadata = self.file.metadata()?;
        let len = metadata.len();
        Ok((len + self.page_size as u64 - 1) / self.page_size as u64)
    }

    
    pub fn allocate_page(&mut self) -> io::Result<u64> {
        let new_page_no = self.num_pages()?;
        let zero_buf = vec![0u8; self.page_size];
        self.write_page(new_page_no, &zero_buf)?;
        Ok(new_page_no)
    }

    
    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}
