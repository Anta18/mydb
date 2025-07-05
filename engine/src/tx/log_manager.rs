

use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};


pub type Lsn = u64;

pub type TxId = u64;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRecordType {
    Begin,
    Commit,
    Abort,
    Update,
    
}


#[derive(Debug)]
struct LogRecordHeader {
    lsn: Lsn,
    prev_lsn: Option<Lsn>,
    tx_id: TxId,
    typ: LogRecordType,
    
    payload_len: u32,
}


#[derive(Debug)]
pub struct LogRecord {
    header: LogRecordHeader,
    payload: Vec<u8>,
}

impl LogRecord {
    
    
    fn serialize(&self) -> Vec<u8> {
        
        let header_size = 8 + 8 + 8 + 1 + 4;
        let total_size = header_size + self.payload.len();
        let mut buf = Vec::with_capacity(4 + total_size);
        buf.extend_from_slice(&(total_size as u32).to_le_bytes());
        buf.extend_from_slice(&self.header.lsn.to_le_bytes());
        buf.extend_from_slice(&self.header.prev_lsn.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&self.header.tx_id.to_le_bytes());
        buf.push(self.header.typ as u8);
        buf.extend_from_slice(&self.header.payload_len.to_le_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }
}


pub struct LogManager {
    inner: Arc<Mutex<LogManagerInner>>,
}

struct LogManagerInner {
    
    writer: BufWriter<File>,
    
    next_lsn: Lsn,
    
    last_lsn: HashMap<TxId, Lsn>,
    
    flushed_lsn: Lsn,
    
    buffer: Vec<LogRecord>,
}

impl LogManager {
    
    pub fn new(path: PathBuf) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .with_context(|| format!("opening WAL file at {:?}", path))?;
        let writer = BufWriter::new(file);
        let inner = LogManagerInner {
            writer,
            next_lsn: 1,
            last_lsn: HashMap::new(),
            flushed_lsn: 0,
            buffer: Vec::new(),
        };
        Ok(LogManager {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    
    pub fn log_begin(&self, tx_id: TxId) -> Result<Lsn> {
        self.append_record(tx_id, LogRecordType::Begin, Vec::new())
    }

    
    pub fn log_commit(&self, tx_id: TxId) -> Result<Lsn> {
        let lsn = self.append_record(tx_id, LogRecordType::Commit, Vec::new())?;
        self.flush(lsn)?;
        Ok(lsn)
    }

    
    pub fn log_abort(&self, tx_id: TxId) -> Result<Lsn> {
        let lsn = self.append_record(tx_id, LogRecordType::Abort, Vec::new())?;
        self.flush(lsn)?;
        Ok(lsn)
    }

    
    pub fn log_update(&self, tx_id: TxId, payload: Vec<u8>) -> Result<Lsn> {
        self.append_record(tx_id, LogRecordType::Update, payload)
    }

    
    fn append_record(&self, tx_id: TxId, typ: LogRecordType, payload: Vec<u8>) -> Result<Lsn> {
        let mut inner = self.inner.lock().unwrap();
        let lsn = inner.next_lsn;
        let prev = inner.last_lsn.insert(tx_id, lsn);
        let header = LogRecordHeader {
            lsn,
            prev_lsn: prev,
            tx_id,
            typ,
            payload_len: payload.len() as u32,
        };
        let record = LogRecord { header, payload };
        inner.buffer.push(record);
        inner.next_lsn += 1;
        Ok(lsn)
    }

    
    pub fn flush(&self, target_lsn: Lsn) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        
        let mut to_write = Vec::new();
        while let Some(rec) = inner.buffer.first() {
            if rec.header.lsn <= target_lsn {
                to_write.push(inner.buffer.remove(0));
            } else {
                break;
            }
        }
        
        for rec in to_write.iter() {
            let bytes = rec.serialize();
            inner
                .writer
                .write_all(&bytes)
                .with_context(|| format!("writing WAL record lsn={}", rec.header.lsn))?;
        }
        inner.writer.flush().context("flushing WAL BufWriter")?;
        inner
            .writer
            .get_ref()
            .sync_data()
            .context("fsync WAL file")?;
        inner.flushed_lsn = target_lsn;
        Ok(())
    }

    
    pub fn flushed_lsn(&self) -> Lsn {
        let inner = self.inner.lock().unwrap();
        inner.flushed_lsn
    }
}
