// tx/log_manager.rs

use anyhow::{Context, Result};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Log sequence number.
pub type Lsn = u64;
/// Transaction identifier.
pub type TxId = u64;

/// Types of log records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRecordType {
    Begin,
    Commit,
    Abort,
    Update,
    // later: Compensation, Checkpoint
}

/// A log record header.
#[derive(Debug)]
struct LogRecordHeader {
    lsn: Lsn,
    prev_lsn: Option<Lsn>,
    tx_id: TxId,
    typ: LogRecordType,
    /// length in bytes of the payload (after the header)
    payload_len: u32,
}

/// A complete log record: header + payload bytes.
#[derive(Debug)]
pub struct LogRecord {
    header: LogRecordHeader,
    payload: Vec<u8>,
}

impl LogRecord {
    /// Serialize into bytes: [total_len][header][payload]
    /// total_len includes header and payload (but not the length field itself).
    fn serialize(&self) -> Vec<u8> {
        // header size: lsn(8) + prev_lsn(8) + tx_id(8) + typ(1) + payload_len(4) = 29 bytes
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

/// The LogManager handles appending and flushing WAL records.
pub struct LogManager {
    inner: Arc<Mutex<LogManagerInner>>,
}

struct LogManagerInner {
    /// On‐disk WAL file writer (append-only).
    writer: BufWriter<File>,
    /// Next LSN to assign.
    next_lsn: Lsn,
    /// Last LSN per transaction.
    last_lsn: HashMap<TxId, Lsn>,
    /// Highest LSN flushed to disk.
    flushed_lsn: Lsn,
    /// In‐memory buffer of serialized records (in-flight before flush).
    buffer: Vec<LogRecord>,
}

impl LogManager {
    /// Open or create the WAL at `path`. If path exists, appends.
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

    /// Write a `BEGIN` record for transaction.
    pub fn log_begin(&self, tx_id: TxId) -> Result<Lsn> {
        self.append_record(tx_id, LogRecordType::Begin, Vec::new())
    }

    /// Write a `COMMIT` record for transaction, and flush up through this LSN.
    pub fn log_commit(&self, tx_id: TxId) -> Result<Lsn> {
        let lsn = self.append_record(tx_id, LogRecordType::Commit, Vec::new())?;
        self.flush(lsn)?;
        Ok(lsn)
    }

    /// Write an `ABORT` record for transaction, and flush.
    pub fn log_abort(&self, tx_id: TxId) -> Result<Lsn> {
        let lsn = self.append_record(tx_id, LogRecordType::Abort, Vec::new())?;
        self.flush(lsn)?;
        Ok(lsn)
    }

    /// Write an `UPDATE` record, payload should encode (page_no, offset, before, after).
    pub fn log_update(&self, tx_id: TxId, payload: Vec<u8>) -> Result<Lsn> {
        self.append_record(tx_id, LogRecordType::Update, payload)
    }

    /// Append a record to the in-memory buffer; assign LSN and chain to prev.
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

    /// Flush all buffered records with LSN ≤ target_lsn to disk (and fsync).
    pub fn flush(&self, target_lsn: Lsn) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        // drain records up to target_lsn
        let mut to_write = Vec::new();
        while let Some(rec) = inner.buffer.first() {
            if rec.header.lsn <= target_lsn {
                to_write.push(inner.buffer.remove(0));
            } else {
                break;
            }
        }
        // serialize and write
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

    /// Return the last flushed LSN.
    pub fn flushed_lsn(&self) -> Lsn {
        let inner = self.inner.lock().unwrap();
        inner.flushed_lsn
    }
}
