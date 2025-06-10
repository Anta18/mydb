// src/tx/recovery_manager.rs

use crate::storage::pagefile::PageFile;
use crate::storage::storage::Storage;
use crate::tx::log_manager::{LogManager, LogRecord, LogRecordType, Lsn, TxId};
use anyhow::{Context, Result};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::RwLock; // Changed from std::sync::Mutex

/// A log record header (making fields public for recovery).
#[derive(Debug)]
pub struct LogRecordHeader {
    pub lsn: Lsn,
    pub prev_lsn: Option<Lsn>,
    pub tx_id: TxId,
    pub typ: LogRecordType,
    pub payload_len: u32,
}

/// A complete log record: header + payload bytes (making fields public for recovery).
#[derive(Debug)]
pub struct RecoveryLogRecord {
    pub header: LogRecordHeader,
    pub payload: Vec<u8>,
}

/// RecoveryManager drives crash recovery using the WAL and the storage layer.
pub struct RecoveryManager {
    wal_path: PathBuf,
    storage: Arc<RwLock<Storage>>, // Changed from Mutex to RwLock
}

impl RecoveryManager {
    pub fn new(wal_path: PathBuf, storage: Arc<RwLock<Storage>>) -> Self {
        RecoveryManager { wal_path, storage }
    }

    /// Run full recovery: analysis, redo, undo.
    pub async fn recover(&self) -> Result<()> {
        // Made async
        // Open WAL
        let mut file = File::open(&self.wal_path)
            .with_context(|| format!("opening WAL file for recovery: {:?}", self.wal_path))?;
        // 1. Analysis pass
        let (dirty_pages, tx_status, tx_last_lsn) = self.analysis_pass(&mut file)?;
        // 2. Redo pass
        self.redo_pass(&mut file, &dirty_pages).await?; // Made async
        // 3. Undo pass
        self.undo_pass(&tx_status, &tx_last_lsn).await?; // Made async
        Ok(())
    }

    /// Read through all WAL, collecting:
    /// - dirty_pages: pages that have been updated (lsn of first update)
    /// - tx_status: for each tx, whether committed (true), aborted (false), or still active (None)
    /// - tx_last_lsn: last LSN seen for each tx
    fn analysis_pass(
        &self,
        file: &mut File,
    ) -> Result<(
        HashSet<u64>,
        HashMap<TxId, Option<bool>>,
        HashMap<TxId, Lsn>,
    )> {
        let mut dirty_pages = HashSet::new();
        let mut tx_status: HashMap<TxId, Option<bool>> = HashMap::new();
        let mut tx_last_lsn: HashMap<TxId, Lsn> = HashMap::new();
        file.rewind()?;
        loop {
            // Read record size
            let mut len_buf = [0u8; 4];
            if file.read_exact(&mut len_buf).is_err() {
                break;
            }
            let rec_size = u32::from_le_bytes(len_buf) as usize;
            let mut rec_buf = vec![0u8; rec_size];
            file.read_exact(&mut rec_buf)?;
            let record = Self::deserialize_record(&rec_buf)?;
            let hdr = &record.header;
            // Track last LSN per tx
            tx_last_lsn.insert(hdr.tx_id, hdr.lsn);
            match hdr.typ {
                LogRecordType::Begin => {
                    tx_status.insert(hdr.tx_id, None);
                }
                LogRecordType::Update => {
                    // payload begins with page_no (u64)
                    let page_no = u64::from_le_bytes(record.payload[0..8].try_into().unwrap());
                    dirty_pages.insert(page_no);
                }
                LogRecordType::Commit => {
                    tx_status.insert(hdr.tx_id, Some(true));
                }
                LogRecordType::Abort => {
                    tx_status.insert(hdr.tx_id, Some(false));
                }
            }
        }
        Ok((dirty_pages, tx_status, tx_last_lsn))
    }

    /// Redo all updates for pages still marked dirty, in LSN order.
    async fn redo_pass(&self, file: &mut File, dirty_pages: &HashSet<u64>) -> Result<()> {
        // Made async
        file.rewind()?;
        while let Some(record) = Self::next_record(file)? {
            if record.header.typ == LogRecordType::Update {
                let payload = &record.payload;
                let page_no = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                if !dirty_pages.contains(&page_no) {
                    continue; // already clean
                }
                // payload: [page_no(8)|offset(4)|before(..)|after(..)]
                let offset = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as u64;
                let after = &payload[12..];

                // Lock storage for this operation
                let mut storage = self.storage.write().await; // Changed to async write lock

                // apply after-image
                let mut page = storage.buffer_pool.pagefile.read_page(page_no)?;
                page[offset as usize..offset as usize + after.len()].copy_from_slice(after);
                storage.buffer_pool.pagefile.write_page(page_no, &page)?;

                // storage lock is automatically released here
            }
        }
        Ok(())
    }

    /// Undo any uncommitted transactions by walking their LSN chain backward.
    async fn undo_pass(
        // Made async
        &self,
        tx_status: &HashMap<TxId, Option<bool>>,
        tx_last_lsn: &HashMap<TxId, Lsn>,
    ) -> Result<()> {
        for (&tx, status) in tx_status.iter() {
            if status.is_none() {
                // active at crash → needs undo
                let mut lsn = tx_last_lsn[&tx];
                while lsn > 0 {
                    let record = self.fetch_record(lsn)?;
                    if record.header.typ == LogRecordType::Update {
                        // payload: after/then before-image follows offset
                        let payload = &record.payload;
                        let page_no = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                        let offset = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as u64;
                        // before-image length = rest / 2
                        let half = (payload.len() - 12) / 2;
                        let before = &payload[12..12 + half];

                        // Lock storage for this operation
                        let mut storage = self.storage.write().await; // Changed to async write lock

                        // apply before-image
                        let mut page = storage.buffer_pool.pagefile.read_page(page_no)?;
                        page[offset as usize..offset as usize + before.len()]
                            .copy_from_slice(before);
                        storage.buffer_pool.pagefile.write_page(page_no, &page)?;

                        // storage lock is automatically released here
                    }
                    // follow prev_lsn
                    lsn = record.header.prev_lsn.unwrap_or(0);
                }
                // write abort for this tx
                let log_manager = LogManager::new(self.wal_path.clone())?;
                log_manager.log_abort(tx)?;
            }
        }
        Ok(())
    }

    /// Read and deserialize the next log record from the current file position.
    fn next_record(file: &mut File) -> Result<Option<RecoveryLogRecord>> {
        let mut len_buf = [0u8; 4];
        if file.read_exact(&mut len_buf).is_err() {
            return Ok(None);
        }
        let rec_size = u32::from_le_bytes(len_buf) as usize;
        let mut rec_buf = vec![0u8; rec_size];
        file.read_exact(&mut rec_buf)?;
        Ok(Some(Self::deserialize_record(&rec_buf)?))
    }

    /// Fetch a specific log record by scanning forward to its LSN.
    /// Inefficient but acceptable for recovery on small logs.
    fn fetch_record(&self, target_lsn: Lsn) -> Result<RecoveryLogRecord> {
        let mut file = File::open(&self.wal_path)?;
        while let Some(record) = Self::next_record(&mut file)? {
            if record.header.lsn == target_lsn {
                return Ok(record);
            }
        }
        anyhow::bail!("LSN {} not found in WAL", target_lsn);
    }

    /// Deserialize header+payload from record bytes.
    fn deserialize_record(buf: &[u8]) -> Result<RecoveryLogRecord> {
        // skip size, already removed
        let mut pos = 0;
        let read_u64 = |b: &[u8]| u64::from_le_bytes(b.try_into().unwrap());
        let lsn = read_u64(&buf[pos..pos + 8]);
        pos += 8;
        let prev = read_u64(&buf[pos..pos + 8]);
        pos += 8;
        let tx_id = read_u64(&buf[pos..pos + 8]);
        pos += 8;
        let typ = match buf[pos] {
            0 => LogRecordType::Begin,
            1 => LogRecordType::Commit,
            2 => LogRecordType::Abort,
            3 => LogRecordType::Update,
            _ => unreachable!(),
        };
        pos += 1;
        let payload_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let payload = buf[pos..pos + payload_len].to_vec();
        Ok(RecoveryLogRecord {
            header: LogRecordHeader {
                lsn,
                prev_lsn: if prev == 0 { None } else { Some(prev) },
                tx_id,
                typ,
                payload_len: payload_len as u32,
            },
            payload,
        })
    }
}
