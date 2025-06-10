// storage/storage.rs

use crate::index::node_serializer::{LeafNodeSerializer, NodeHeader, NodeType};
use crate::storage::buffer_pool::BufferPool;
use crate::storage::free_list::FreeList;
use crate::storage::pagefile::PageFile;
use crate::storage::record::{Page as RecordPage, RID};
use anyhow::{Result, anyhow};
use std::collections::HashMap;

/// Metadata for a B⁺-tree index
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table: String,
    pub column: String,
    pub order: usize,
    pub root_page: u64,
}

/// Column metadata
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub enum DataType {
    Int,
    String,
}

/// Table metadata
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub records: Vec<RID>,
}

/// System catalog for tables and indexes
#[derive(Debug)]
pub struct Catalog {
    pub tables: HashMap<String, TableInfo>,
    pub indexes: HashMap<String, Vec<IndexInfo>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: String, columns: Vec<ColumnInfo>) -> Result<()> {
        if self.tables.contains_key(&name) {
            return Err(anyhow!("Table '{}' already exists", name));
        }
        let table = TableInfo {
            name: name.clone(),
            columns,
            records: Vec::new(),
        };
        self.tables.insert(name, table);
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Result<&TableInfo> {
        self.tables
            .get(name)
            .ok_or_else(|| anyhow!("Table '{}' not found", name))
    }

    pub fn get_table_mut(&mut self, name: &str) -> Result<&mut TableInfo> {
        self.tables
            .get_mut(name)
            .ok_or_else(|| anyhow!("Table '{}' not found", name))
    }

    pub fn create_index(
        &mut self,
        table: String,
        column: String,
        index_name: String,
        order: usize,
        root_page: u64,
    ) {
        let info = IndexInfo {
            name: index_name,
            table: table.clone(),
            column,
            order,
            root_page,
        };
        self.indexes.entry(table).or_default().push(info);
    }

    pub fn get_indexes(&self, table: &str) -> Vec<IndexInfo> {
        self.indexes.get(table).cloned().unwrap_or_default()
    }
}

/// Enhanced storage engine
pub struct Storage {
    pub buffer_pool: BufferPool,
    pub free_list: FreeList,
    pub page_size: usize,
    pub catalog: Catalog,
}

impl Storage {
    pub fn new(path: &str, page_size: usize, pool_size: usize) -> Result<Self> {
        let pf = PageFile::open(path, page_size)?;
        let bp = BufferPool::new(pf, pool_size)?;
        let fl = FreeList::new();
        Ok(Storage {
            buffer_pool: bp,
            free_list: fl,
            page_size,
            catalog: Catalog::new(),
        })
    }

    /// Insert raw bytes into a page (no WAL here).
    pub fn insert(&mut self, data: &[u8]) -> Result<RID> {
        let needed = data.len() + RecordPage::SLOT_ENTRY_SIZE;
        let page_no = if let Some(pn) = self.free_list.choose_page(needed) {
            pn
        } else {
            let pn = self.buffer_pool.pagefile.allocate_page()?;
            let page = RecordPage::new(pn, self.page_size);
            self.free_list.register(pn, page.free_space());
            pn
        };

        let frame = self.buffer_pool.fetch_page(page_no)?;
        let mut page = RecordPage::from_bytes(frame.data.clone(), self.page_size);
        let rid = page.insert_tuple(data)?;
        frame.data = page.to_bytes();
        let free = RecordPage::from_bytes(frame.data.clone(), self.page_size).free_space();
        self.buffer_pool.unpin_page(page_no, true);
        self.free_list.register(page_no, free);
        Ok(rid)
    }

    /// Insert a row into a table (no WAL here).
    pub fn insert_row(
        &mut self,
        table_name: &str,
        columns: &[String],
        values: Vec<crate::query::binder::Value>,
    ) -> Result<()> {
        let _ = self.catalog.get_table(table_name)?;
        if columns.len() != values.len() {
            return Err(anyhow!("Column/value count mismatch"));
        }
        let row_data = self.serialize_row(&values)?;
        let rid = self.insert(&row_data)?;
        let table = self.catalog.get_table_mut(table_name)?;
        table.records.push(rid);
        Ok(())
    }

    /// Scan a table (read-only).
    pub fn scan_table(
        &mut self,
        table_name: &str,
    ) -> Result<Vec<Vec<crate::query::binder::Value>>> {
        let rids = self.catalog.get_table(table_name)?.records.clone();
        let mut rows = Vec::new();
        for rid in rids {
            let raw = self.fetch(rid)?;
            let vals = self.deserialize_row(&raw)?;
            rows.push(vals);
        }
        Ok(rows)
    }

    pub fn create_table(&mut self, name: String, cols: Vec<ColumnInfo>) -> Result<()> {
        self.catalog.create_table(name, cols)
    }

    fn serialize_row(&self, values: &[crate::query::binder::Value]) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for v in values {
            match v {
                crate::query::binder::Value::Int(i) => {
                    buf.push(0);
                    buf.extend_from_slice(&i.to_le_bytes());
                }
                crate::query::binder::Value::String(s) => {
                    buf.push(1);
                    let b = s.as_bytes();
                    buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    buf.extend_from_slice(b);
                }
            }
        }
        Ok(buf)
    }

    fn deserialize_row(&self, data: &[u8]) -> Result<Vec<crate::query::binder::Value>> {
        let mut cursor = 0;
        if data.len() < 4 {
            return Err(anyhow!("Invalid row data"));
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        cursor += 4;
        let mut vals = Vec::with_capacity(count);
        for _ in 0..count {
            let tag = data[cursor];
            cursor += 1;
            match tag {
                0 => {
                    let i = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
                    vals.push(crate::query::binder::Value::Int(i));
                    cursor += 8;
                }
                1 => {
                    let len =
                        u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    let s = String::from_utf8(data[cursor..cursor + len].to_vec())?;
                    vals.push(crate::query::binder::Value::String(s));
                    cursor += len;
                }
                _ => return Err(anyhow!("Invalid tag")),
            }
        }
        Ok(vals)
    }

    pub fn fetch(&mut self, rid: RID) -> Result<Vec<u8>> {
        let (page_no, slot) = rid;
        let frame = self.buffer_pool.fetch_page(page_no)?;
        let page = RecordPage::from_bytes(frame.data.clone(), self.page_size);
        let rec = page.get_tuple(slot).ok_or_else(|| anyhow!("Not found"))?;
        self.buffer_pool.unpin_page(page_no, false);
        Ok(rec.to_vec())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.buffer_pool.flush_all()?;
        Ok(())
    }

    /// Create a B⁺-tree index
    pub fn create_index(
        &mut self,
        table_name: &str,
        column: &str,
        index_name: &str,
        order: usize,
    ) -> Result<u64> {
        self.catalog.get_table(table_name)?;
        let root = self.buffer_pool.pagefile.allocate_page()?;

        let hdr = NodeHeader {
            node_type: NodeType::Leaf,
            key_count: 0,
            parent: 0,
        };
        let buf = LeafNodeSerializer { order }.serialize(&hdr, &[], &[], 0, self.page_size);

        {
            let frame = self.buffer_pool.fetch_page(root)?;
            frame.data.copy_from_slice(&buf);
            self.buffer_pool.unpin_page(root, true);
        }
        let free = self.page_size.saturating_sub(buf.len());
        self.free_list.register(root, free);

        self.catalog.create_index(
            table_name.to_string(),
            column.to_string(),
            index_name.to_string(),
            order,
            root,
        );
        Ok(root)
    }

    pub fn get_indexes(&self, table: &str) -> Vec<IndexInfo> {
        self.catalog.get_indexes(table)
    }
}
