use crate::index::node_serializer::{LeafNodeSerializer, NodeHeader, NodeType};
use crate::storage::{
    buffer_pool::BufferPool,
    free_list::FreeList,
    pagefile::PageFile,
    record::{Page as RecordPage, RID},
};
use anyhow::{Result, anyhow};
use std::collections::HashMap;

/// Metadata for a B⁺-tree index
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Logical name of the index
    pub name: String,
    /// Table it indexes
    pub table: String,
    /// Column this index is on
    pub column: String,
    /// B⁺-tree order
    pub order: usize,
    /// Root page number of the tree
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
    pub records: Vec<RID>, // Track all records in this table
}

/// System catalog for managing table and index metadata
#[derive(Debug)]
pub struct Catalog {
    pub tables: HashMap<String, TableInfo>,
    pub indexes: HashMap<String, Vec<IndexInfo>>, // table_name → list of indexes
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
        let table_info = TableInfo {
            name: name.clone(),
            columns,
            records: Vec::new(),
        };
        self.tables.insert(name, table_info);
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

    /// Register a new index in the catalog
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
        self.indexes
            .entry(table)
            .or_insert_with(Vec::new)
            .push(info);
    }

    /// Get all indexes defined on a table
    pub fn get_indexes(&self, table: &str) -> Vec<IndexInfo> {
        self.indexes.get(table).cloned().unwrap_or_default()
    }
}

/// Enhanced storage engine with catalog support
pub struct Storage {
    pub buffer_pool: BufferPool,
    pub free_list: FreeList,
    pub page_size: usize,
    pub catalog: Catalog,
}

impl Storage {
    /// Initialize storage with a data file path, page size, and buffer pool capacity.
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

    /// Insert a new record, returning its RID
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
        let free_space = page.free_space();
        frame.data = page.to_bytes();
        self.buffer_pool.unpin_page(page_no, true);
        self.free_list.register(page_no, free_space);
        Ok(rid)
    }

    /// Insert a row into a specific table
    pub fn insert_row(
        &mut self,
        table_name: &str,
        columns: &[String],
        values: Vec<crate::query::binder::Value>,
    ) -> Result<()> {
        let table_info = self.catalog.get_table(table_name)?;
        if columns.len() != values.len() {
            return Err(anyhow!(
                "Column count mismatch: {} columns, {} values",
                columns.len(),
                values.len()
            ));
        }
        let row_data = self.serialize_row(&values)?;
        let rid = self.insert(&row_data)?;
        let table_info = self.catalog.get_table_mut(table_name)?;
        table_info.records.push(rid);
        Ok(())
    }

    /// Scan all records in a table
    pub fn scan_table(
        &mut self,
        table_name: &str,
    ) -> Result<Vec<Vec<crate::query::binder::Value>>> {
        let rids = {
            let table_info = self.catalog.get_table(table_name)?;
            table_info.records.clone()
        };
        let mut results = Vec::new();
        for rid in rids {
            let raw = self.fetch(rid)?;
            let vals = self.deserialize_row(&raw)?;
            results.push(vals);
        }
        Ok(results)
    }

    /// Create a new table
    pub fn create_table(&mut self, name: String, columns: Vec<ColumnInfo>) -> Result<()> {
        self.catalog.create_table(name, columns)
    }

    /// Serialize a row of values to bytes
    fn serialize_row(&self, values: &[crate::query::binder::Value]) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        result.extend_from_slice(&(values.len() as u32).to_le_bytes());
        for value in values {
            match value {
                crate::query::binder::Value::Int(i) => {
                    result.push(0);
                    result.extend_from_slice(&i.to_le_bytes());
                }
                crate::query::binder::Value::String(s) => {
                    result.push(1);
                    let b = s.as_bytes();
                    result.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    result.extend_from_slice(b);
                }
            }
        }
        Ok(result)
    }

    /// Deserialize bytes back to values
    fn deserialize_row(&self, data: &[u8]) -> Result<Vec<crate::query::binder::Value>> {
        let mut cursor = 0;
        if data.len() < 4 {
            return Err(anyhow!("Invalid row data: too short"));
        }
        let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        cursor += 4;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let tag = data[cursor];
            cursor += 1;
            match tag {
                0 => {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&data[cursor..cursor + 8]);
                    let v = i64::from_le_bytes(b);
                    values.push(crate::query::binder::Value::Int(v));
                    cursor += 8;
                }
                1 => {
                    let mut lb = [0u8; 4];
                    lb.copy_from_slice(&data[cursor..cursor + 4]);
                    let len = u32::from_le_bytes(lb) as usize;
                    cursor += 4;
                    let s = String::from_utf8(data[cursor..cursor + len].to_vec())?;
                    values.push(crate::query::binder::Value::String(s));
                    cursor += len;
                }
                _ => return Err(anyhow!("Invalid type tag: {}", tag)),
            }
        }
        Ok(values)
    }

    /// Fetch a record by RID
    pub fn fetch(&mut self, rid: RID) -> Result<Vec<u8>> {
        let (page_no, slot_no) = rid;
        let frame = self.buffer_pool.fetch_page(page_no)?;
        let page = RecordPage::from_bytes(frame.data.clone(), self.page_size);
        let rec = page
            .get_tuple(slot_no)
            .ok_or_else(|| anyhow!("Record not found"))?;
        self.buffer_pool.unpin_page(page_no, false);
        Ok(rec.to_vec())
    }

    /// Flush all pending writes to disk
    pub fn flush(&mut self) -> Result<()> {
        self.buffer_pool.flush_all()?;
        Ok(())
    }

    /// Create a new B⁺-tree index on `table_name(column)` called `index_name`.
    pub fn create_index(
        &mut self,
        table_name: &str,
        column: &str,
        index_name: &str,
        order: usize,
    ) -> Result<u64> {
        // Validate
        self.catalog.get_table(table_name)?;
        // Allocate
        let root = self.buffer_pool.pagefile.allocate_page()?;
        // Initialize leaf
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
        // Register space
        let free = self.page_size.saturating_sub(buf.len());
        self.free_list.register(root, free);
        // Catalog
        self.catalog.create_index(
            table_name.to_string(),
            column.to_string(),
            index_name.to_string(),
            order,
            root,
        );
        Ok(root)
    }

    /// Retrieve all indexes on a table
    pub fn get_indexes(&self, table_name: &str) -> Vec<IndexInfo> {
        self.catalog.get_indexes(table_name)
    }
}
