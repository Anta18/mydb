// storage/storage.rs
use crate::storage::{
    buffer_pool::BufferPool,
    free_list::FreeList,
    pagefile::PageFile,
    record::{Page as RecordPage, RID},
};
use anyhow::{Result, anyhow};
use std::collections::HashMap;

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

/// System catalog for managing table metadata
#[derive(Debug)]
pub struct Catalog {
    pub tables: HashMap<String, TableInfo>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
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
        // choose existing page or allocate new
        let page_no = if let Some(pn) = self.free_list.choose_page(needed) {
            pn
        } else {
            let pn = self.buffer_pool.pagefile.allocate_page()?;
            // register fresh page
            let page = RecordPage::new(pn, self.page_size);
            self.free_list.register(pn, page.free_space());
            pn
        };

        // fetch into buffer
        let frame = self.buffer_pool.fetch_page(page_no)?;
        // wrap raw bytes into RecordPage
        let mut page = RecordPage::from_bytes(frame.data.clone(), self.page_size);
        // insert tuple
        let rid = page.insert_tuple(data)?;
        // update free space before moving page
        let free_space = page.free_space();
        // write back
        frame.data = page.to_bytes();
        self.buffer_pool.unpin_page(page_no, true);
        // update free list
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
        // Validate table exists and columns match
        let table_info = self.catalog.get_table(table_name)?;

        if columns.len() != values.len() {
            return Err(anyhow!(
                "Column count mismatch: {} columns, {} values",
                columns.len(),
                values.len()
            ));
        }

        // Serialize the row data
        let row_data = self.serialize_row(&values)?;

        // Insert the raw data and get RID
        let rid = self.insert(&row_data)?;

        // Update catalog to track this record
        let table_info = self.catalog.get_table_mut(table_name)?;
        table_info.records.push(rid);

        Ok(())
    }

    /// Scan all records in a table
    pub fn scan_table(
        &mut self,
        table_name: &str,
    ) -> Result<Vec<Vec<crate::query::binder::Value>>> {
        // Clone the RIDs to avoid borrowing issues
        let rids = {
            let table_info = self.catalog.get_table(table_name)?;
            table_info.records.clone()
        };

        let mut results = Vec::new();
        for rid in rids {
            let raw_data = self.fetch(rid)?;
            let values = self.deserialize_row(&raw_data)?;
            results.push(values);
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

        // Write number of values
        result.extend_from_slice(&(values.len() as u32).to_le_bytes());

        for value in values {
            match value {
                crate::query::binder::Value::Int(i) => {
                    result.push(0); // Type tag for Int
                    result.extend_from_slice(&i.to_le_bytes());
                }
                crate::query::binder::Value::String(s) => {
                    result.push(1); // Type tag for String
                    let bytes = s.as_bytes();
                    result.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    result.extend_from_slice(bytes);
                }
            }
        }

        Ok(result)
    }

    /// Deserialize bytes back to values
    fn deserialize_row(&self, data: &[u8]) -> Result<Vec<crate::query::binder::Value>> {
        let mut cursor = 0;
        let mut values = Vec::new();

        if data.len() < 4 {
            return Err(anyhow!("Invalid row data: too short"));
        }

        // Read number of values
        let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        cursor += 4;

        for _ in 0..count {
            if cursor >= data.len() {
                return Err(anyhow!("Invalid row data: unexpected end"));
            }

            let type_tag = data[cursor];
            cursor += 1;

            match type_tag {
                0 => {
                    // Int
                    if cursor + 8 > data.len() {
                        return Err(anyhow!("Invalid row data: insufficient data for int"));
                    }
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&data[cursor..cursor + 8]);
                    let value = i64::from_le_bytes(bytes);
                    values.push(crate::query::binder::Value::Int(value));
                    cursor += 8;
                }
                1 => {
                    // String
                    if cursor + 4 > data.len() {
                        return Err(anyhow!(
                            "Invalid row data: insufficient data for string length"
                        ));
                    }
                    let mut len_bytes = [0u8; 4];
                    len_bytes.copy_from_slice(&data[cursor..cursor + 4]);
                    let len = u32::from_le_bytes(len_bytes) as usize;
                    cursor += 4;

                    if cursor + len > data.len() {
                        return Err(anyhow!("Invalid row data: insufficient data for string"));
                    }

                    let string_bytes = &data[cursor..cursor + len];
                    let value = String::from_utf8(string_bytes.to_vec())
                        .map_err(|e| anyhow!("Invalid UTF-8 in string: {}", e))?;
                    values.push(crate::query::binder::Value::String(value));
                    cursor += len;
                }
                _ => return Err(anyhow!("Invalid type tag: {}", type_tag)),
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
            .ok_or_else(|| anyhow::anyhow!("Record not found"))?;
        self.buffer_pool.unpin_page(page_no, false);
        Ok(rec.to_vec())
    }

    /// Flush all pending writes to disk
    pub fn flush(&mut self) -> Result<()> {
        self.buffer_pool.flush_all()?;
        Ok(())
    }
}
