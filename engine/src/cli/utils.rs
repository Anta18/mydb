use crate::storage::Storage;
use anyhow::Result;
use csv::{ReaderBuilder, WriterBuilder};
use std::path::Path;

/// Import CSV into `table_name`, matching columns by header row.
pub fn import_csv<P: AsRef<Path>>(storage: &mut Storage, table: &str, path: P) -> Result<()> {
    let mut rdr = ReaderBuilder::new().from_path(path)?;
    let headers = rdr.headers()?.clone();
    for result in rdr.records() {
        let record = result?;
        let mut values = Vec::new();
        for val in record.iter() {
            if let Ok(i) = val.parse::<i64>() {
                values.push(crate::sql::binder::Value::Int(i));
            } else {
                values.push(crate::sql::binder::Value::String(val.to_string()));
            }
        }
        storage.insert_row(
            table,
            &headers.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            values,
        )?;
    }
    Ok(())
}

/// Export table to CSV with header row.
pub fn export_csv<P: AsRef<Path>>(storage: &Storage, table: &str, path: P) -> Result<()> {
    let mut wtr = WriterBuilder::new().from_path(path)?;
    let meta = storage.catalog.get_table(table)?;
    let headers: Vec<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();
    wtr.write_record(&headers)?;
    for tuple in storage.scan_table(table)? {
        let row: Vec<String> = tuple
            .into_iter()
            .map(|v| match v {
                crate::sql::binder::Value::Int(i) => i.to_string(),
                crate::sql::binder::Value::String(s) => s,
            })
            .collect();
        wtr.write_record(&row)?;
    }
    wtr.flush()?;
    Ok(())
}
