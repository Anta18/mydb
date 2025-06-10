// cli/utils.rs
use crate::storage::storage::Storage;
use anyhow::Result;
use csv::{ReaderBuilder, WriterBuilder};
use std::path::Path;

/// Import CSV into `table_name`, matching columns by header row.
/// Creates the table if it doesn't exist.
pub fn import_csv<P: AsRef<Path>>(storage: &mut Storage, table: &str, path: P) -> Result<()> {
    let mut rdr = ReaderBuilder::new().from_path(path)?;
    let headers = rdr.headers()?.clone();

    // Check if table exists, create it if not
    if storage.catalog.get_table(table).is_err() {
        let columns: Vec<_> = headers
            .iter()
            .map(|h| crate::storage::storage::ColumnInfo {
                name: h.to_string(),
                data_type: crate::storage::storage::DataType::String, // Default to String, will be inferred
            })
            .collect();
        storage.create_table(table.to_string(), columns)?;
    }

    // Import records
    for result in rdr.records() {
        let record = result?;
        let mut values = Vec::new();

        for val in record.iter() {
            // Try to parse as integer first, otherwise treat as string
            if let Ok(i) = val.parse::<i64>() {
                values.push(crate::query::binder::Value::Int(i));
            } else {
                values.push(crate::query::binder::Value::String(val.to_string()));
            }
        }

        let column_names: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
        storage.insert_row(table, &column_names, values)?;
    }

    Ok(())
}

/// Export table to CSV with header row.
pub fn export_csv<P: AsRef<Path>>(storage: &mut Storage, table: &str, path: P) -> Result<()> {
    let mut wtr = WriterBuilder::new().from_path(path)?;

    // Get table metadata
    let meta = storage.catalog.get_table(table)?;
    let headers: Vec<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();

    // Write header row
    wtr.write_record(&headers)?;

    // Scan table and write data rows
    for tuple in storage.scan_table(table)? {
        let row: Vec<String> = tuple
            .into_iter()
            .map(|v| match v {
                crate::query::binder::Value::Int(i) => i.to_string(),
                crate::query::binder::Value::String(s) => s,
            })
            .collect();
        wtr.write_record(&row)?;
    }

    wtr.flush()?;
    Ok(())
}

/// Helper function to infer column types from CSV data
pub fn infer_csv_schema<P: AsRef<Path>>(
    path: P,
) -> Result<Vec<crate::storage::storage::ColumnInfo>> {
    let mut rdr = ReaderBuilder::new().from_path(path)?;
    let headers = rdr.headers()?.clone();

    let mut columns = Vec::new();

    // Initialize all columns as potentially integers
    let mut is_int: Vec<bool> = vec![true; headers.len()];

    // Sample first few rows to infer types
    for result in rdr.records().take(100) {
        // Sample first 100 rows
        let record = result?;
        for (i, val) in record.iter().enumerate() {
            if is_int[i] && val.parse::<i64>().is_err() {
                is_int[i] = false;
            }
        }
    }

    // Create column info based on inference
    for (i, header) in headers.iter().enumerate() {
        let data_type = if is_int[i] {
            crate::storage::storage::DataType::Int
        } else {
            crate::storage::storage::DataType::String
        };

        columns.push(crate::storage::storage::ColumnInfo {
            name: header.to_string(),
            data_type,
        });
    }

    Ok(columns)
}

/// Import CSV with type inference
pub fn import_csv_with_inference<P: AsRef<Path>>(
    storage: &mut Storage,
    table: &str,
    path: P,
) -> Result<()> {
    // Infer schema first
    let columns = infer_csv_schema(&path)?;

    // Create table with inferred schema
    if storage.catalog.get_table(table).is_err() {
        storage.create_table(table.to_string(), columns)?;
    }

    // Now import the data
    import_csv(storage, table, path)
}
