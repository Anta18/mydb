
use crate::storage::storage::Storage;
use anyhow::Result;
use csv::{ReaderBuilder, WriterBuilder};
use std::path::Path;



pub fn import_csv<P: AsRef<Path>>(storage: &mut Storage, table: &str, path: P) -> Result<()> {
    let mut rdr = ReaderBuilder::new().from_path(path)?;
    let headers = rdr.headers()?.clone();

    
    if storage.catalog.get_table(table).is_err() {
        let columns: Vec<_> = headers
            .iter()
            .map(|h| crate::storage::storage::ColumnInfo {
                name: h.to_string(),
                data_type: crate::storage::storage::DataType::String, 
            })
            .collect();
        storage.create_table(table.to_string(), columns)?;
    }

    
    for result in rdr.records() {
        let record = result?;
        let mut values = Vec::new();

        for val in record.iter() {
            
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


pub fn export_csv<P: AsRef<Path>>(storage: &mut Storage, table: &str, path: P) -> Result<()> {
    let mut wtr = WriterBuilder::new().from_path(path)?;

    
    let meta = storage.catalog.get_table(table)?;
    let headers: Vec<&str> = meta.columns.iter().map(|c| c.name.as_str()).collect();

    
    wtr.write_record(&headers)?;

    
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


pub fn infer_csv_schema<P: AsRef<Path>>(
    path: P,
) -> Result<Vec<crate::storage::storage::ColumnInfo>> {
    let mut rdr = ReaderBuilder::new().from_path(path)?;
    let headers = rdr.headers()?.clone();

    let mut columns = Vec::new();

    
    let mut is_int: Vec<bool> = vec![true; headers.len()];

    
    for result in rdr.records().take(100) {
        
        let record = result?;
        for (i, val) in record.iter().enumerate() {
            if is_int[i] && val.parse::<i64>().is_err() {
                is_int[i] = false;
            }
        }
    }

    
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


pub fn import_csv_with_inference<P: AsRef<Path>>(
    storage: &mut Storage,
    table: &str,
    path: P,
) -> Result<()> {
    
    let columns = infer_csv_schema(&path)?;

    
    if storage.catalog.get_table(table).is_err() {
        storage.create_table(table.to_string(), columns)?;
    }

    
    import_csv(storage, table, path)
}
