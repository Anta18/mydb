// src/query/binder.rs

use crate::query::parser::{BinaryOp, Expr as RawExpr, Statement as RawStmt, Value as RawValue};
use crate::storage::storage::Storage;
use anyhow::{Context, Result, bail};
use std::collections::HashMap;

/// Metadata for one column in a table.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: DataType,
    pub ordinal: usize,
}

/// Metadata for a table.
#[derive(Debug, Clone)]
pub struct TableMeta {
    pub name: String,
    pub columns: Vec<ColumnMeta>,
    /// Map from lowercase column name → ordinal for fast lookup
    pub col_index: HashMap<String, usize>,
}

/// In-memory catalog of tables (populate from CREATE TABLE).
pub struct Catalog {
    tables: HashMap<String, TableMeta>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
        }
    }

    /// Register a new table; error if it already exists.
    pub fn create_table(&mut self, name: &str, cols: &[(String, String)]) -> Result<()> {
        let key = name.to_ascii_lowercase();
        if self.tables.contains_key(&key) {
            bail!("Table '{}' already exists", name);
        }
        let mut col_index = HashMap::new();
        let mut columns = Vec::with_capacity(cols.len());
        for (i, (col_name, col_type_str)) in cols.iter().enumerate() {
            let dt = DataType::from_str(col_type_str).with_context(|| {
                format!("Unknown type '{}' for column {}", col_type_str, col_name)
            })?;
            let name_lc = col_name.to_ascii_lowercase();
            col_index.insert(name_lc.clone(), i);
            columns.push(ColumnMeta {
                name: col_name.clone(),
                data_type: dt,
                ordinal: i,
            });
        }
        self.tables.insert(
            key,
            TableMeta {
                name: name.to_string(),
                columns,
                col_index,
            },
        );
        Ok(())
    }

    /// Look up table metadata by name (case-insensitive).
    pub fn get_table(&self, name: &str) -> Result<&TableMeta> {
        self.tables
            .get(&name.to_ascii_lowercase())
            .with_context(|| format!("Unknown table '{}'", name))
    }
}

/// Supported column types.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    Varchar,
}

impl DataType {
    pub fn from_str(s: &str) -> Option<Self> {
        match &s.to_ascii_lowercase()[..] {
            "int" | "integer" => Some(DataType::Int),
            "varchar" | "text" | "string" => Some(DataType::Varchar),
            _ => None,
        }
    }
}

/// Bound versions of expressions/statements, with concrete table and column IDs.
#[derive(Debug)]
pub enum BoundStmt {
    CreateTable {
        name: String,
        columns: Vec<(String, DataType)>,
    },
    Insert {
        table: String,
        col_ordinals: Vec<usize>,
        values: Vec<BoundExpr>,
    },
    Select {
        projections: Vec<BoundExpr>,
        table: String,
        filter: Option<BoundExpr>,
    },
}

/// Bound expression, where column refs carry ordinals.
#[derive(Debug, Clone)]
pub enum BoundExpr {
    Column {
        table: String,
        col: String,
        ordinal: usize,
        data_type: DataType,
    },
    Literal(Value),
    BinaryOp {
        left: Box<BoundExpr>,
        op: BinaryOp,
        right: Box<BoundExpr>,
        data_type: DataType,
    },
}

#[derive(Debug, Clone)]
pub enum Value {
    Int(i64),
    String(String),
}

pub struct Binder<'a> {
    catalog: &'a mut Catalog,
    storage: &'a mut Storage,
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a mut Catalog, storage: &'a mut Storage) -> Self {
        Binder { catalog, storage }
    }

    pub fn bind(&mut self, stmt: RawStmt) -> Result<BoundStmt> {
        match stmt {
            RawStmt::CreateTable { name, columns } => {
                // update catalog
                self.catalog.create_table(&name, &columns)?;
                // emit bound
                let cols = columns
                    .into_iter()
                    .map(|(n, t)| {
                        let dt = DataType::from_str(&t).unwrap();
                        (n, dt)
                    })
                    .collect();
                Ok(BoundStmt::CreateTable {
                    name,
                    columns: cols,
                })
            }
            RawStmt::Insert {
                table,
                columns,
                values,
            } => {
                let meta = self.catalog.get_table(&table)?;
                // match supplied columns
                let mut ords = Vec::with_capacity(columns.len());
                for col in columns {
                    let lc = col.to_ascii_lowercase();
                    let &ord = meta.col_index.get(&lc).with_context(|| {
                        format!("Unknown column '{}' in table '{}'", col, table)
                    })?;
                    ords.push(ord);
                }
                // bind expressions
                let mut bound_vals = Vec::with_capacity(values.len());
                for expr in values {
                    let b = self.bind_expr(expr, &table)?;
                    bound_vals.push(b);
                }
                Ok(BoundStmt::Insert {
                    table,
                    col_ordinals: ords,
                    values: bound_vals,
                })
            }
            RawStmt::Select {
                projections,
                table,
                filter,
            } => {
                let meta = self.catalog.get_table(&table)?;
                // bind each projection
                let mut bound_proj = Vec::with_capacity(projections.len());
                for expr in projections {
                    bound_proj.push(self.bind_expr(expr.clone(), &table)?);
                }
                // bind filter if any
                let bound_filter = match filter {
                    Some(f) => Some(self.bind_expr(f, &table)?),
                    None => None,
                };
                Ok(BoundStmt::Select {
                    projections: bound_proj,
                    table,
                    filter: bound_filter,
                })
            }
        }
    }

    fn bind_expr(&self, expr: RawExpr, table: &str) -> Result<BoundExpr> {
        match expr {
            RawExpr::Column(col) => {
                let meta = self.catalog.get_table(table)?;
                let lc = col.to_ascii_lowercase();
                let &ord = meta
                    .col_index
                    .get(&lc)
                    .with_context(|| format!("Unknown column '{}' in table '{}'", col, table))?;
                let dt = meta.columns[ord].data_type.clone();
                Ok(BoundExpr::Column {
                    table: table.to_string(),
                    col,
                    ordinal: ord,
                    data_type: dt,
                })
            }
            RawExpr::Literal(rv) => {
                let val = match rv {
                    RawValue::Int(i) => Value::Int(i),
                    RawValue::String(s) => Value::String(s),
                };
                Ok(BoundExpr::Literal(val))
            }
            RawExpr::BinaryOp { left, op, right } => {
                let left_b = self.bind_expr(*left, table)?;
                let right_b = self.bind_expr(*right, table)?;
                // type-check: only int comparisons for now
                let dt = match (&left_b, &right_b) {
                    (BoundExpr::Literal(Value::Int(_)), BoundExpr::Literal(Value::Int(_)))
                    | (
                        BoundExpr::Column {
                            data_type: DataType::Int,
                            ..
                        },
                        BoundExpr::Column {
                            data_type: DataType::Int,
                            ..
                        },
                    )
                    | (
                        BoundExpr::Column {
                            data_type: DataType::Int,
                            ..
                        },
                        BoundExpr::Literal(Value::Int(_)),
                    )
                    | (
                        BoundExpr::Literal(Value::Int(_)),
                        BoundExpr::Column {
                            data_type: DataType::Int,
                            ..
                        },
                    ) => DataType::Int,
                    _ => bail!("Type mismatch in binary operator"),
                };
                Ok(BoundExpr::BinaryOp {
                    left: Box::new(left_b),
                    op,
                    right: Box::new(right_b),
                    data_type: dt,
                })
            }
        }
    }
}
