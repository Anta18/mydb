

use crate::query::parser::{BinaryOp, Expr as RawExpr, Statement as RawStmt, Value as RawValue};
use crate::storage::storage::Storage;
use anyhow::{Context, Result, bail};
use std::collections::HashMap;


#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: DataType,
    pub ordinal: usize,
}


#[derive(Debug, Clone)]
pub struct TableMeta {
    pub name: String,
    pub columns: Vec<ColumnMeta>,
    pub col_index: HashMap<String, usize>,
}

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


pub struct Catalog {
    pub tables: HashMap<String, TableMeta>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, cols: &[(String, String)]) -> Result<()> {
        let key = name.to_ascii_lowercase();
        if self.tables.contains_key(&key) {
            bail!("Table '{}' already exists", name);
        }
        let mut col_index = HashMap::new();
        let mut columns = Vec::new();
        for (i, (col_name, col_type)) in cols.iter().enumerate() {
            let dt = DataType::from_str(col_type)
                .with_context(|| format!("Unknown type '{}' for '{}'", col_type, col_name))?;
            col_index.insert(col_name.to_ascii_lowercase(), i);
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

    pub fn get_table(&self, name: &str) -> Result<&TableMeta> {
        let key = name.to_ascii_lowercase();
        self.tables
            .get(&key)
            .with_context(|| format!("Unknown table '{}'", name))
    }
}


#[derive(Debug)]
pub enum BoundStmt {
    CreateTable {
        name: String,
        columns: Vec<(String, DataType)>,
    },
    CreateIndex {
        index_name: String,
        table: String,
        column: String,
        order: usize,
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
        use RawStmt::*;
        match stmt {
            CreateTable { name, columns } => {
                self.catalog.create_table(&name, &columns)?;
                let cols = columns
                    .into_iter()
                    .map(|(n, t)| (n, DataType::from_str(&t).unwrap()))
                    .collect();
                Ok(BoundStmt::CreateTable {
                    name,
                    columns: cols,
                })
            }
            CreateIndex {
                index_name,
                table,
                column,
            } => {
                let order = 4;
                self.storage
                    .create_index(&table, &column, &index_name, order)
                    .context("Failed to create index")?;
                Ok(BoundStmt::CreateIndex {
                    index_name,
                    table,
                    column,
                    order,
                })
            }
            Insert {
                table,
                columns,
                values,
            } => {
                let meta = self.catalog.get_table(&table)?;
                let mut ords = Vec::new();
                for col in columns {
                    let lc = col.to_ascii_lowercase();
                    let &o = meta
                        .col_index
                        .get(&lc)
                        .with_context(|| format!("Unknown column '{}' in '{}'", col, table))?;
                    ords.push(o);
                }
                let mut bv = Vec::new();
                for expr in values {
                    bv.push(self.bind_expr(expr, &table)?);
                }
                Ok(BoundStmt::Insert {
                    table,
                    col_ordinals: ords,
                    values: bv,
                })
            }
            Select {
                projections,
                table,
                filter,
            } => {
                let _ = self.catalog.get_table(&table)?;
                let mut bp = Vec::new();
                for expr in projections {
                    bp.push(self.bind_expr(expr.clone(), &table)?);
                }
                let bf = if let Some(f) = filter {
                    Some(self.bind_expr(f, &table)?)
                } else {
                    None
                };
                Ok(BoundStmt::Select {
                    projections: bp,
                    table,
                    filter: bf,
                })
            }
        }
    }

    fn bind_expr(&self, expr: RawExpr, table: &str) -> Result<BoundExpr> {
        use RawExpr::*;
        match expr {
            Column(c) => {
                let meta = self.catalog.get_table(table)?;
                let lc = c.to_ascii_lowercase();
                let &o = meta
                    .col_index
                    .get(&lc)
                    .with_context(|| format!("Unknown column '{}' in '{}'", c, table))?;
                let dt = meta.columns[o].data_type.clone();
                Ok(BoundExpr::Column {
                    table: table.to_string(),
                    col: c,
                    ordinal: o,
                    data_type: dt,
                })
            }
            Literal(rv) => {
                let v = match rv {
                    RawValue::Int(i) => Value::Int(i),
                    RawValue::String(s) => Value::String(s),
                };
                Ok(BoundExpr::Literal(v))
            }
            BinaryOp { left, op, right } => {
                let l = self.bind_expr(*left, table)?;
                let r = self.bind_expr(*right, table)?;
                Ok(BoundExpr::BinaryOp {
                    left: Box::new(l),
                    op,
                    right: Box::new(r),
                    data_type: DataType::Int,
                })
            }
        }
    }
}
