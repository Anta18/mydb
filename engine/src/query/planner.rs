// query/planner.rs

use crate::query::binder::{BoundExpr, BoundStmt, DataType, TableMeta};
use crate::storage::storage::Storage;
use anyhow::{Result, anyhow, bail};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    CreateTable {
        table_name: String,
        columns: Vec<(String, DataType)>,
    },
    CreateIndex {
        index_name: String,
        table: String,
        column: String,
        order: usize,
    },
    Insert {
        table_name: String,
        col_ordinals: Vec<usize>,
        values: Vec<BoundExpr>,
    },
    SeqScan {
        table: String,
        predicate: Option<BoundExpr>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: BoundExpr,
    },
    Projection {
        input: Box<LogicalPlan>,
        exprs: Vec<BoundExpr>,
    },
}

pub struct Planner<'a> {
    catalog: &'a HashMap<String, TableMeta>,
    storage: &'a mut Storage,
}

impl<'a> Planner<'a> {
    pub fn new(catalog: &'a HashMap<String, TableMeta>, storage: &'a mut Storage) -> Self {
        Planner { catalog, storage }
    }

    pub fn plan(&mut self, stmt: BoundStmt) -> Result<LogicalPlan> {
        use BoundStmt::*;
        match stmt {
            CreateTable { name, columns } => Ok(LogicalPlan::CreateTable {
                table_name: name,
                columns,
            }),
            CreateIndex {
                index_name,
                table,
                column,
                order,
            } => Ok(LogicalPlan::CreateIndex {
                index_name,
                table,
                column,
                order,
            }),
            Insert {
                table,
                col_ordinals,
                values,
            } => {
                let key = table.to_ascii_lowercase();
                if !self.catalog.contains_key(&key) {
                    bail!("Unknown table '{}'", table);
                }
                Ok(LogicalPlan::Insert {
                    table_name: table,
                    col_ordinals,
                    values,
                })
            }
            Select {
                projections,
                table,
                filter,
            } => self.plan_select(table, projections, filter),
        }
    }

    fn plan_select(
        &mut self,
        table: String,
        projections: Vec<BoundExpr>,
        filter: Option<BoundExpr>,
    ) -> Result<LogicalPlan> {
        let key = table.to_ascii_lowercase();
        // Use ok_or_else since `with_context` isn't on Option
        let _ = self
            .catalog
            .get(&key)
            .ok_or_else(|| anyhow!("Unknown table '{}'", table))?;
        let mut plan = LogicalPlan::SeqScan {
            table: table.clone(),
            predicate: None,
        };
        if let Some(pred) = filter {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: pred,
            };
        }
        plan = LogicalPlan::Projection {
            input: Box::new(plan),
            exprs: projections,
        };
        Ok(plan)
    }
}
