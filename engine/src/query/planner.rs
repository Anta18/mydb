// src/sql/planner.rs

use crate::query::binder::{BoundExpr, BoundStmt, ColumnMeta, TableMeta, Value as BoundValue};
use crate::storage::storage::Storage;
use anyhow::{Result, bail};
use std::collections::HashMap;

////////////////////////////////////////////////////////////////////////////////
// Logical Plan Definition
////////////////////////////////////////////////////////////////////////////////

/// A relational expression before physical choice.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// CREATE TABLE (no data-producing node; for DDL handling)
    CreateTable {
        table_name: String,
        columns: Vec<(String, String)>,
    },

    /// INSERT INTO table
    Insert {
        table_name: String,
        col_ordinals: Vec<usize>,
        values: Vec<BoundExpr>,
    },

    /// A scan of an entire table
    SeqScan {
        table: String,
        alias: Option<String>,
        predicate: Option<BoundExpr>,
    },

    /// Filter rows from input
    Filter {
        input: Box<LogicalPlan>,
        predicate: BoundExpr,
    },

    /// Project expressions from input
    Projection {
        input: Box<LogicalPlan>,
        exprs: Vec<BoundExpr>,
    },
    // Placeholder for future logical joins
    // Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, on: BoundExpr },
}

////////////////////////////////////////////////////////////////////////////////
// Planner
////////////////////////////////////////////////////////////////////////////////

/// Transforms bound statements into logical plans.
pub struct Planner<'a> {
    catalog: &'a HashMap<String, TableMeta>,
    storage: &'a mut Storage,
}

impl<'a> Planner<'a> {
    /// Create a new planner with access to the catalog and storage.
    pub fn new(catalog: &'a HashMap<String, TableMeta>, storage: &'a mut Storage) -> Self {
        Planner { catalog, storage }
    }

    /// Entry: plan a bound statement into a logical plan.
    pub fn plan(&mut self, stmt: BoundStmt) -> Result<LogicalPlan> {
        match stmt {
            BoundStmt::CreateTable { name, columns } => Ok(LogicalPlan::CreateTable {
                table_name: name,
                columns,
            }),
            BoundStmt::Insert {
                table,
                col_ordinals,
                values,
            } => {
                // Check table exists
                if !self.catalog.contains_key(&table.to_ascii_lowercase()) {
                    bail!("Planner: unknown table '{}'", table);
                }
                Ok(LogicalPlan::Insert {
                    table_name: table,
                    col_ordinals,
                    values,
                })
            }
            BoundStmt::Select {
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
        // Validate table
        let table_lc = table.to_ascii_lowercase();
        let meta = self
            .catalog
            .get(&table_lc)
            .ok_or_else(|| anyhow::anyhow!("Planner: unknown table '{}'", table))?;

        // 1. Base scan
        let mut plan = LogicalPlan::SeqScan {
            table: table.clone(),
            alias: None,
            predicate: None,
        };

        // 2. Filter if present
        if let Some(pred) = filter {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: pred,
            };
        }

        // 3. Projection
        //   - If projection is *) and first expr is a wildcard literal,
        //     we could expand to all columns in order.
        //   - For now, we simply project what the user asked.
        plan = LogicalPlan::Projection {
            input: Box::new(plan),
            exprs: projections,
        };

        Ok(plan)
    }
}
