// src/sql/physical_planner.rs

use crate::sql::binder::{BinaryOp, BoundExpr, DataType};
use crate::sql::optimizer::Optimizer;
use crate::sql::planner::LogicalPlan;
use crate::storage::storage::Storage;
use anyhow::{Result, bail};

////////////////////////////////////////////////////////////////////////////////
// Physical Plan Definition
////////////////////////////////////////////////////////////////////////////////

/// A physical operator in the execution plan.
#[derive(Debug)]
pub enum PhysicalPlan {
    /// Create the table in the catalog (DDL).
    CreateTable {
        table_name: String,
        columns: Vec<(String, DataType)>,
    },

    /// Insert a single row (DML).
    Insert {
        table_name: String,
        col_ordinals: Vec<usize>,
        values: Vec<BoundExpr>,
    },

    /// A full table scan over all pages/tuples.
    SeqScan {
        table_name: String,
        predicate: Option<BoundExpr>,
    },

    /// An index‐based scan using a B⁺-tree.
    IndexScan {
        table_name: String,
        index_name: String,
        predicate: BoundExpr, // equality predicate on the index key
    },

    /// Filter operator: applies `predicate` on tuples from `input`.
    Filter {
        input: Box<PhysicalPlan>,
        predicate: BoundExpr,
    },

    /// Projection operator: evaluates `exprs` on tuples from `input`.
    Projection {
        input: Box<PhysicalPlan>,
        exprs: Vec<BoundExpr>,
    },
}

////////////////////////////////////////////////////////////////////////////////
// Physical Planner
////////////////////////////////////////////////////////////////////////////////

/// Transforms optimized logical plans into physical plans by picking algorithms.
pub struct PhysicalPlanner<'a> {
    catalog: &'a crate::sql::binder::Catalog,
    storage: &'a mut Storage,
}

impl<'a> PhysicalPlanner<'a> {
    /// Create a new physical planner with catalog & storage access.
    pub fn new(catalog: &'a crate::sql::binder::Catalog, storage: &'a mut Storage) -> Self {
        PhysicalPlanner { catalog, storage }
    }

    /// Entry point: take a bound & optimized logical plan, produce a physical plan.
    pub fn create_physical_plan(&mut self, logical: LogicalPlan) -> Result<PhysicalPlan> {
        // First apply optimizer to the logical plan
        let optimized = Optimizer::optimize(logical)?;
        self.plan_node(optimized)
    }

    fn plan_node(&mut self, node: LogicalPlan) -> Result<PhysicalPlan> {
        use LogicalPlan::*;
        match node {
            // DDL and DML map directly
            CreateTable {
                table_name,
                columns,
            } => Ok(PhysicalPlan::CreateTable {
                table_name,
                columns,
            }),
            Insert {
                table_name,
                col_ordinals,
                values,
            } => Ok(PhysicalPlan::Insert {
                table_name,
                col_ordinals,
                values,
            }),

            // For SELECT, choose between index scan or full scan + filter
            SeqScan {
                table,
                alias: _,
                predicate,
            } => {
                if let Some(pred) = predicate.clone() {
                    // if predicate is simple equality on a primary key, use index
                    if let Some((col, op, lit)) = Self::extract_eq_pred(&pred) {
                        if let Some(idx_name) = self.catalog.get_primary_index(&table, &col) {
                            return Ok(PhysicalPlan::IndexScan {
                                table_name: table,
                                index_name: idx_name,
                                predicate: pred,
                            });
                        }
                    }
                    // else fall through to scan + filter
                }

                let mut plan = PhysicalPlan::SeqScan {
                    table_name: table.clone(),
                    predicate: None,
                };
                if let Some(pred) = predicate {
                    plan = PhysicalPlan::Filter {
                        input: Box::new(plan),
                        predicate: pred,
                    };
                }
                Ok(plan)
            }

            Filter { input, predicate } => {
                let child = self.plan_node(*input)?;
                Ok(PhysicalPlan::Filter {
                    input: Box::new(child),
                    predicate,
                })
            }
            Projection { input, exprs } => {
                let child = self.plan_node(*input)?;
                Ok(PhysicalPlan::Projection {
                    input: Box::new(child),
                    exprs,
                })
            }
        }
    }

    /// If predicate is `col = literal`, return (col, op, literal).
    fn extract_eq_pred(expr: &BoundExpr) -> Option<(String, BinaryOp, BoundExpr)> {
        if let BoundExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } = expr
        {
            // Only handle column = literal
            if let BoundExpr::Column { ref col, .. } = **left {
                return Some((col.clone(), BinaryOp::Eq, (*right).clone()));
            }
            if let BoundExpr::Column { ref col, .. } = **right {
                return Some((col.clone(), BinaryOp::Eq, (*left).clone()));
            }
        }
        None
    }
}
