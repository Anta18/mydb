// query/physical_planner.rs

use crate::query::binder::{BoundExpr, DataType};
use crate::query::optimizer::Optimizer;
use crate::query::parser::BinaryOp; // Import BinaryOp from parser module
use crate::query::planner::LogicalPlan;
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
    catalog: &'a crate::query::binder::Catalog,
    storage: &'a mut Storage,
}

impl<'a> PhysicalPlanner<'a> {
    /// Create a new physical planner with catalog & storage access.
    pub fn new(catalog: &'a crate::query::binder::Catalog, storage: &'a mut Storage) -> Self {
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
                    if let Some((col, _op, _lit)) = Self::extract_eq_pred(&pred) {
                        // For now, we'll skip index optimization since get_primary_index doesn't exist
                        // This is where you'd check for available indexes:
                        // if let Some(idx_name) = self.get_primary_index(&table, &col) {
                        //     return Ok(PhysicalPlan::IndexScan {
                        //         table_name: table,
                        //         index_name: idx_name,
                        //         predicate: pred,
                        //     });
                        // }
                    }
                    // Fall through to scan + filter
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
                return Some((col.clone(), BinaryOp::Eq, (**right).clone())); // Dereference the Box
            }
            if let BoundExpr::Column { ref col, .. } = **right {
                return Some((col.clone(), BinaryOp::Eq, (**left).clone())); // Dereference the Box
            }
        }
        None
    }

    // Helper method placeholder for index lookup
    // You would need to implement this based on your catalog structure
    #[allow(dead_code)]
    fn get_primary_index(&self, table: &str, col: &str) -> Option<String> {
        // This is a placeholder implementation
        // You would need to add index metadata to your Catalog struct
        // and implement the actual index lookup logic here
        None
    }
}
