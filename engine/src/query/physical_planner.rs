// query/physical_planner.rs

use crate::query::binder::{BoundExpr, DataType};
use crate::query::optimizer::Optimizer;
use crate::query::parser::BinaryOp;
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

    /// Create an index (handled at bind time, no runtime action).
    // (Optional: you can add a CreateIndex variant if you want explicit DDL execution.)

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

    /// An index-based scan using a B⁺-tree.
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

    /// Entry point: take an optimized logical plan, produce a physical plan.
    pub fn create_physical_plan(&mut self, logical: LogicalPlan) -> Result<PhysicalPlan> {
        // Already optimized by caller, but we can ensure fixpoint again if desired
        self.plan_node(logical)
    }

    fn plan_node(&mut self, node: LogicalPlan) -> Result<PhysicalPlan> {
        use LogicalPlan::*;
        match node {
            // DDL and DML
            CreateTable {
                table_name,
                columns,
            } => Ok(PhysicalPlan::CreateTable {
                table_name,
                columns,
            }),

            CreateIndex { .. } => {
                // index was created at bind time; no runtime action
                // you could emit a no-op or separate variant
                bail!("CreateIndex should have been handled at bind time");
            }

            Insert {
                table_name,
                col_ordinals,
                values,
            } => Ok(PhysicalPlan::Insert {
                table_name,
                col_ordinals,
                values,
            }),

            // SELECT: prefer index scan if possible
            SeqScan { table, predicate } => {
                if let Some(pred) = predicate.clone() {
                    if let Some((col, _op, _lit)) = Self::extract_eq_pred(&pred) {
                        // check for matching index metadata
                        for idx in self.storage.get_indexes(&table) {
                            if idx.column == col {
                                return Ok(PhysicalPlan::IndexScan {
                                    table_name: table.clone(),
                                    index_name: idx.name.clone(),
                                    predicate: pred,
                                });
                            }
                        }
                    }
                }
                // fallback to full scan + optional filter
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
            if let BoundExpr::Column { ref col, .. } = **left {
                return Some((col.clone(), BinaryOp::Eq, (**right).clone()));
            }
            if let BoundExpr::Column { ref col, .. } = **right {
                return Some((col.clone(), BinaryOp::Eq, (**left).clone()));
            }
        }
        None
    }
}
