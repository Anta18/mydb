

use crate::query::binder::{BoundExpr, DataType};
use crate::query::parser::BinaryOp;
use crate::query::planner::LogicalPlan;
use crate::storage::storage::Storage;
use anyhow::{Result, bail};






#[derive(Debug)]
pub enum PhysicalPlan {
    
    CreateTable {
        table_name: String,
        columns: Vec<(String, DataType)>,
    },

    
    

    
    Insert {
        table_name: String,
        col_ordinals: Vec<usize>,
        values: Vec<BoundExpr>,
    },

    
    SeqScan {
        table_name: String,
        predicate: Option<BoundExpr>,
    },

    
    IndexScan {
        table_name: String,
        index_name: String,
        predicate: BoundExpr, 
    },

    
    Filter {
        input: Box<PhysicalPlan>,
        predicate: BoundExpr,
    },

    
    Projection {
        input: Box<PhysicalPlan>,
        exprs: Vec<BoundExpr>,
    },
}






pub struct PhysicalPlanner<'a> {
    catalog: &'a crate::query::binder::Catalog,
    storage: &'a mut Storage,
}

impl<'a> PhysicalPlanner<'a> {
    
    pub fn new(catalog: &'a crate::query::binder::Catalog, storage: &'a mut Storage) -> Self {
        PhysicalPlanner { catalog, storage }
    }

    
    pub fn create_physical_plan(&mut self, logical: LogicalPlan) -> Result<PhysicalPlan> {
        
        self.plan_node(logical)
    }

    fn plan_node(&mut self, node: LogicalPlan) -> Result<PhysicalPlan> {
        use LogicalPlan::*;
        match node {
            
            CreateTable {
                table_name,
                columns,
            } => Ok(PhysicalPlan::CreateTable {
                table_name,
                columns,
            }),

            CreateIndex { .. } => {
                
                
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

            
            SeqScan { table, predicate } => {
                if let Some(pred) = predicate.clone() {
                    if let Some((col, _op, _lit)) = Self::extract_eq_pred(&pred) {
                        
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
