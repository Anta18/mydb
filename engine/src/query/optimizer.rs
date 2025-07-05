

use crate::query::binder::BoundExpr;
use crate::query::parser::BinaryOp;
use crate::query::planner::LogicalPlan;
use anyhow::Result;


pub struct Optimizer;

impl Optimizer {
    
    
    pub fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut current = plan;
        loop {
            let next = Self::rewrite(&current)?;
            if std::mem::discriminant(&next) == std::mem::discriminant(&current)
                && format!("{:?}", next) == format!("{:?}", current)
            {
                break Ok(next);
            }
            current = next;
        }
    }

    
    fn rewrite(plan: &LogicalPlan) -> Result<LogicalPlan> {
        use LogicalPlan::*;

        
        let rewritten = match plan {
            CreateTable { .. } | CreateIndex { .. } | Insert { .. } => plan.clone(),

            
            SeqScan { table, predicate } => SeqScan {
                table: table.clone(),
                predicate: predicate.clone(),
            },

            
            Filter { input, predicate } => {
                let new_input = Self::rewrite(input)?;
                Filter {
                    input: Box::new(new_input),
                    predicate: predicate.clone(),
                }
            }

            
            Projection { input, exprs } => {
                let new_input = Self::rewrite(input)?;
                Projection {
                    input: Box::new(new_input),
                    exprs: exprs.clone(),
                }
            }
        };

        
        Ok(Self::apply_rules(rewritten))
    }

    
    fn apply_rules(plan: LogicalPlan) -> LogicalPlan {
        use LogicalPlan::*;

        match plan {
            
            Filter { input, predicate } => {
                if let Filter {
                    input: inner,
                    predicate: p1,
                } = *input.clone()
                {
                    let combined = BoundExpr::BinaryOp {
                        left: Box::new(p1),
                        op: BinaryOp::And,
                        right: Box::new(predicate.clone()),
                        data_type: crate::query::binder::DataType::Int,
                    };
                    return Filter {
                        input: inner,
                        predicate: combined,
                    };
                }
                
                if let Projection {
                    input: proj_input,
                    exprs,
                } = *input.clone()
                {
                    return Projection {
                        input: Box::new(Filter {
                            input: proj_input,
                            predicate: predicate.clone(),
                        }),
                        exprs,
                    };
                }
                Filter { input, predicate }
            }

            
            Projection { input, exprs } => {
                if let Projection {
                    input: inner,
                    exprs: _,
                } = *input.clone()
                {
                    return Projection {
                        input: inner,
                        exprs,
                    };
                }
                Projection { input, exprs }
            }

            
            other => other,
        }
    }
}
