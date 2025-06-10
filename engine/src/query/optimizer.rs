// query/optimizer.rs

use crate::query::binder::BoundExpr;
use crate::query::parser::BinaryOp;
use crate::query::planner::LogicalPlan;
use anyhow::Result;

/// A simple rule‐based optimizer for our logical plans.
pub struct Optimizer;

impl Optimizer {
    /// Optimize the given logical plan by repeatedly applying rewrite rules to
    /// push filters down, push projections down, and remove redundant nodes.
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

    /// Single‐pass rewrite: apply each rule bottom‐up.
    fn rewrite(plan: &LogicalPlan) -> Result<LogicalPlan> {
        use LogicalPlan::*;

        // Recursively rewrite children first
        let rewritten = match plan {
            CreateTable { .. } | CreateIndex { .. } | Insert { .. } => plan.clone(),

            // SeqScan has no children
            SeqScan { table, predicate } => SeqScan {
                table: table.clone(),
                predicate: predicate.clone(),
            },

            // Rewrite input of Filter
            Filter { input, predicate } => {
                let new_input = Self::rewrite(input)?;
                Filter {
                    input: Box::new(new_input),
                    predicate: predicate.clone(),
                }
            }

            // Rewrite input of Projection
            Projection { input, exprs } => {
                let new_input = Self::rewrite(input)?;
                Projection {
                    input: Box::new(new_input),
                    exprs: exprs.clone(),
                }
            }
        };

        // Now apply local rewrite rules
        Ok(Self::apply_rules(rewritten))
    }

    /// Apply top‐down transformation rules once.
    fn apply_rules(plan: LogicalPlan) -> LogicalPlan {
        use LogicalPlan::*;

        match plan {
            // Merge consecutive filters: Filter(Filter(X,p1),p2) → Filter(X, p1 AND p2)
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
                // Push filter below projection: Projection(Filter(X,p),exprs) → Projection(Filter(X,p),exprs)
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

            // Merge consecutive projections: Projection(Projection(X,e1),e2) → Projection(X,e2)
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

            // Everything else unchanged
            other => other,
        }
    }
}
