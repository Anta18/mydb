// query/optimizer.rs

use crate::query::binder::BoundExpr;
use crate::query::parser::BinaryOp; // Import BinaryOp from parser module
use crate::query::planner::LogicalPlan;
use anyhow::Result;
use std::sync::Arc;

/// A simple rule‐based optimizer for our logical plans.
pub struct Optimizer;

impl Optimizer {
    /// Optimize the given logical plan by repeatedly applying rewrite rules to
    /// push filters down, push projections down, and remove redundant nodes.
    pub fn optimize(plan: LogicalPlan) -> Result<LogicalPlan> {
        // We do a fixpoint iteration: keep applying rewrite until no change.
        let mut current = plan;
        loop {
            let next = Self::rewrite(&current)?;
            if std::mem::discriminant(&next) == std::mem::discriminant(&current)
                && format!("{:?}", &next) == format!("{:?}", &current)
            {
                // no structural change
                break Ok(next);
            }
            current = next;
        }
    }

    /// Single‐pass rewrite: apply each rule bottom‐up.
    fn rewrite(plan: &LogicalPlan) -> Result<LogicalPlan> {
        use LogicalPlan::*;
        // First recursively rewrite children
        let rewritten = match plan {
            CreateTable { .. } | Insert { .. } => plan.clone(),

            // SeqScan has no children
            SeqScan {
                table,
                alias,
                predicate,
            } => SeqScan {
                table: table.clone(),
                alias: alias.clone(),
                predicate: predicate.clone(),
            },

            // Rewrite input, then rebuild
            Filter { input, predicate } => {
                let input_opt = Self::rewrite(input)?;
                Filter {
                    input: Box::new(input_opt), // Remove Arc wrapper and double clone
                    predicate: predicate.clone(),
                }
            }

            Projection { input, exprs } => {
                let input_opt = Self::rewrite(input)?;
                Projection {
                    input: Box::new(input_opt), // Remove Arc wrapper and double clone
                    exprs: exprs.clone(),
                }
            }
        };

        // Now apply rewrite rules top‐down on `rewritten`
        Ok(Self::apply_rules(rewritten))
    }

    /// Apply all local transformation rules once.
    fn apply_rules(plan: LogicalPlan) -> LogicalPlan {
        use LogicalPlan::*;

        match plan {
            // Rule 1: Filter(Filter(X,p1),p2) --> Filter(X, p1 AND p2)
            Filter { input, predicate } => {
                if let Filter {
                    input: inner,
                    predicate: p1,
                } = *input.clone()
                {
                    let combined = BoundExpr::BinaryOp {
                        left: Box::new(p1),
                        op: BinaryOp::And, // Use BinaryOp from parser module
                        right: Box::new(predicate.clone()),
                        data_type: crate::query::binder::DataType::Int, // Add missing data_type field
                    };
                    return Filter {
                        input: inner,
                        predicate: combined,
                    };
                }
                // Rule 2: push Filter below Projection
                if let Projection {
                    input: grand,
                    exprs,
                } = *input.clone()
                {
                    return Projection {
                        input: Box::new(Filter {
                            input: grand, // Remove Box::new wrapper since grand is already Box<LogicalPlan>
                            predicate: predicate.clone(),
                        }),
                        exprs,
                    };
                }
                Filter { input, predicate }
            }

            // Rule 3: Projection(Projection(X,e1),e2) -> Projection(X,e2)
            Projection { input, exprs } => {
                if let Projection {
                    input: inner,
                    exprs: e1,
                } = *input.clone()
                {
                    return Projection {
                        input: inner,
                        exprs,
                    };
                }
                // (Optional) Rule 4: Projection(Filter(X,p),exprs) ->
                // Filter(Projection(X, needed_cols ∪ preds), p)
                // For brevity, not implemented here.

                Projection { input, exprs }
            }

            // Others unchanged
            other => other,
        }
    }
}
