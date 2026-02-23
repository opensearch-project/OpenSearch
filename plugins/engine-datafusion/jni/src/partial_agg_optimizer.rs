use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::config::ConfigOptions;
use datafusion::common::Result;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_expr::{PhysicalExpr, expressions::Column};
use std::sync::Arc;

#[derive(Debug)]
pub struct PartialAggregationOptimizer;

impl PhysicalOptimizerRule for PartialAggregationOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_plan(plan)
    }

    fn name(&self) -> &str {
        "partial_aggregation_optimizer"
    }

    fn schema_check(&self) -> bool {
        // Partial mode can cause schema checks to fail
        false
    }
}

impl PartialAggregationOptimizer {
    pub fn optimize_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
//         println!("[DEBUG] Before: {}", displayable(plan.as_ref()).indent(true));
        let result = self.rewrite_plan(plan)?;
//         println!("[DEBUG] After: {}", displayable(result.as_ref()).indent(true));
        Ok(result)
    }

    fn rewrite_plan(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let new_children: Result<Vec<_>> = plan.children()
            .into_iter()
            .map(|child| self.rewrite_plan(Arc::clone(child)))
            .collect();
        let new_children = new_children?;

        if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
            return match agg.mode() {
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    // Remove the Final/FinalPartitioned node, preserving the
                    // repartition/coalesce layers underneath. The Java side
                    // handles merging partial results across partitions.
                    Ok(new_children[0].clone())
                }
                AggregateMode::Partial => {
                    plan.with_new_children(new_children)
                }
                _ => {
                    // Single/SinglePartitioned → convert to Partial
                    let new_agg = AggregateExec::try_new(
                        AggregateMode::Partial,
                        agg.group_expr().clone(),
                        agg.aggr_expr().to_vec(),
                        agg.filter_expr().to_vec(),
                        new_children[0].clone(),
                        new_children[0].schema(),
                    )?;
                    Ok(Arc::new(new_agg))
                }
            };
        }

        // Use original expression's aliases to make the final aliases
        if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let new_input = new_children[0].clone();
            let input_schema = new_input.schema();

            let new_exprs: Vec<_> = proj.expr().iter().map(|orig_expr| {
                if let Some(orig_col) = orig_expr.expr.as_any().downcast_ref::<Column>() {
                    let idx = orig_col.index();
                    (Arc::new(Column::new(input_schema.field(idx).name(), idx)) as Arc<dyn PhysicalExpr>, orig_expr.alias.clone())
                } else {
                    (orig_expr.expr.clone(), orig_expr.alias.clone())
                }
            }).collect();

            return Ok(Arc::new(ProjectionExec::try_new(new_exprs, new_input)?));
        }

        plan.with_new_children(new_children)
    }
}
