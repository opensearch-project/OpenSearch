use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::config::ConfigOptions;
use datafusion::common::Result;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_expr::{PhysicalExpr, expressions::Column};
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::functions_aggregate::approx_distinct::approx_distinct_udaf;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
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
        let result = self.optimize_plan_with_alias(plan, None)?;
//         println!("[DEBUG] After: {}", displayable(result.as_ref()).indent(true));
        Ok(result)
    }

    fn optimize_plan_with_alias(&self, plan: Arc<dyn ExecutionPlan>, parent_alias: Option<String>) -> Result<Arc<dyn ExecutionPlan>> {
        // Recursively optimize children first
        let optimized_children: Result<Vec<_>> = plan.children()
            .into_iter()
            .map(|child| self.optimize_plan_with_alias(Arc::clone(child), parent_alias.clone()))
            .collect();
        let optimized_children = optimized_children?;

        // Handle AggregateExec: convert to Partial mode only for avg/approx_distinct
        if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
            // println!("[DEBUG] Found AggregateExec, mode: {:?}", agg.mode());
            // println!("[DEBUG] Aggregate output schema: {:?}", agg.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>());
            // println!("[DEBUG] Aggregate expressions: {:?}", agg.aggr_expr().iter().map(|e| e.name()).collect::<Vec<_>>());

            let needs_partial = agg.aggr_expr().iter().any(|e| {
                let name = e.name().to_lowercase();
                name.starts_with("approx_distinct(")
            });

            if needs_partial && !matches!(agg.mode(), &AggregateMode::Partial) {
                let new_agg = AggregateExec::try_new(
                    AggregateMode::Partial,
                    agg.group_expr().clone(),
                    agg.aggr_expr().to_vec(),
                    agg.filter_expr().to_vec(),
                    optimized_children[0].clone(),
                    optimized_children[0].schema(),
                )?;
                return Ok(Arc::new(new_agg));
            }
            return plan.with_new_children(optimized_children);
        }

        // Use original expression's aliases to make the final aliases
        if let Some(proj) = plan.as_any().downcast_ref::<ProjectionExec>() {
            let new_input = optimized_children[0].clone();
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

        // For all other nodes, just update with optimized children
        // println!("[DEBUG] Returning plan with new children: {}", plan.name());
        plan.with_new_children(optimized_children)
    }
}
