/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Aggregate mode stripping for distributed partial/final execution.

use std::sync::Arc;

use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::optimizer::{PhysicalOptimizer, PhysicalOptimizerRule};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Mode {
    Default,
    Partial,
    Final,
}

/// Returns the default physical optimizer rules with `CombinePartialFinalAggregate` removed.
pub(crate) fn physical_optimizer_rules_without_combine(
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let combine_name = CombinePartialFinalAggregate::new().name().to_string();
    PhysicalOptimizer::new()
        .rules
        .into_iter()
        .filter(|r| r.name() != combine_name)
        .collect()
}

/// Applies aggregate mode stripping to a physical plan.
pub(crate) fn apply_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    mode: Mode,
) -> Result<Arc<dyn ExecutionPlan>> {
    match mode {
        Mode::Default => Ok(plan),
        Mode::Partial => force_aggregate_mode(plan, AggregateMode::Partial),
        Mode::Final => force_aggregate_mode(plan, AggregateMode::Final),
    }
}

/// Walks the plan tree and strips the half that doesn't match `target`.
fn force_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    target: AggregateMode,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if *agg.mode() == target {
            // Keep this node, recurse into children
            let new_children: Vec<Arc<dyn ExecutionPlan>> = agg
                .children()
                .into_iter()
                .map(|c| force_aggregate_mode(Arc::clone(c), target))
                .collect::<Result<_>>()?;
            return plan.with_new_children(new_children);
        }
        // Mode mismatch — strip this node
        match target {
            AggregateMode::Partial => {
                // Current node is Final; find the Partial subtree below
                if let Some(partial_subtree) = find_partial_input(Arc::clone(agg.input())) {
                    return Ok(partial_subtree);
                }
                // If no Partial found below, the input itself is the Partial
                Ok(Arc::clone(agg.input()))
            }
            AggregateMode::Final => {
                // Current node is Partial; skip it, return its child
                // (the Final above will keep itself)
                let child = agg.children()[0];
                force_aggregate_mode(Arc::clone(child), target)
            }
            _ => Ok(plan),
        }
    } else if plan.as_any().downcast_ref::<RepartitionExec>().is_some()
        || plan
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some()
    {
        // Transparent — recurse through
        let new_children: Vec<Arc<dyn ExecutionPlan>> = plan
            .children()
            .into_iter()
            .map(|c| force_aggregate_mode(Arc::clone(c), target))
            .collect::<Result<_>>()?;
        plan.with_new_children(new_children)
    } else {
        // Leaf or unrelated node — return as-is
        Ok(plan)
    }
}

/// Walks down through RepartitionExec/CoalescePartitionsExec to find an
/// AggregateExec(Partial) and returns the entire Partial subtree (the
/// AggregateExec node itself, not just its input).
fn find_partial_input(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
        if *agg.mode() == AggregateMode::Partial {
            return Some(plan);
        }
        return None;
    }
    if plan.as_any().downcast_ref::<RepartitionExec>().is_some()
        || plan
            .as_any()
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some()
    {
        let children = plan.children();
        if children.len() == 1 {
            return find_partial_input(Arc::clone(children[0]));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use datafusion::physical_plan::displayable;

    /// Helper: create a SessionContext with CombinePartialFinalAggregate disabled,
    /// register a memtable, and produce a physical plan for `SELECT SUM(x) FROM t`.
    async fn make_agg_plan() -> Arc<dyn ExecutionPlan> {
        let ctx = SessionContext::new_with_state(
            datafusion::execution::SessionStateBuilder::new()
                .with_config(SessionConfig::new())
                .with_default_features()
                .with_physical_optimizer_rules(physical_optimizer_rules_without_combine())
                .build(),
        );
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.sql("SELECT SUM(x) FROM t").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    /// Helper: create a plan with Repartition between Final and Partial.
    async fn make_agg_plan_with_repartition() -> Arc<dyn ExecutionPlan> {
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = 4;
        let ctx = SessionContext::new_with_state(
            datafusion::execution::SessionStateBuilder::new()
                .with_config(config)
                .with_default_features()
                .with_physical_optimizer_rules(physical_optimizer_rules_without_combine())
                .build(),
        );
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("x", arrow::datatypes::DataType::Int64, false),
            ])),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        ctx.register_batch("t", batch).unwrap();
        // GROUP BY forces repartition with multiple target partitions
        let df = ctx.sql("SELECT x, SUM(x) FROM t GROUP BY x").await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    fn plan_string(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    fn contains_node(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        if plan.name().contains(name) {
            return true;
        }
        plan.children().iter().any(|c| contains_node(c, name))
    }

    fn find_agg_modes(plan: &Arc<dyn ExecutionPlan>) -> Vec<AggregateMode> {
        let mut modes = Vec::new();
        if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
            modes.push(*agg.mode());
        }
        for child in plan.children() {
            modes.extend(find_agg_modes(child));
        }
        modes
    }

    #[tokio::test]
    async fn test_strip_partial_over_scan() {
        // Final(Partial(memtable)) → strip to Partial only
        let plan = make_agg_plan().await;
        let modes = find_agg_modes(&plan);
        assert!(
            modes.contains(&AggregateMode::Final) || modes.contains(&AggregateMode::Partial),
            "Plan should have aggregate nodes: {}",
            plan_string(&plan)
        );

        let result = apply_aggregate_mode(plan, Mode::Partial).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            result_modes.contains(&AggregateMode::Partial),
            "Should contain Partial: {}",
            plan_string(&result)
        );
        assert!(
            !result_modes.contains(&AggregateMode::Final),
            "Should NOT contain Final: {}",
            plan_string(&result)
        );
    }

    #[tokio::test]
    async fn test_strip_final_over_scan() {
        // Final(Partial(memtable)) → strip to Final only (Partial removed)
        let plan = make_agg_plan().await;
        let result = apply_aggregate_mode(plan, Mode::Final).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            result_modes.contains(&AggregateMode::Final),
            "Should contain Final: {}",
            plan_string(&result)
        );
        assert!(
            !result_modes.contains(&AggregateMode::Partial),
            "Should NOT contain Partial: {}",
            plan_string(&result)
        );
    }

    #[tokio::test]
    async fn test_strip_partial_past_repartition() {
        // Final → Repartition/Coalesce → Partial → scan; strip to Partial
        let plan = make_agg_plan_with_repartition().await;
        let plan_str = plan_string(&plan);
        // Verify the plan has the expected structure
        let modes = find_agg_modes(&plan);
        if modes.len() < 2 {
            // If optimizer collapsed it, just verify Mode::Partial works
            let result = apply_aggregate_mode(plan, Mode::Partial).unwrap();
            let result_modes = find_agg_modes(&result);
            assert!(!result_modes.contains(&AggregateMode::Final));
            return;
        }

        let result = apply_aggregate_mode(plan, Mode::Partial).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            !result_modes.contains(&AggregateMode::Final),
            "Should NOT contain Final after strip: {}\nOriginal: {}",
            plan_string(&result),
            plan_str
        );
    }

    #[tokio::test]
    async fn test_strip_final_past_coalesce() {
        // Final → CoalescePartitions → Partial → scan; strip to Final
        let plan = make_agg_plan().await;
        // The simple plan has CoalescePartitions between Final and Partial
        let result = apply_aggregate_mode(plan, Mode::Final).unwrap();
        let result_modes = find_agg_modes(&result);
        assert!(
            !result_modes.contains(&AggregateMode::Partial),
            "Should NOT contain Partial after strip: {}",
            plan_string(&result)
        );
        assert!(
            result_modes.contains(&AggregateMode::Final),
            "Should contain Final: {}",
            plan_string(&result)
        );
    }

    #[test]
    fn test_combine_rule_absent() {
        let rules = physical_optimizer_rules_without_combine();
        let combine_name = CombinePartialFinalAggregate::new().name().to_string();
        assert!(
            !rules.iter().any(|r| r.name() == combine_name),
            "CombinePartialFinalAggregate should be filtered out"
        );
        // Verify we still have other rules
        assert!(!rules.is_empty(), "Should have other optimizer rules");
    }
}
