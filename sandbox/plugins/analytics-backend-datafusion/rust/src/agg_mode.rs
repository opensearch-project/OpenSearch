/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Aggregate mode stripping for distributed partial/final execution.

use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::combine_partial_final_agg::CombinePartialFinalAggregate;
use datafusion::physical_optimizer::optimizer::{PhysicalOptimizer, PhysicalOptimizerRule};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
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
/// `has_topk`: when true and stripping to Partial, replaces Final/FinalPartitioned with
/// PartialReduce so CSS partitions are merged by group key before the TopK sort truncates.
pub(crate) fn apply_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    mode: Mode,
    has_topk: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    match mode {
        Mode::Default => Ok(plan),
        Mode::Partial => force_aggregate_mode(plan, AggregateMode::Partial, has_topk),
        Mode::Final => force_aggregate_mode(plan, AggregateMode::Final, false),
    }
}

/// Returns the output schema of the Partial aggregate without rebuilding the plan tree.
/// Used by `derive_schema_from_partial_plan` where we only need types, not an executable plan.
pub(crate) fn partial_aggregate_schema(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<arrow::datatypes::SchemaRef> {
    find_partial_input(Arc::clone(plan)).map(|p| p.schema())
}

/// Walks the plan tree and strips the half that doesn't match `target`.
fn force_aggregate_mode(
    plan: Arc<dyn ExecutionPlan>,
    target: AggregateMode,
    has_topk: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        // Treat `FinalPartitioned` as `Final`: DataFusion picks `FinalPartitioned` for
        // grouped aggregates that consume hash-partitioned input and `Final` for scalar /
        // un-partitioned ones. Both are the FINAL half of the Partial/Final pair we strip.
        let agg_is_target = *agg.mode() == target
            || (target == AggregateMode::Final && *agg.mode() == AggregateMode::FinalPartitioned);
        if agg_is_target {
            // Keep this node, recurse into children
            let new_children: Vec<Arc<dyn ExecutionPlan>> = agg
                .children()
                .into_iter()
                .map(|c| force_aggregate_mode(Arc::clone(c), target, has_topk))
                .collect::<Result<_>>()?;
            return plan.with_new_children(new_children);
        }
        // Mode mismatch — strip this node
        match target {
            AggregateMode::Partial => {
                // Current node is Final/FinalPartitioned.
                // When TopK is active and the input has multiple partitions (CSS), replace
                // with PartialReduce instead of stripping. PartialReduce keeps agg.input()
                // (RepartitionExec(Hash) → Partial(×N)) so CSS partitions are merged by
                // group key before TopK truncation. Skip when input_partitions=1 — PartialReduce
                // over a single partition is redundant and adds unnecessary overhead.
                if has_topk && agg.input().output_partitioning().partition_count() > 1 {
                    return Ok(Arc::new(AggregateExec::try_new(
                        AggregateMode::PartialReduce,
                        agg.group_expr().clone(),
                        agg.aggr_expr().to_vec(),
                        agg.filter_expr().to_vec(),
                        Arc::clone(agg.input()),
                        agg.input_schema(),
                    )?));
                }
                // Normal path: strip Final, return Partial subtree
                if let Some(partial_subtree) = find_partial_input(Arc::clone(agg.input())) {
                    return Ok(partial_subtree);
                }
                Ok(Arc::clone(agg.input()))
            }
            AggregateMode::Final => {
                // Current node is Partial; skip it, return its child
                // (the Final above will keep itself)
                let child = agg.children()[0];
                force_aggregate_mode(Arc::clone(child), target, false)
            }
            _ => Ok(plan),
        }
    } else if plan.children().len() == 1 {
        // Single-input wrapper — recurse transparently.
        let old_child = Arc::clone(plan.children()[0]);
        let new_child = force_aggregate_mode(old_child.clone(), target, has_topk)?;

        // DataFusion's ProjectionMapping::try_new asserts col.name() == input_schema.field(i).name();
        // with_new_children triggers it. Remap columns to the post-strip schema so it passes.
        if let Some(proj) = plan.downcast_ref::<ProjectionExec>() {
            if old_child.schema() != new_child.schema() {
                let new_schema = &new_child.schema();
                let remapped: Vec<(Arc<dyn PhysicalExpr>, String)> = proj
                    .expr()
                    .iter()
                    .map(|pe| (remap_column(pe.expr.clone(), new_schema), pe.alias.clone()))
                    .collect();
                return Ok(Arc::new(ProjectionExec::try_new(remapped, new_child)?));
            }
        }

        plan.with_new_children(vec![new_child])
    } else {
        // Leaf or multi-input node — return as-is
        Ok(plan)
    }
}

/// Walks down through any single-input wrapper (RelabelExec / RepartitionExec /
/// CoalescePartitionsExec / ProjectionExec / etc.) to find an
/// AggregateExec(Partial) and returns the entire Partial subtree (the
/// AggregateExec node itself, not just its input).
fn find_partial_input(plan: Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        if *agg.mode() == AggregateMode::Partial {
            return Some(plan);
        }
        // Non-Partial aggregate (Final/FinalPartitioned) — look into its input for Partial
        return find_partial_input(Arc::clone(agg.input()));
    }
    let children = plan.children();
    if children.len() == 1 {
        return find_partial_input(Arc::clone(children[0]));
    }
    None
}

/// Updates Column expression names to match the given schema (by index). Recurses into children.
fn remap_column(
    expr: Arc<dyn PhysicalExpr>,
    schema: &arrow::datatypes::SchemaRef,
) -> Arc<dyn PhysicalExpr> {
    if let Some(col) = expr.downcast_ref::<Column>() {
        return Arc::new(Column::new(schema.field(col.index()).name(), col.index()));
    }
    let children = expr.children();
    if children.is_empty() {
        return expr;
    }
    let new_children: Vec<_> = children
        .into_iter()
        .map(|c| remap_column(c.clone(), schema))
        .collect();
    let fallback = expr.clone();
    expr.with_new_children(new_children).unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::*;

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
        if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
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

        let result = apply_aggregate_mode(plan, Mode::Partial, false).unwrap();
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
        let result = apply_aggregate_mode(plan, Mode::Final, false).unwrap();
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
            let result = apply_aggregate_mode(plan, Mode::Partial, false).unwrap();
            let result_modes = find_agg_modes(&result);
            assert!(!result_modes.contains(&AggregateMode::Final));
            return;
        }

        let result = apply_aggregate_mode(plan, Mode::Partial, false).unwrap();
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
        let result = apply_aggregate_mode(plan, Mode::Final, false).unwrap();
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
        assert!(!rules.is_empty(), "Should have other optimizer rules");
    }

    /// Verifies apply_aggregate_mode(Partial) strips the Final aggregate and keeps
    /// only the Partial subtree — the core behavior the indexed executor relies on
    /// for engine-native-merge (dc/HLL) queries.
    #[tokio::test]
    async fn test_apply_partial_strips_final() {
        let plan = make_agg_plan().await;
        let display_before = plan_string(&plan);
        assert!(
            display_before.contains("AggregateExec: mode=Final"),
            "expected Final in plan"
        );
        assert!(
            display_before.contains("AggregateExec: mode=Partial"),
            "expected Partial in plan"
        );

        let stripped = apply_aggregate_mode(plan, Mode::Partial, false).unwrap();
        let display_after = plan_string(&stripped);
        assert!(
            !display_after.contains("mode=Final"),
            "Final should be stripped"
        );
        assert!(
            display_after.contains("mode=Partial"),
            "Partial should remain"
        );
    }

    /// When has_topk=true and the input has multiple partitions (CSS), Final/FinalPartitioned
    /// must be replaced with PartialReduce rather than stripped, so the coordinator receives
    /// correctly merged partial state instead of per-partition-truncated results.
    #[tokio::test]
    async fn test_apply_partial_with_topk_produces_partial_reduce() {
        let plan = make_agg_plan_with_repartition().await;
        let display_before = plan_string(&plan);
        // With target_partitions=4 and GROUP BY, DF produces FinalPartitioned.
        assert!(
            display_before.contains("mode=FinalPartitioned")
                || display_before.contains("mode=Final"),
            "expected Final/FinalPartitioned in multi-partition plan, got:\n{display_before}"
        );

        let result = apply_aggregate_mode(plan, Mode::Partial, true).unwrap();
        let modes = find_agg_modes(&result);
        assert!(
            modes.contains(&AggregateMode::PartialReduce),
            "has_topk=true with multi-partition input must produce PartialReduce, got modes: {modes:?}"
        );
        assert!(
            !modes.contains(&AggregateMode::Final)
                && !modes.contains(&AggregateMode::FinalPartitioned),
            "Final/FinalPartitioned must not remain after stripping"
        );
    }
}
