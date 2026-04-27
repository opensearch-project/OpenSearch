/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Tests asserting the metrics wired in [`crate::indexed_table::metrics`]
//! actually get incremented during query execution. Covers both Tier-1
//! (restored existing counters) and Tier-3 (new diagnostic counters)
//! metrics.

use super::*;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Walk the physical plan tree and merge metrics from our operators.
///
/// Our counters live on `QueryShardExec`'s `ExecutionPlanMetricsSet`
/// (see `PartitionMetrics::new(&self.metrics, ...)` in
/// `table_provider.rs`). `IndexedExec` instances are built inside
/// `QueryShardExec::execute` and aren't part of the plan tree — their
/// metrics propagate up via the shared counter handles passed through
/// `StreamMetrics`, but we only need to inspect `QueryShardExec` itself
/// to read them back out.
fn aggregate_metrics(plan: &Arc<dyn ExecutionPlan>) -> MetricsSet {
    let mut set = MetricsSet::new();
    fn walk(plan: &Arc<dyn ExecutionPlan>, out: &mut MetricsSet) {
        if plan.name() == "QueryShardExec" {
            if let Some(m) = plan.metrics() {
                for metric in m.iter() {
                    out.push(Arc::clone(metric));
                }
            }
        }
        for child in plan.children() {
            walk(child, out);
        }
    }
    walk(plan, &mut set);
    set
}

/// Look up a named counter in an aggregated `MetricsSet`. Returns 0 if
/// not present. Filters to `DEV`-type metrics to exclude inner parquet
/// `SUMMARY` counters (e.g. `output_rows` from the inner
/// `DataSourceExec` which counts pre-mask rows).
fn get_counter(set: &MetricsSet, name: &str) -> usize {
    use datafusion::physical_plan::metrics::MetricType;
    set.sum(|m| {
        m.value().name() == name && m.metric_type() == MetricType::DEV
    })
    .map(|v| v.as_usize())
    .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_output_rows_matches_returned_row_count() {
    // Every apple row is returned — 5 of them (rows 4,5,6,7,13).
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let (rows, plan) = run_tree_and_plan(tree).await;
    let m = aggregate_metrics(&plan);
    assert_eq!(get_counter(&m, "output_rows"), rows.len());
    assert_eq!(rows.len(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_row_groups_processed_counts_rgs_with_matches() {
    // Fixture has 4 row groups of 4 rows each; apple appears in RG1 and RG3.
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let (_, plan) = run_tree_and_plan(tree).await;
    let m = aggregate_metrics(&plan);
    // RGs the candidate stage yielded non-empty candidates for.
    assert!(
        get_counter(&m, "row_groups_processed") >= 1,
        "expected at least 1 RG processed for apple query"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_ffm_collector_calls_equal_rgs_processed() {
    // Single-collector path: one FFM call per processed RG. Sum should
    // equal `row_groups_processed` (Collector is always called once
    // per RG before any page-pruning).
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let (_, plan) = run_tree_and_plan(tree).await;
    let m = aggregate_metrics(&plan);
    let ffm = get_counter(&m, "ffm_collector_calls");
    let rg_processed = get_counter(&m, "row_groups_processed");
    let rg_skipped = get_counter(&m, "row_groups_skipped");
    assert_eq!(
        ffm,
        rg_processed + rg_skipped,
        "ffm_collector_calls ({}) should equal total RGs touched ({}+{}={})",
        ffm,
        rg_processed,
        rg_skipped,
        rg_processed + rg_skipped
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_batches_counters_relate() {
    // `parquet_batches_received >= batches_produced` because empty-batch
    // output is dropped in finalize_batch (mask rejects every row).
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let (_, plan) = run_tree_and_plan(tree).await;
    let m = aggregate_metrics(&plan);
    let received = get_counter(&m, "parquet_batches_received");
    let produced = get_counter(&m, "batches_produced");
    assert!(
        received >= produced,
        "parquet_batches_received ({}) should be >= batches_produced ({})",
        received,
        produced
    );
    assert!(received > 0, "expected at least one input batch");
    assert!(produced > 0, "expected at least one output batch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_position_map_variants_sum_to_rgs_processed() {
    // Every RG that was processed (non-empty candidate set) produces
    // exactly one PositionMap variant count. The sum across the three
    // variants equals `row_groups_processed`.
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let (_, plan) = run_tree_and_plan(tree).await;
    let m = aggregate_metrics(&plan);
    let identity = get_counter(&m, "position_map_identity");
    let bitmap = get_counter(&m, "position_map_bitmap");
    let runs = get_counter(&m, "position_map_runs");
    let rg_processed = get_counter(&m, "row_groups_processed");
    assert_eq!(
        identity + bitmap + runs,
        rg_processed,
        "identity+bitmap+runs ({}+{}+{}={}) should equal rg_processed ({})",
        identity, bitmap, runs, identity + bitmap + runs, rg_processed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_rows_pruned_plus_rows_matched_upper_bounded() {
    // rows_matched = candidates per RG; rows_pruned = (rg rows -
    // candidates) per RG. Their sum equals total rows in processed RGs
    // (and is bounded by the fixture size, 16).
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let (_, plan) = run_tree_and_plan(tree).await;
    let m = aggregate_metrics(&plan);
    let matched = get_counter(&m, "rows_matched");
    let pruned = get_counter(&m, "rows_pruned_by_page_index");
    assert!(
        matched + pruned <= 16,
        "matched ({}) + pruned ({}) should be ≤ 16 fixture rows",
        matched,
        pruned
    );
    assert!(matched >= 5, "at least 5 apple rows should match");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_min_skip_run_bucket_consistency() {
    // Each processed RG lands in exactly one of the two min_skip_run
    // buckets. Sum equals row_groups_processed.
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let (_, plan) = run_tree_and_plan(tree).await;
    let m = aggregate_metrics(&plan);
    let row_granular = get_counter(&m, "min_skip_run_row_granular");
    let block_granular = get_counter(&m, "min_skip_run_block_granular");
    let rg_processed = get_counter(&m, "row_groups_processed");
    assert_eq!(
        row_granular + block_granular,
        rg_processed,
        "row_granular+block_granular should equal rg_processed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_elapsed_compute_is_nonzero_when_query_runs() {
    // Time metrics are nanoseconds; `elapsed_compute` should be >0 for
    // any non-trivial query.
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let (_, plan) = run_tree_and_plan(tree).await;
    let m = aggregate_metrics(&plan);
    let elapsed_ns = m
        .sum(|metric| metric.value().name() == "elapsed_compute")
        .map(|v| v.as_usize())
        .unwrap_or(0);
    assert!(elapsed_ns > 0, "elapsed_compute should be nonzero");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_empty_result_still_touches_rgs() {
    // A collector that matches nothing: no output, but RGs are still
    // counted as skipped (candidate set was empty for each).
    let tree = BoolNode::And(vec![
        index_leaf(1), // apple
        BoolNode::Not(Box::new(index_leaf(1))),
    ]);
    let (rows, plan) = run_tree_and_plan(tree).await;
    assert_eq!(rows.len(), 0, "contradictory tree → zero rows");
    let m = aggregate_metrics(&plan);
    // Either rows_pruned accounts for everything, or every RG was skipped.
    let pruned = get_counter(&m, "rows_pruned_by_page_index");
    let rg_skipped = get_counter(&m, "row_groups_skipped");
    assert!(
        pruned > 0 || rg_skipped > 0,
        "empty-result query should register pruning or RG-skipping activity"
    );
}
