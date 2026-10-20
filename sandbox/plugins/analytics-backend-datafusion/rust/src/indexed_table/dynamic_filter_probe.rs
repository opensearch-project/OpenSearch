/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Probe test (no production code): confirms that DataFusion's post-optimization
//! `FilterPushdown` rule actually delivers a `DynamicFilterPhysicalExpr` to a
//! **leaf** scan's `handle_child_pushdown_result` when a `SortExec { fetch }`
//! (TopK) sits above it.
//!
//! This is the make-or-break prerequisite for wiring dynamic filters into
//! `IndexedExec`/`QueryShardExec`: if the filter never reaches our leaf, the
//! whole feature is moot. We model our scan with a minimal recording leaf so
//! the test is hermetic and fast.

#![cfg(test)]

use std::fmt;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::{col, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

/// What a leaf observed in `handle_child_pushdown_result`. Shared out-of-band
/// (the optimizer clones/rebuilds nodes, so we can't read it back off the plan).
#[derive(Default)]
struct Observed {
    /// Stringified filters that arrived as parent_filters.
    arrived: Vec<String>,
    /// Whether at least one arriving filter was a DynamicFilterPhysicalExpr.
    saw_dynamic: bool,
}

/// Minimal leaf scan that records the filters pushed to it. Mirrors the role of
/// `QueryShardExec` for the purpose of this probe — a leaf with no children that
/// must accept a Post-phase dynamic filter.
#[derive(Clone)]
struct RecordingLeaf {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    observed: Arc<Mutex<Observed>>,
}

impl RecordingLeaf {
    fn new(schema: SchemaRef, observed: Arc<Mutex<Observed>>) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            schema,
            props,
            observed,
        }
    }
}

impl fmt::Debug for RecordingLeaf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordingLeaf")
    }
}

impl DisplayAs for RecordingLeaf {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RecordingLeaf")
    }
}

impl ExecutionPlan for RecordingLeaf {
    fn name(&self) -> &str {
        "RecordingLeaf"
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unreachable!("probe test never executes the plan")
    }

    /// The behaviour under test: when the TopK above us pushes its dynamic
    /// filter, it arrives here as `parent_filters`. Record what we see and
    /// claim support so the optimizer is happy.
    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        let mut obs = self.observed.lock().unwrap();
        let mut statuses = Vec::new();
        for f in &child_pushdown_result.parent_filters {
            obs.arrived.push(format!("{}", f.filter));
            // Detect a DynamicFilterPhysicalExpr anywhere in the expr tree.
            let mut is_dynamic = false;
            f.filter
                .apply(|e| {
                    if e.downcast_ref::<DynamicFilterPhysicalExpr>().is_some() {
                        is_dynamic = true;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .ok();
            obs.saw_dynamic |= is_dynamic;
            statuses.push(PushedDown::Yes);
        }
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            statuses,
        ))
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("brand", DataType::Utf8, true),
    ]))
}

/// SortExec(ts DESC, fetch=Some(k)) over RecordingLeaf, then run the
/// post-optimization FilterPushdown rule. Assert a dynamic filter arrived.
#[test]
fn dynamic_filter_reaches_leaf_under_topk() {
    let schema = test_schema();
    let observed = Arc::new(Mutex::new(Observed::default()));
    let leaf: Arc<dyn ExecutionPlan> =
        Arc::new(RecordingLeaf::new(schema.clone(), Arc::clone(&observed)));

    // ORDER BY ts DESC LIMIT 10
    let sort_expr = PhysicalSortExpr {
        expr: col("ts", &schema).unwrap(),
        options: datafusion::arrow::compute::SortOptions {
            descending: true,
            nulls_first: false,
        },
    };
    let ordering = LexOrdering::new(vec![sort_expr]).unwrap();
    let sort = Arc::new(SortExec::new(ordering, leaf).with_fetch(Some(10)));

    let mut config = ConfigOptions::default();
    // Defaults are already true, but be explicit — this is the switch.
    config.optimizer.enable_dynamic_filter_pushdown = true;
    config.optimizer.enable_topk_dynamic_filter_pushdown = true;

    let rule = FilterPushdown::new_post_optimization();
    let _optimized = rule
        .optimize(sort, &config)
        .expect("post-optimization filter pushdown should not error");

    let obs = observed.lock().unwrap();
    assert!(
        obs.saw_dynamic,
        "expected a DynamicFilterPhysicalExpr to reach the leaf; arrived filters = {:?}",
        obs.arrived
    );
    // Exactly one filter arrives — the TopK self-filter. Its `Display` at plan
    // time is the opaque `DynamicFilter [ empty ]` placeholder (the concrete
    // `ts > <threshold>` value materializes only at runtime via snapshot()).
    // This is *why* acceptance keys on the referenced columns, not the printed
    // value: we must downcast + walk the children to find the sort column.
    assert_eq!(
        obs.arrived.len(),
        1,
        "expected exactly the TopK self-filter"
    );
    assert!(
        obs.arrived[0].contains("DynamicFilter"),
        "arrived filter should be the dynamic placeholder; got {:?}",
        obs.arrived
    );
}
