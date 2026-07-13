/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Unified DataFusion `TableProvider` for all indexed-query paths.
//!
//! This is the ONE provider. Paths B and C differ only in the evaluator
//! factory closure supplied in `IndexedTableConfig`. The provider itself,
//! the `QueryShardExec` it wraps, and the `IndexedExec`s it spawns are
//! identical across paths.
//!
//! ```text
//!     IndexedTableProvider (scan)
//!             │
//!             ▼
//!     QueryShardExec (1 per query, partitioned across chunks)
//!             │
//!             ├── IndexedExec(chunk_0) ── IndexedStream ── RowGroupBitsetSource
//!             ├── IndexedExec(chunk_1) ── IndexedStream ── RowGroupBitsetSource
//!             └── IndexedExec(chunk_N) ── IndexedStream ── RowGroupBitsetSource
//! ```

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Result, Statistics};
use datafusion::datasource::TableType;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, Partitioning, PhysicalSortExpr,
};
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::DataFusionError;

use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;

use super::bool_tree::BoolNode;
use super::eval::RowGroupBitsetSource;
use super::metrics::PartitionMetrics;
use super::parquet_bridge::ReadIoStats;
use super::partitioning::{
    compute_assignments, compute_assignments_one_per_segment, segments_chain_on_sort_key,
    PartitionAssignment, SegmentChunk, SegmentLayout,
};
use super::stream::{IndexedExec, RowGroupInfo};
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::indexed_table::metrics::StreamMetrics;
use crate::indexed_table::page_pruner::StatsPruneTree;
use crate::search_stats::accumulate_from_exec;
use std::collections::HashSet;

/// Info about a segment and its corresponding parquet file.
#[derive(Debug, Clone)]
pub struct SegmentFileInfo {
    /// Writer generation for this segment — the stable per-segment identifier
    /// that crosses the FFM boundary to identify a segment on the Java side.
    /// Read from the parquet footer key-value metadata
    /// (`opensearch.writer_generation`) at `build_segments` time.
    pub writer_generation: i64,
    pub max_doc: i64,
    /// Object-store-relative path to the parquet file (same as the
    /// `ObjectMeta.location` DataFusion uses for the vanilla `ListingTable`).
    pub object_path: object_store::path::Path,
    pub parquet_size: u64,
    pub row_groups: Vec<RowGroupInfo>,
    pub metadata: Arc<ParquetMetaData>,
    /// Cumulative row count from all preceding segments. Used to compute
    /// shard-global row IDs: `global_base + rg.first_row + position_in_rg`.
    pub global_base: u64,
    /// Min/max of the LEAD `index.sort.field` column across all row groups
    /// in this segment, read from parquet footer column statistics.
    /// Both `None` when no sort field is configured, when any RG is missing
    /// stats for the lead column, or when the column type isn't supported by
    /// `StatisticsConverter`. `None` means the segment cannot participate in
    /// the chain decision (treated as "can't chain").
    pub sort_min: Option<datafusion::common::ScalarValue>,
    pub sort_max: Option<datafusion::common::ScalarValue>,
}

/// Factory: build a `RowGroupBitsetSource` for one `SegmentChunk`.
///
/// Invoked once per chunk per query. For the single-collector path this
/// produces a `SingleCollectorEvaluator`. For the multi-filter tree path it
/// produces a `BitmapTreeEvaluator`-backed `TreeBitsetSource`.
///
/// The closure is cloneable (stored in an `Arc`) so the provider can spawn
/// many `IndexedExec`s from a single config.
///
/// # Pluggability
///
/// `RowGroupBitsetSource` is the single seam that determines *where* tree
/// evaluation happens. Today the built-in impls all walk the tree in Rust,
/// but a future `JavaTreeBitsetSource` could route per-RG evaluation to
/// analytics-core via an FFM upcall without touching `IndexedStream`,
/// `IndexedExec`, or this factory's signature. Evaluators that carry
/// cross-chunk or cross-query state (e.g. a Java-resident tree) should
/// keep that state external and reference it by handle from the evaluator.
pub type EvaluatorFactory = Arc<
    dyn Fn(
            &SegmentFileInfo,
            &SegmentChunk,
            &StreamMetrics,
            Option<&Arc<StatsPruneTree>>,
        ) -> Result<Arc<dyn RowGroupBitsetSource>, String>
        + Send
        + Sync,
>;

/// Build a `LexOrdering` from `sort_fields` / `sort_orders` against the given
/// projected schema, mirroring DataFusion's `create_ordering`
/// (`physical-expr/src/physical_expr.rs:134`):
/// - on first column that doesn't resolve, **break** out of the loop and
///   keep whatever prefix we built (the rest is "violated"),
/// - returns `None` when the prefix is empty (no useful claim to advertise).
///
/// Direction strings are `"asc"` / `"desc"` (lowercase, as plumbed from Java).
/// Nulls placement matches Lucene's convention: ASC → NULLS FIRST,
/// DESC → NULLS LAST. Same as the vanilla path's `build_file_sort_order` in
/// `session_context.rs`.
fn build_projected_lex_ordering(
    projected_schema: &SchemaRef,
    sort_fields: &[String],
    sort_orders: &[String],
) -> Option<LexOrdering> {
    if sort_fields.is_empty() {
        return None;
    }
    let mut exprs: Vec<PhysicalSortExpr> = Vec::with_capacity(sort_fields.len());
    for (i, field) in sort_fields.iter().enumerate() {
        let phys = match physical_col(field, projected_schema) {
            Ok(e) => e,
            Err(_) => break,
        };
        let descending = sort_orders
            .get(i)
            .map(|s| s.eq_ignore_ascii_case("desc"))
            .unwrap_or(false);
        let ascending = !descending;
        let opts = SortOptions {
            descending,
            // ASC → NULLS FIRST, DESC → NULLS LAST (matches Lucene + vanilla path).
            nulls_first: ascending,
        };
        exprs.push(PhysicalSortExpr::new(phys, opts));
    }
    LexOrdering::new(exprs)
}

/// Configuration used to build an `IndexedTableProvider`.
pub struct IndexedTableConfig {
    pub schema: SchemaRef,
    pub segments: Vec<SegmentFileInfo>,
    /// Object store for reading parquet bytes. All I/O on the indexed path
    /// goes through this same store resolution as vanilla — no hardcoded
    /// LocalFileSystem. Resolved once per query from the runtime env.
    pub store: Arc<dyn object_store::ObjectStore>,
    /// URL of the store for DataFusion's `FileScanConfig`.
    pub store_url: datafusion::execution::object_store::ObjectStoreUrl,
    pub evaluator_factory: EvaluatorFactory,
    /// Parquet-native residual predicate to push into decode time via
    /// `ParquetSource::with_predicate`. Derived from the BoolNode tree
    /// by `execute_indexed_query`:
    /// - `FilterClass::SingleCollector`: residual (non-Collector
    ///   children of top AND) as a single `PhysicalExpr`.
    /// - `FilterClass::Tree`: `None` (BitmapTreeEvaluator does all
    ///   refinement in `on_batch_mask`; pushdown would risk invoking
    ///   the `index_filter` UDF).
    ///
    /// `scan()` uses this rather than the `filters` argument it
    /// receives from DataFusion, because DataFusion's filters include
    /// the `index_filter(...)` UDF marker whose body panics.
    pub pushdown_predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Query-scoped tunables (batch_size, target_partitions, costs, …).
    /// Shared by reference across fanned-out `QueryShardExec` instances.
    pub query_config: Arc<DatafusionQueryConfig>,
    /// Full-schema column indices referenced by BoolNode Predicate leaves.
    pub predicate_columns: Vec<usize>,
    /// When true, the `___row_id` column in the output projection is computed
    /// from position (global_base + rg.first_row + position_in_rg) instead of
    /// being read from parquet. Other projected columns are read normally.
    pub emit_row_ids: bool,
    /// Query-level data for building StatsPruneTree per segment.
    /// (BoolNode tree, prebuilt PruningPredicates keyed by Arc ptr, schema)
    pub prune_tree_config: Option<(
        Arc<BoolNode>,
        Arc<std::collections::HashMap<usize, Arc<PruningPredicate>>>,
        SchemaRef,
    )>,
    /// `index.sort.field` — column names that the parquet writer used to sort
    /// rows on disk. Empty when the index has no `index.sort.field`.
    pub sort_fields: Vec<String>,
    /// Parallel to `sort_fields`. Each entry is `"asc"` or `"desc"` (lowercase,
    /// matches the wire format from `DataFusionPlugin`). Same length as
    /// `sort_fields` (validated at index creation).
    pub sort_orders: Vec<String>,
    /// Per-query cancellation token (from the global `QUERY_REGISTRY`). Threaded
    /// down to `IndexReader` so the scan cooperatively stops when the query task
    /// is cancelled. `None` for untracked queries (`context_id == 0`) and tests.
    pub cancellation_token: Option<tokio_util::sync::CancellationToken>,
}

/// Table provider. Returns a `QueryShardExec` that fans out across chunks.
pub struct IndexedTableProvider {
    config: Arc<IndexedTableConfig>,
}

impl fmt::Debug for IndexedTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedTableProvider")
            .field("segments", &self.config.segments.len())
            .field("partitions", &self.config.query_config.target_partitions)
            .finish()
    }
}

impl IndexedTableProvider {
    pub fn new(config: IndexedTableConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

#[async_trait]
impl TableProvider for IndexedTableProvider {
    fn schema(&self) -> SchemaRef {
        self.config.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // `Exact` — the BoolNode tree held by the evaluator factory
        // fully handles every WHERE filter (Collectors via FFM bitsets,
        // Predicates via arrow kernels in refinement). DataFusion
        // removes the outer FilterExec, which is important because
        // otherwise FilterExec would try to evaluate the
        // `index_filter(...)` UDF whose body panics by design.
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let full_schema = self.config.schema.clone();

        // Detect __row_id__ in the output projection when emit_row_ids=true.
        // If present, we strip it from the parquet read and compute it from position.
        let row_id_col_in_full_schema = full_schema.index_of(crate::ROW_ID_COLUMN_NAME).ok();
        let row_id_output_index: Option<usize> = if self.config.emit_row_ids {
            match projection {
                Some(proj) => proj
                    .iter()
                    .position(|&idx| Some(idx) == row_id_col_in_full_schema),
                None => row_id_col_in_full_schema,
            }
        } else {
            None
        };

        // Output schema = what DataFusion expects (includes ___row_id if projected).
        // When computing row IDs, replace the ___row_id field type with UInt64.
        let output_schema: SchemaRef = {
            let base: SchemaRef = match projection {
                Some(proj) => Arc::new(full_schema.project(proj)?),
                None => full_schema.clone(),
            };
            if let Some(idx) = row_id_output_index {
                let mut fields: Vec<Field> =
                    base.fields().iter().map(|f| f.as_ref().clone()).collect();
                fields[idx] = Field::new(crate::ROW_ID_COLUMN_NAME, DataType::Int64, false);
                Arc::new(Schema::new(fields))
            } else {
                base
            }
        };

        // Read projection = output columns (minus ___row_id) + predicate columns for evaluator.
        let read_projection: Option<Vec<usize>> = if self.config.emit_row_ids {
            let output_cols: Vec<usize> = match projection {
                Some(proj) => proj
                    .iter()
                    .filter(|&&idx| Some(idx) != row_id_col_in_full_schema)
                    .copied()
                    .collect(),
                None => (0..full_schema.fields().len())
                    .filter(|&idx| Some(idx) != row_id_col_in_full_schema)
                    .collect(),
            };
            let mut cols = output_cols;
            for &idx in &self.config.predicate_columns {
                if !cols.contains(&idx) {
                    cols.push(idx);
                }
            }
            cols.sort();
            Some(cols)
        } else if self.config.predicate_columns.is_empty() {
            projection.cloned()
        } else {
            projection.map(|proj| {
                let mut cols = proj.clone();
                for &idx in &self.config.predicate_columns {
                    if !cols.contains(&idx) {
                        cols.push(idx);
                    }
                }
                cols.sort();
                cols
            })
        };

        let projected_schema = output_schema;

        // Ignore DataFusion's `filters` argument. The `index_filter(...)`
        // UDF call would be in there (its body panics), and the
        // BoolNode tree held by the evaluator factory already contains
        // the full WHERE semantics.
        //
        // The pushdown predicate — the parquet-native residual to hand
        // to `ParquetSource::with_predicate` — is derived from the
        // BoolNode in `execute_indexed_query` and stashed on the
        // config by that caller.
        let predicate = self.config.pushdown_predicate.clone();

        // Row-group-aligned partition assignments
        let layouts: Vec<SegmentLayout> = self
            .config
            .segments
            .iter()
            .map(|seg| SegmentLayout {
                row_groups: seg.row_groups.clone(),
            })
            .collect();

        // Decide whether to use the sort-aware path: requires a configured
        // index sort, segments that chain on the lead sort key (per-segment
        // min/max are disjoint), `target_partitions >= num_segments` so the
        // optimizer's chain check at `file_scan_config.rs:551` would accept,
        // and that we're not on the QTF row-id-emit path (gated for v1).
        let target_partitions = self.config.query_config.target_partitions.max(1);
        let chain_ok = !self.config.sort_fields.is_empty()
            && !self.config.emit_row_ids
            && segments_chain_on_sort_key(&self.config.segments)
            && target_partitions >= self.config.segments.len();

        // Build the LexOrdering against the projected schema. If the lead
        // sort field is projected away, this returns None and we fall back to
        // the row-count partitioning. Mirror of `create_ordering` at
        // `physical-expr/src/physical_expr.rs:134` — break on first
        // unresolvable column rather than erroring out.
        let lex_ordering = if chain_ok {
            build_projected_lex_ordering(
                &projected_schema,
                &self.config.sort_fields,
                &self.config.sort_orders,
            )
        } else {
            None
        };

        let (assignments, eq_properties, advertised_ordering) = if chain_ok
            && lex_ordering.is_some()
        {
            let assignments = compute_assignments_one_per_segment(&self.config.segments, &layouts);
            let lex = lex_ordering.unwrap();
            let eq = EquivalenceProperties::new_with_orderings(
                projected_schema.clone(),
                vec![lex.clone()],
            );
            (assignments, eq, Some(lex))
        } else {
            let assignments = compute_assignments(&layouts, target_partitions);
            (
                assignments,
                EquivalenceProperties::new(projected_schema.clone()),
                None,
            )
        };

        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(assignments.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        // Surface the sort-aware-path decision as a counter so it's visible in
        // EXPLAIN ANALYZE / `metrics()` output: 1 when ordering was advertised
        // (chain held + lead column projected + non-QTF), 0 otherwise.
        let metrics = ExecutionPlanMetricsSet::new();
        let ordering_optimized: Count =
            MetricBuilder::new(&metrics).global_counter("ordering_optimized");
        if advertised_ordering.is_some() {
            ordering_optimized.add(1);
        }

        Ok(Arc::new(QueryShardExec {
            config: Arc::clone(&self.config),
            full_schema,
            projected_schema,
            projection: read_projection,
            assignments,
            properties,
            predicate,
            metrics,
            inner_parquet_metrics: Arc::new(std::sync::Mutex::new(Vec::new())),
            io_stats: Arc::new(ReadIoStats::default()),
            row_id_output_index,
            dynamic_filters: Vec::new(),
            advertised_ordering,
        }))
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

// ── QueryShardExec ───────────────────────────────────────────────────

/// One execution plan per query. Partitions into `assignments.len()` streams,
/// each backed by one or more `IndexedExec`s (chained per-chunk).
pub struct QueryShardExec {
    config: Arc<IndexedTableConfig>,
    full_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    assignments: Vec<PartitionAssignment>,
    properties: Arc<PlanProperties>,
    /// Residual physical predicate pushed down from the planner. Threaded
    /// into each `IndexedExec` so `ParquetSource.with_predicate(...)` can
    /// apply it during decode.
    predicate: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    metrics: ExecutionPlanMetricsSet,
    inner_parquet_metrics: Arc<std::sync::Mutex<Vec<MetricsSet>>>,
    io_stats: Arc<ReadIoStats>,
    /// Column index in the OUTPUT schema where computed `___row_id` should be
    /// injected. `None` means no row ID computation (normal data path).
    row_id_output_index: Option<usize>,
    /// Runtime dynamic filters accepted from a parent operator (typically a
    /// `SortExec`-TopK `DynamicFilterPhysicalExpr`) via physical filter
    /// pushdown. Each is read per row-group at execution time to prune RGs
    /// whose parquet statistics cannot satisfy the (tightening) predicate.
    /// Empty unless `handle_child_pushdown_result` accepted one.
    ///
    /// These reference only the SORT columns, never the WHERE clause — so they
    /// are orthogonal to the Lucene/parquet boolean split. See
    /// `docs/dynamic-filters-indexed-table-impl.md` §4b.
    dynamic_filters: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Sort ordering this scan claims to produce — `Some` only when the
    /// sort-aware partitioning fired (chain holds + lead column is in the
    /// projected schema). Each `IndexedExec` spawned by `execute()` advertises
    /// the same ordering so DataFusion's `EnforceSorting` can substitute
    /// `SortPreservingMergeExec` for the outer `SortExec(TopK)`.
    advertised_ordering: Option<LexOrdering>,
}

impl fmt::Debug for QueryShardExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryShardExec")
            .field("partitions", &self.assignments.len())
            .field("segments", &self.config.segments.len())
            .finish()
    }
}

impl DisplayAs for QueryShardExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> fmt::Result {
        // `ordering=` shows whether the sort-aware path fired:
        //   - `unsorted`: no `index.sort.field`, OR sort field set but the
        //     sort-aware path didn't fire (chain didn't hold, target_partitions
        //     too low, lead column projected away, or QTF row-id-emit gate).
        //   - `sorted=[<col> ASC|DESC, ...]`: chain held, output_ordering
        //     advertised → DataFusion can substitute SortPreservingMergeExec.
        write!(
            f,
            "QueryShardExec: partitions={}, segments={}, ordering={}",
            self.assignments.len(),
            self.config.segments.len(),
            describe_ordering(&self.advertised_ordering),
        )
    }
}

fn describe_ordering(ordering: &Option<LexOrdering>) -> String {
    match ordering {
        None => "unsorted".to_string(),
        Some(lex) => {
            use std::fmt::Write;
            let mut s = String::from("sorted=[");
            for (i, e) in lex.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                let dir = if e.options.descending { "DESC" } else { "ASC" };
                let _ = write!(&mut s, "{} {}", e.expr, dir);
            }
            s.push(']');
            s
        }
    }
}

impl ExecutionPlan for QueryShardExec {
    fn name(&self) -> &str {
        "QueryShardExec"
    }
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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
    fn metrics(&self) -> Option<MetricsSet> {
        let mut combined = self.metrics.clone_inner();
        if let Ok(inner) = self.inner_parquet_metrics.lock() {
            for set in inner.iter() {
                for m in set.iter() {
                    let name = m.value().name();
                    if name == "output_rows" || name == "output_batches" || name == "output_bytes" {
                        continue;
                    }
                    combined.push(m.clone());
                }
            }
        }
        Some(combined)
    }

    /// Accept runtime dynamic filters (TopK / join) pushed from a parent.
    ///
    /// `QueryShardExec` is a leaf (no children), so the default
    /// `gather_filters_for_pushdown` already returns an empty description — the
    /// parent's self-filter arrives here as `parent_filters`. We accept a filter
    /// only in the `Post` phase and only when every column it references is a
    /// readable parquet column in our schema (so per-RG statistics pruning is
    /// possible). Anything else is declined, leaving the parent's safety-net
    /// `FilterExec` in place — declining is always correctness-safe.
    fn handle_child_pushdown_result(
        &self,
        phase: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase,
        child_pushdown_result: datafusion::physical_plan::filter_pushdown::ChildPushdownResult,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<
        datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation<
            Arc<dyn ExecutionPlan>,
        >,
    > {
        use datafusion::physical_plan::filter_pushdown::{
            FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
        };

        // Only the Post phase carries dynamic filters; in Pre we own static
        // WHERE semantics via the BoolNode tree and want no interference.
        if phase != FilterPushdownPhase::Post {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }

        let mut statuses = Vec::with_capacity(child_pushdown_result.parent_filters.len());
        let mut accepted: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = Vec::new();
        for f in &child_pushdown_result.parent_filters {
            if self.dynamic_filter_is_acceptable(&f.filter) {
                statuses.push(PushedDown::Yes);
                accepted.push(Arc::clone(&f.filter));
            } else {
                statuses.push(PushedDown::No);
            }
        }

        if accepted.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                statuses,
            ));
        }

        let new_self = self.clone_with_dynamic_filters(accepted);
        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(statuses)
                .with_updated_node(Arc::new(new_self) as Arc<dyn ExecutionPlan>),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let assignment = self.assignments.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!("partition {} out of range", partition))
        })?;

        let pmetrics = PartitionMetrics::new(&self.metrics, partition);
        let mut stream_metrics =
            pmetrics.into_stream_metrics(Some(Arc::clone(&self.inner_parquet_metrics)));
        stream_metrics.io_stats = Some(Arc::clone(&self.io_stats));

        let dynamic_filter: Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> = (!self
            .dynamic_filters
            .is_empty())
        .then(|| datafusion::physical_expr::utils::conjunction(self.dynamic_filters.clone()));

        let mut streams: Vec<SendableRecordBatchStream> =
            Vec::with_capacity(assignment.chunks.len());
        for chunk in &assignment.chunks {
            let segment = self.config.segments.get(chunk.segment_idx).ok_or_else(|| {
                DataFusionError::Internal(format!("segment_idx {} out of range", chunk.segment_idx))
            })?;

            let rg_set: HashSet<usize> = chunk.row_group_indices.iter().copied().collect();
            let row_groups: Vec<RowGroupInfo> = segment
                .row_groups
                .iter()
                .filter(|rg| rg_set.contains(&rg.index))
                .cloned()
                .collect();

            if row_groups.is_empty() {
                continue;
            }

            // Build stats prune tree for segment/RG/subtree-level pruning.
            let stats_prune_tree =
                self.config
                    .prune_tree_config
                    .as_ref()
                    .map(|(tree, preds, schema)| {
                        let rg_indices: Vec<usize> = row_groups.iter().map(|rg| rg.index).collect();
                        Arc::new(StatsPruneTree::build_from_bool_node(
                            tree,
                            preds,
                            &segment.metadata,
                            schema,
                            &rg_indices,
                        ))
                    });

            // Segment-level skip: if no RG in the chunk can match, skip entirely.
            if let Some(ref spt) = stats_prune_tree {
                if !spt.rg_can_match.iter().any(|&k| k) {
                    native_bridge_common::log_debug!(
                        "[segment-skip] skipping chunk — pruned by segment-level stats"
                    );
                    continue;
                }
            }

            let evaluator = (self.config.evaluator_factory)(
                segment,
                chunk,
                &stream_metrics,
                stats_prune_tree.as_ref(),
            )
            .map_err(|e| DataFusionError::External(e.into()))?;

            // When the sort-aware path fired (`advertised_ordering: Some`), the
            // chain-aware partitioning guarantees this chunk is one whole
            // segment, and the writer's k-way-merge guarantees rows in this
            // segment are in lead-key order. So this `IndexedExec` produces a
            // sorted run and we advertise the same ordering as the parent
            // `QueryShardExec`. Without this, `EnforceSorting` can't see that
            // input partitions are already sorted and won't substitute
            // `SortPreservingMergeExec` for the outer `SortExec(TopK)`.
            let exec_eq_props = match &self.advertised_ordering {
                Some(lex) => EquivalenceProperties::new_with_orderings(
                    self.projected_schema.clone(),
                    vec![lex.clone()],
                ),
                None => EquivalenceProperties::new(self.projected_schema.clone()),
            };
            let props = Arc::new(PlanProperties::new(
                exec_eq_props,
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ));

            let exec = IndexedExec {
                schema: self.projected_schema.clone(),
                full_schema: self.full_schema.clone(),
                object_path: segment.object_path.clone(),
                file_size: segment.parquet_size,
                store: Arc::clone(&self.config.store),
                store_url: self.config.store_url.clone(),
                row_groups,
                projection: self.projection.clone(),
                properties: props,
                metadata: Arc::clone(&segment.metadata),
                predicate: self.predicate.clone(),
                evaluator: std::sync::Mutex::new(Some(evaluator)),
                doc_range: Some((chunk.doc_min, chunk.doc_max)),
                metrics: ExecutionPlanMetricsSet::new(),
                stream_metrics: stream_metrics.clone(),
                query_config: Arc::clone(&self.config.query_config),
                global_base: segment.global_base,
                emit_row_ids: self.config.emit_row_ids,
                row_id_output_index: self.row_id_output_index,
                dynamic_filter: dynamic_filter.clone(),
                cancellation_token: self.config.cancellation_token.clone(),
            };
            streams.push(exec.execute(0, Arc::clone(&context))?);
        }

        let stream: SendableRecordBatchStream = match streams.len() {
            0 => {
                let empty =
                    datafusion::physical_plan::empty::EmptyExec::new(self.projected_schema.clone());
                empty.execute(0, context)?
            }
            1 => streams.into_iter().next().unwrap(),
            _ => {
                let schema = self.projected_schema.clone();
                let chained = futures::stream::iter(streams).flatten();
                Box::pin(RecordBatchStreamAdapter::new(schema, chained))
            }
        };
        Ok(stream)
    }
}

impl Drop for QueryShardExec {
    fn drop(&mut self) {
        accumulate_from_exec(&self.metrics, &self.inner_parquet_metrics, &self.io_stats);
    }
}

impl QueryShardExec {
    /// True if `filter` can be used for per-RG statistics pruning: every column
    /// it references must exist in our full (parquet) schema. Dynamic filters
    /// reference sort columns; a sort on a Lucene-only / computed column (not in
    /// the parquet file) is declined so we never try to prune on absent stats.
    ///
    /// Conservative by design — a `false` here just keeps the parent's
    /// `FilterExec` and forgoes the optimization; it can never drop a row.
    fn dynamic_filter_is_acceptable(
        &self,
        filter: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    ) -> bool {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::physical_expr::expressions::Column;

        let mut all_cols_known = true;
        let mut saw_column = false;
        let _ = filter.apply(|e| {
            if let Some(c) = e.downcast_ref::<Column>() {
                saw_column = true;
                if self.full_schema.index_of(c.name()).is_err() {
                    all_cols_known = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        // Require at least one resolved column — a column-free predicate (e.g.
        // the bare `true` placeholder) carries no pruning signal.
        saw_column && all_cols_known
    }

    /// Rebuild this exec with accepted dynamic filters attached. `QueryShardExec`
    /// holds a non-`Clone` `ExecutionPlanMetricsSet`; we mint a fresh one (the
    /// pushdown rewrite happens before execution, so no metrics are lost) and
    /// reuse the shared `Arc` fields verbatim.
    fn clone_with_dynamic_filters(
        &self,
        dynamic_filters: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    ) -> Self {
        QueryShardExec {
            config: Arc::clone(&self.config),
            full_schema: self.full_schema.clone(),
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            assignments: self.assignments.clone(),
            properties: Arc::clone(&self.properties),
            predicate: self.predicate.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            inner_parquet_metrics: Arc::clone(&self.inner_parquet_metrics),
            io_stats: Arc::clone(&self.io_stats),
            row_id_output_index: self.row_id_output_index,
            dynamic_filters,
            advertised_ordering: self.advertised_ordering.clone(),
        }
    }
}

#[cfg(test)]
impl QueryShardExec {
    /// Test-only accessor for the conjoined physical predicate produced
    /// by `scan()`. `None` when no filters were pushed down.
    pub(crate) fn test_predicate(
        &self,
    ) -> Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        self.predicate.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{col, lit};
    use datafusion::prelude::SessionContext;

    fn empty_config() -> IndexedTableConfig {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        IndexedTableConfig {
            schema,
            segments: Vec::new(),
            store: Arc::new(object_store::local::LocalFileSystem::new()),
            store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
            // Evaluator factory would never be invoked for this test (no segments).
            evaluator_factory: Arc::new(|_, _, _, _| unreachable!()),
            pushdown_predicate: None,
            query_config: std::sync::Arc::new(
                crate::datafusion_query_config::DatafusionQueryConfig::test_default(),
            ),
            predicate_columns: vec![],
            emit_row_ids: false,
            prune_tree_config: None,
            sort_fields: vec![],
            sort_orders: vec![],
            cancellation_token: None,
        }
    }

    // QueryShardExec holds an ExecutionPlanMetricsSet (not Clone). We only
    // need to inspect `.predicate`, so read through a reference.
    async fn scan_predicate(
        provider: &IndexedTableProvider,
        filters: &[Expr],
    ) -> Option<Arc<dyn datafusion::physical_expr::PhysicalExpr>> {
        let ctx = SessionContext::new();
        let plan = provider
            .scan(&ctx.state(), None, filters, None)
            .await
            .expect("scan");
        let shard = plan
            .downcast_ref::<QueryShardExec>()
            .expect("scan returns QueryShardExec");
        shard.test_predicate().cloned()
    }

    #[tokio::test]
    async fn scan_with_no_filters_produces_none_predicate() {
        let provider = IndexedTableProvider::new(empty_config());
        let pred = scan_predicate(&provider, &[]).await;
        assert!(pred.is_none(), "no filters → no predicate");
    }
}
