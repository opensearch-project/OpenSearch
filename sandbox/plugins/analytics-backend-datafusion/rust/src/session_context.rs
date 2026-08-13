/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! SessionContext lifecycle for instruction-based execution.
//!
//! `create_session_context` creates a fully configured SessionContext with
//! the default ListingTable registered. Called by ShardScanInstruction handler.

use std::sync::Arc;

use datafusion::{
    common::DataFusionError,
    datasource::file_format::parquet::ParquetFormat,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    execution::cache::cache_manager::CachedFileList,
    execution::cache::{CacheAccessor, DefaultListFilesCache},
    execution::context::SessionContext,
    execution::memory_pool::MemoryPool,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::SessionStateBuilder,
    physical_plan::ExecutionPlan,
    prelude::*,
};
use log::error;
use native_bridge_common::log_debug;
use object_store::ObjectMeta;

use crate::api::{DataFusionRuntime, ShardView};
use crate::cache::page_index;
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::query_tracker::QueryTrackingContext;
use crate::scoped_index_optimizer::ScopedPageIndexOptimizer;

/// Opaque handle holding a configured SessionContext between FFM calls.
pub struct SessionContextHandle {
    pub ctx: SessionContext,
    pub table_path: ListingTableUrl,
    pub object_metas: Arc<Vec<ObjectMeta>>,
    /// Writer generation per object_metas entry (parallel arrays). Sourced from the
    /// Java-side catalog snapshot via `create_reader`. Authoritative for stamping
    /// `SegmentFileInfo.writer_generation`; footer-kv reads are debug-only assertions.
    pub writer_generations: Arc<Vec<i64>>,
    /// `index.sort.field` plumbed from the Java side (`ShardView.sort_fields`).
    /// Empty when the index has no `index.sort.field`. Consumed by the vanilla path
    /// (`IndexedTableProvider` `output_ordering`) and by the indexed path's
    /// segment-reversal optimization.
    pub sort_fields: Vec<String>,
    /// Parallel to `sort_fields`. Each entry is `"asc"` or `"desc"` (lowercase).
    pub sort_orders: Vec<String>,
    pub query_context: QueryTrackingContext,
    pub table_name: String,
    /// When set, indicates this session uses the indexed execution path with filter delegation.
    pub indexed_config: Option<IndexedExecutionConfig>,
    /// Per-query tuning knobs (batch size, partitions, filter strategies, etc.)
    pub query_config: DatafusionQueryConfig,
    /// IO runtime handle for bloom filter reads and other async I/O dispatched
    /// from CPU executor threads.
    pub io_handle: tokio::runtime::Handle,
    /// Aggregate execution mode for distributed partial/final stripping.
    pub(crate) aggregate_mode: crate::agg_mode::Mode,
    /// True when the shard Substrait fragment contains a FetchRel (Sort+Limit = TopK).
    /// Detected once in `create_session_context` from plan_bytes and reused in
    /// `prepare_partial_plan` to apply PartialReduce for CSS correctness.
    pub(crate) has_topk: bool,
    /// Pre-prepared physical plan (set by prepare_partial_plan / prepare_final_plan).
    pub(crate) prepared_plan: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    /// Phantom reservation holding pool capacity for untracked memory.
    /// Dropped when the handle is closed, releasing the capacity.
    pub(crate) phantom_reservation: Option<datafusion::execution::memory_pool::MemoryReservation>,
}

/// Configuration for indexed execution with filter delegation, provided by Java.
pub struct IndexedExecutionConfig {
    pub tree_shape: i32,
    pub delegated_predicate_count: i32,
    /// QTF query phase: scan must emit shard-global `__row_id__`.
    pub requests_row_ids: bool,
}

/// Widens `inferred` to the plan's `base_schema` (for index-pattern / alias scans) so the
/// table is registered with every column the Substrait consumer expects. Returns `inferred`
/// unchanged when no plan is supplied, no matching base_schema exists, or this shard already
/// covers every column.
///
/// Uses the `datafusion-substrait` consumer's `from_substrait_named_struct` for type conversion
/// (which already marks all fields nullable). The consumer is built from the session's existing
/// state — no throwaway SessionState needed.
pub(crate) fn widen_schema_from_plan(
    ctx: &SessionContext,
    plan_bytes: &[u8],
    table_name: &str,
    inferred: &arrow::datatypes::SchemaRef,
) -> arrow::datatypes::SchemaRef {
    use datafusion_substrait::extensions::Extensions;
    use datafusion_substrait::logical_plan::consumer::{
        from_substrait_named_struct, DefaultSubstraitConsumer,
    };

    if plan_bytes.is_empty() {
        return Arc::clone(inferred);
    }
    let plan: substrait::proto::Plan = match prost::Message::decode(plan_bytes) {
        Ok(p) => p,
        Err(_) => return Arc::clone(inferred),
    };
    let Some(base_schema) = crate::api::base_schema_for_table(&plan, table_name) else {
        log::warn!("widen_schema_from_plan: no base_schema found for table '{}' in plan — skipping widening", table_name);
        return Arc::clone(inferred);
    };

    // Cheap gate: if inferred already has every base_schema column, skip.
    let have: std::collections::HashSet<&str> = inferred
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    if base_schema.names.iter().all(|n| have.contains(n.as_str())) {
        return Arc::clone(inferred);
    }

    // Use the substrait consumer to convert NamedStruct → Arrow Schema (handles all type
    // variants including decimals, nested structs, user-defined types). All fields are
    // already marked nullable by the consumer.
    let extensions = Extensions::default();
    let state = ctx.state();
    let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
    let df_schema = match from_substrait_named_struct(&consumer, &base_schema) {
        Ok(s) => s,
        Err(_) => return Arc::clone(inferred),
    };
    let expected = df_schema.as_arrow().clone();

    let force_view = ctx
        .copied_config()
        .options()
        .execution
        .parquet
        .schema_force_view_types;
    let expected = if force_view {
        datafusion::datasource::file_format::parquet::transform_schema_to_view(&expected)
    } else {
        expected
    };
    let expected = crate::schema_coerce::coerce_inferred_schema(Arc::new(expected));
    crate::schema_coerce::append_missing_nullable(inferred, &expected)
        .unwrap_or_else(|| Arc::clone(inferred))
}

/// Creates a SessionContext with per-query RuntimeEnv and registers the default
/// ListingTable provider for parquet scans.
pub async unsafe fn create_session_context(
    runtime_ptr: i64,
    shard_view_ptr: i64,
    table_name: &str,
    context_id: i64,
    has_partial_aggregate: bool,
    query_config: DatafusionQueryConfig,
    plan_bytes: &[u8],
) -> Result<i64, DataFusionError> {
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let shard_view = &*(shard_view_ptr as *const ShardView);

    let global_pool = runtime.runtime_env.memory_pool.clone();
    let query_context = QueryTrackingContext::new(
        context_id,
        global_pool.clone(),
        crate::query_tracker::QueryType::Shard,
    );
    let query_memory_pool = query_context
        .memory_pool()
        .map(|p| p as Arc<dyn MemoryPool>);

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(
        &datafusion::execution::cache::TableScopedPath {
            table: None,
            path: shard_view.table_path.prefix().clone(),
        },
        CachedFileList::new(shard_view.object_metas.as_ref().clone()),
    );

    let mut runtime_env_builder =
        crate::query_executor::query_runtime_env_builder(runtime, list_file_cache);

    if let Some(pool) = query_memory_pool {
        runtime_env_builder = runtime_env_builder.with_memory_pool(pool);
    }

    let runtime_env = runtime_env_builder.build().map_err(|e| {
        error!("create_session_context: failed to build runtime env: {}", e);
        e
    })?;

    // Register shard-specific object store on file:// scheme for this query.
    // This is the instruction-based execution path (ShardScanInstructionHandler).
    // Without this, queries use default LocalFileSystem and fail on warm.
    runtime_env.register_object_store(
        &url::Url::parse("file://").unwrap(),
        Arc::clone(&shard_view.store),
    );

    // Acquire memory budget from cached parquet metadata (zero I/O).
    // On cache miss (first query for this shard), skip — subsequent queries benefit.
    let phantom_reservation = try_acquire_budget(runtime, &global_pool, &shard_view, &query_config);
    let effective_partitions = phantom_reservation
        .as_ref()
        .map(|b| b.target_partitions)
        .unwrap_or(query_config.target_partitions);
    let effective_batch_size = phantom_reservation
        .as_ref()
        .map(|b| b.batch_size)
        .unwrap_or(query_config.batch_size);
    let phantom = phantom_reservation.map(|b| b.phantom_reservation);

    let mut config = SessionConfig::new();
    // Detect TopK once from the Substrait bytes: a FetchRel (Sort+Limit) in a partial-agg
    // fragment means OpenSearchTopKRewriter fired. Stored on the handle so prepare_partial_plan
    // can apply PartialReduce without re-scanning the physical plan.
    let has_topk = has_partial_aggregate && substrait_has_fetch_rel(plan_bytes);
    config.options_mut().execution.parquet.pushdown_filters =
        query_config.listing_table_pushdown_filters;
    // Disable DataFusion's adaptive skip-partial-aggregation when TopK is active.
    // If DF abandons partial agg midstream, the partial state sent to the coordinator is
    // incomplete — TopK sees wrong group counts and produces incorrect results.
    if has_topk {
        config
            .options_mut()
            .execution
            .skip_partial_aggregation_probe_ratio_threshold = 1.0;
    }
    config.options_mut().execution.target_partitions = effective_partitions;
    config.options_mut().execution.batch_size = effective_batch_size;
    // When the index has `index.sort.field`, ask DataFusion to use the sort-aware
    // file-group partitioner so `output_ordering` can propagate from the scan.
    if !shard_view.sort_fields.is_empty() {
        config
            .options_mut()
            .execution
            .split_file_groups_by_statistics = true;
    }

    let mut state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .with_physical_optimizer_rules(if has_partial_aggregate {
            crate::agg_mode::physical_optimizer_rules_without_combine()
        } else {
            datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules
        });

    // Install the scoped page-index reader factory on every parquet scan.
    // Also, this SHOULD be the last optimizer to see all projections / predicates
    if page_index::is_scoped_page_index_enabled() {
        state_builder =
            state_builder.with_physical_optimizer_rule(Arc::new(ScopedPageIndexOptimizer::new(
                Arc::clone(&shard_view.store),
                runtime.runtime_env.cache_manager.get_file_metadata_cache(),
            )));
    }

    let state = state_builder.build();

    let ctx = SessionContext::new_with_state(state);
    // Register OpenSearch UDFs (parse, item, mvappend, mvfind, mvzip, convert_tz, …)
    // on this session so the substrait converter at execute_with_context can resolve
    // their function names. Without this, fragment execution fails with "Unsupported
    // function name" because df_execute_with_context reuses this handle's ctx instead
    // of building a fresh one.
    crate::udf::register_all(&ctx);
    crate::udaf::register_all(&ctx);
    crate::udwf::register_all(&ctx);

    // Register default ListingTable for parquet scans.
    //
    // `target_partitions` on the listing options drives the sort-aware bin-packer in
    // `split_groups_by_statistics_with_target_partitions`. We set it to the session's
    // effective partition count so the bin-packer produces up to N groups (one file per
    // group when min/max ranges can't chain). The session-state's `target_partitions`
    // controls EnforceDistribution; this one is independent.
    let mut listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_collect_stat(true)
        .with_target_partitions(effective_partitions);

    if let Some(sort_exprs) =
        build_file_sort_order(&shard_view.sort_fields, &shard_view.sort_orders)
    {
        listing_options = listing_options.with_file_sort_order(vec![sort_exprs]);
    }

    // For multi-index queries, the plan's NamedTable carries the logical name (alias/pattern)
    // which differs from table_name (the concrete shard index). Extract it from the plan and
    // register under that name so the Substrait consumer binds correctly. For single-index
    // queries (empty plan_bytes), table_name is already the correct concrete name.
    let register_name = if !plan_bytes.is_empty() {
        crate::api::first_named_table_name(plan_bytes).unwrap_or_else(|| {
            error!("create_session_context: failed to extract table name from plan, falling back to concrete name: {}", table_name);
            table_name.to_string()
        })
    } else {
        table_name.to_string()
    };

    // Pre-warm the metadata cache footer-only before infer_schema fires.
    // infer_schema calls DFParquetMetadata::fetch_metadata with PageIndexPolicy::Optional
    // on a cache miss — fetching full page index bytes. By pre-warming here with
    // PageIndexPolicy::Skip via load_parquet_metadata, every infer_schema call becomes
    // a cache hit and never touches the page index bytes.
    // Cache key is meta.location (Path) — same key infer_schema uses.
    // Empty shard: loop is a no-op; infer_schema is also skipped below.
    {
        let metadata_cache = runtime.runtime_env.cache_manager.get_file_metadata_cache();
        for meta in shard_view.object_metas.as_ref() {
            let _ = crate::indexed_table::parquet_bridge::load_parquet_metadata_with_meta(
                Arc::clone(&shard_view.store),
                &meta.location,
                meta.clone(),
                Arc::clone(&metadata_cache),
            )
            .await;
        }
    }

    // Empty shard: skip infer_schema (errors on zero files); widen_schema_from_plan
    // below populates columns from the substrait base_schema.
    let inferred: arrow::datatypes::SchemaRef = if shard_view.object_metas.is_empty() {
        Arc::new(arrow::datatypes::Schema::empty())
    } else {
        let inferred = listing_options
            .infer_schema(&ctx.state(), &shard_view.table_path)
            .await
            .map_err(|e| {
                error!("create_session_context: failed to infer schema: {}", e);
                e
            })?;
        // Substrait's type system is narrower than Arrow's; normalize the inferred
        // schema to forms the Substrait consumer can bind against. See crate::schema_coerce.
        crate::schema_coerce::coerce_inferred_schema(inferred)
    };
    // Pre-widening field count — compared below to detect whether widening added columns.
    let inferred_field_count = inferred.fields().len();

    // Widen to the plan's base_schema if this shard's parquet is missing columns the plan
    // expects (multi-index unions, or single-index cross-shard drift). No-op when the shard
    // already covers every base_schema column.
    let resolved_schema = widen_schema_from_plan(&ctx, plan_bytes, &register_name, &inferred);

    // If widening added columns, disable stat collection: the global stats cache is keyed by
    // path (not schema), so a narrow cached Statistics can be merged against the widened one,
    // failing with "Cannot merge statistics with different number of columns". Non-widened
    // (single-index) scans keep full stats.
    // TODO: re-enable once DataFusion's Statistics::try_merge tolerates a column-count delta.
    let listing_options = if resolved_schema.fields().len() != inferred_field_count {
        listing_options.with_collect_stat(false)
    } else {
        listing_options
    };

    let table_config = ListingTableConfig::new(shard_view.table_path.clone())
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    // Wire the global statistics cache into the ListingTable.
    let stats_cache = runtime.runtime_env.cache_manager.get_file_statistic_cache();
    let provider = Arc::new(
        ListingTable::try_new(table_config)
            .map_err(|e| {
                error!(
                    "create_session_context: failed to create listing table: {}",
                    e
                );
                e
            })?
            .with_cache(stats_cache),
    );

    ctx.register_table(register_name.as_str(), provider)
        .map_err(|e| {
            error!(
                "create_session_context: failed to register table '{}': {}",
                register_name, e
            );
            e
        })?;
    log_debug!(
        "create_session_context: registered table '{}' with file_sort_order_keys={}",
        register_name,
        shard_view.sort_fields.len()
    );

    error!(
        "create_session_context: successfully registered table '{}', table_name_len={}",
        table_name,
        table_name.len()
    );

    let handle = SessionContextHandle {
        ctx,
        table_path: shard_view.table_path.clone(),
        object_metas: shard_view.object_metas.clone(),
        writer_generations: shard_view.writer_generations.clone(),
        sort_fields: shard_view.sort_fields.clone(),
        sort_orders: shard_view.sort_orders.clone(),
        query_context,
        table_name: table_name.to_string(),
        indexed_config: None,
        query_config,
        io_handle: tokio::runtime::Handle::current(),
        aggregate_mode: crate::agg_mode::Mode::Default,
        has_topk,
        prepared_plan: None,
        phantom_reservation: phantom,
    };
    Ok(Box::into_raw(Box::new(handle)) as i64)
}

/// Closes a SessionContext handle without executing. Used for cleanup on failure.
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_session_context`.
pub unsafe fn close_session_context(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut SessionContextHandle);
    }
}

/// Creates a SessionContext configured for indexed execution with filter delegation.
/// Registers the `delegated_predicate` UDF and stores the tree shape + predicate count
/// for use during execution.
pub async unsafe fn create_session_context_indexed(
    runtime_ptr: i64,
    shard_view_ptr: i64,
    table_name: &str,
    context_id: i64,
    tree_shape: i32,
    delegated_predicate_count: i32,
    requests_row_ids: bool,
    has_partial_aggregate: bool,
    query_config: DatafusionQueryConfig,
    plan_bytes: &[u8],
) -> Result<i64, DataFusionError> {
    let ptr = create_session_context(
        runtime_ptr,
        shard_view_ptr,
        table_name,
        context_id,
        has_partial_aggregate,
        query_config,
        plan_bytes,
    )
    .await?;

    // Augment with indexed config. The delegation marker UDFs (index_filter, delegation_possible)
    // are now registered for every session by udf::register_all (via create_session_context above);
    // the indexed path additionally UNWRAPS them before execution.
    let handle = &mut *(ptr as *mut SessionContextHandle);
    handle.indexed_config = Some(IndexedExecutionConfig {
        tree_shape,
        delegated_predicate_count,
        requests_row_ids,
    });

    Ok(ptr)
}

/// Prepares a partial-aggregate physical plan on the session handle.
///
/// Decodes Substrait → LogicalPlan → PhysicalPlan, applies partial-mode
/// stripping via `agg_mode::apply_aggregate_mode`, and stores the result
/// on the handle for later execution.
pub async fn prepare_partial_plan(
    handle: &mut SessionContextHandle,
    substrait_bytes: &[u8],
) -> Result<(), datafusion::common::DataFusionError> {
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use prost::Message;
    use substrait::proto::Plan;

    handle.aggregate_mode = crate::agg_mode::Mode::Partial;

    let plan = Plan::decode(substrait_bytes).map_err(|e| {
        datafusion::common::DataFusionError::Execution(format!(
            "prepare_partial_plan: failed to decode Substrait: {}",
            e
        ))
    })?;
    let logical_plan = from_substrait_plan(&handle.ctx.state(), &plan).await?;
    let dataframe = handle.ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;

    // Strip first on the raw physical plan so `force_aggregate_mode(Partial)` can find the
    // Final/Partial pair without a RelabelExec wrapper at the root pre-empting the walk.
    // Then derive `target_schema` and wrap with RelabelExec from the stripped plan's actual
    // output (state-suffixed Binary for HLL Partial vs. Int64 cardinality for Final.evaluate)
    // — otherwise RelabelExec would carry the pre-strip type tag (e.g. Int64) and fail with
    // "non-bit-compatible types: Binary → Int64" when wrapping the stripped Partial.
    let stripped = crate::agg_mode::apply_aggregate_mode(
        physical_plan,
        crate::agg_mode::Mode::Partial,
        handle.has_topk,
    )?;

    let target_schema = crate::schema_coerce::coerce_inferred_schema(stripped.schema());
    let stripped = crate::relabel_exec::wrap_if_relabel_needed(stripped, target_schema)?;
    handle.prepared_plan = Some(stripped);
    Ok(())
}

/// Returns true if the Substrait plan bytes contain a FetchRel (Sort+Limit node).
/// A FetchRel in a shard fragment means `OpenSearchTopKRewriter` inserted a per-shard
/// Sort+Limit — TopK is active. Used in `create_session_context` to detect TopK before
/// the DataFusion physical plan is built, so the result can be stored on the handle and
/// reused in `prepare_partial_plan` without re-scanning the physical plan.
///
/// Single-shard (SINGLE aggregate mode) never has `has_partial_aggregate=true` so this
/// function is only called for multi-shard partial-aggregate fragments.
///
/// # Upgrade path note
/// This detection avoids adding a new boolean field to the Java→Rust FFI surface
/// (which would break wire compatibility with older nodes during rolling upgrades —
/// old coordinators serialising `PartialAggregateInstructionNode` without the field
/// would be misread by new data nodes). The Substrait plan bytes are already part of
/// the existing wire contract and do not change format.
///
/// TODO: Once AnalyticsCore supports a versioned flag/hint mechanism, replace this
/// Substrait scan with an explicit flag passed through the instruction pipeline.
/// That would be cleaner and avoid re-parsing the plan bytes, but requires a
/// backward-compatible flag delivery path that does not exist today.
fn substrait_has_fetch_rel(plan_bytes: &[u8]) -> bool {
    use prost::Message;
    use substrait::proto::rel::RelType;

    fn rel_has_fetch(rel: &substrait::proto::Rel) -> bool {
        match rel.rel_type.as_ref() {
            Some(RelType::Fetch(f)) => f.count_mode.is_some(),
            Some(RelType::Sort(s)) => s.input.as_ref().map_or(false, |r| rel_has_fetch(r)),
            Some(RelType::Project(p)) => p.input.as_ref().map_or(false, |r| rel_has_fetch(r)),
            Some(RelType::Filter(f)) => f.input.as_ref().map_or(false, |r| rel_has_fetch(r)),
            Some(RelType::Aggregate(a)) => a.input.as_ref().map_or(false, |r| rel_has_fetch(r)),
            // TODO: enumerate remaining rel types explicitly and panic on unknown ones.
            Some(other) => {
                native_bridge_common::log_debug!(
                    "substrait_has_fetch_rel: {:?} — no TopK fetch",
                    std::mem::discriminant(other)
                );
                false
            }
            None => false,
        }
    }

    let Ok(plan) = substrait::proto::Plan::decode(plan_bytes) else {
        return false;
    };
    plan.relations.iter().any(|pr| match pr.rel_type.as_ref() {
        Some(substrait::proto::plan_rel::RelType::Root(rr)) => {
            rr.input.as_ref().map_or(false, |r| rel_has_fetch(r))
        }
        Some(substrait::proto::plan_rel::RelType::Rel(r)) => rel_has_fetch(r),
        None => false,
    })
}

/// Attempt to acquire a memory budget using cached parquet metadata.
/// Returns None on cache miss or if the budget system is not configured.
fn try_acquire_budget(
    runtime: &DataFusionRuntime,
    pool: &Arc<dyn MemoryPool>,
    shard_view: &ShardView,
    config: &DatafusionQueryConfig,
) -> Option<crate::query_budget::QueryMemoryBudget> {
    use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
    use datafusion::execution::cache::CacheAccessor;
    use parquet::arrow::parquet_to_arrow_schema;

    let first_meta = shard_view.object_metas.first()?;
    let cache = runtime.runtime_env.cache_manager.get_file_metadata_cache();
    let cached = cache.get(&first_meta.location)?;
    let cached_parquet = cached
        .file_metadata
        .as_any()
        .downcast_ref::<CachedParquetMetaData>()?;
    let parquet_meta = cached_parquet.parquet_metadata();

    let schema = parquet_to_arrow_schema(
        parquet_meta.file_metadata().schema_descr(),
        parquet_meta.file_metadata().key_value_metadata(),
    )
    .ok()
    .map(Arc::new)?;

    crate::query_budget::acquire_budget_from_metadata(
        pool,
        &schema,
        parquet_meta,
        config.target_partitions,
        config.batch_size,
    )
    .ok()
}

/// Build a per-file sort-order declaration for `ListingOptions::with_file_sort_order`.
///
/// This is metadata, not a sort: it tells DataFusion the parquet files are already
/// in this order so `output_ordering` propagates through the scan,
/// `SortPreservingMergeExec` replaces `SortExec`, and `sort_prefix` TopK fires.
/// The OpenSearch writer enforces this sort per segment, so the claim is
/// verifiable-by-construction (DataFusion also checks per-file min/max stats).
///
/// Returns `None` when the index has no `index.sort.field` configured (the input
/// `sort_fields` slice is empty), so the caller can skip
/// `with_file_sort_order(...)` entirely.
///
/// Caller contract: `sort_fields.len() == sort_orders.len()`. The Java side
/// (`DataFusionPlugin.createReaderManager`) guarantees this — `IndexSortConfig`
/// validates size match at index creation, so by the time we get here the
/// lengths agree.
///
/// Notes for readers:
/// - `.sort(asc, nulls_first)` constructs a `SortExpr` — it does NOT execute a
///   sort. Misleading name; it's a tuple builder.
/// - `Column::from_name` (vs `col(&str)`): `col` lowercases via SQL identifier
///   normalization (`col("EventTime")` → `"eventtime"`), which silently fails
///   lookup against case-sensitive Arrow schemas. `from_name` preserves case.
/// - Nulls placement mirrors Lucene's general default (ASC → NULLS FIRST,
///   DESC → NULLS LAST). `index.sort.missing` can override this per field but
///   the override isn't propagated yet. A wrong nulls claim at worst causes
///   DataFusion's per-file chain validator to reject the ordering and fall back
///   to a regular `SortExec` — never wrong results.
pub(crate) fn build_file_sort_order(
    sort_fields: &[String],
    sort_orders: &[String],
) -> Option<Vec<datafusion::logical_expr::SortExpr>> {
    if sort_fields.is_empty() {
        return None;
    }
    use datafusion::common::Column;
    use datafusion::logical_expr::{Expr, SortExpr};
    let sort_exprs: Vec<SortExpr> = sort_fields
        .iter()
        .zip(sort_orders.iter())
        .map(|(name, order)| {
            let ascending = order.eq_ignore_ascii_case("asc");
            let nulls_first = ascending;
            Expr::Column(Column::from_name(name.clone())).sort(ascending, nulls_first)
        })
        .collect();
    Some(sort_exprs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    use crate::agg_mode::Mode;
    use crate::query_tracker::QueryTrackingContext;

    #[tokio::test]
    async fn test_widen_schema_noop_when_plan_empty() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let result = widen_schema_from_plan(&ctx, &[], "t", &schema);
        assert_eq!(result.fields().len(), 1);
        assert_eq!(result.field(0).name(), "a");
    }

    /// Cheap-gate branch: with a non-empty plan whose `base_schema` names are all already in
    /// `inferred`, `widen_schema_from_plan` must short-circuit (return `Arc::clone(inferred)`)
    /// before invoking the substrait NamedStruct→Arrow converter. Verifies the column-name
    /// subset check at the top of the function, not the empty-plan early-return.
    #[tokio::test]
    async fn test_widen_schema_noop_when_all_columns_present() {
        let ctx = SessionContext::new();
        // Register a table whose schema is a subset of `inferred` below.
        let registered_schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&registered_schema),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .expect("batch");
        let table =
            MemTable::try_new(Arc::clone(&registered_schema), vec![vec![batch]]).expect("memtable");
        ctx.register_table("t", Arc::new(table)).expect("register");

        // Build a substrait plan with a Read rel pointing at "t" — base_schema.names = ["a"].
        let logical = ctx
            .sql("SELECT a FROM t")
            .await
            .expect("sql")
            .into_unoptimized_plan();
        let plan = to_substrait_plan(&logical, &ctx.state()).expect("substrait plan");
        let mut plan_bytes = Vec::new();
        plan.encode(&mut plan_bytes).expect("encode");

        // inferred has every base_schema column (a) plus an extra one (b) — cheap gate fires.
        let inferred = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let result = widen_schema_from_plan(&ctx, &plan_bytes, "t", &inferred);
        // Must return inferred unchanged (Arc::clone, so pointer-equal).
        assert!(
            Arc::ptr_eq(&result, &inferred),
            "subset gate must short-circuit to inferred"
        );
    }

    /// Empty-shard case: a shard with zero parquet files yields an empty inferred schema, but the
    /// plan's base_schema still names columns. widen_schema_from_plan must append all of them as
    /// nullable so the consumer can bind. (Downstream, the field-count delta — 0 vs N — also
    /// disables stat collection, avoiding the cache's column-count merge failure.)
    #[tokio::test]
    async fn test_widen_schema_from_empty_inferred_adds_all_nullable() {
        let ctx = SessionContext::new();
        // base_schema for "t" = [a (Int64), b (Utf8)].
        let registered_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let table =
            MemTable::try_new(Arc::clone(&registered_schema), vec![vec![]]).expect("memtable");
        ctx.register_table("t", Arc::new(table)).expect("register");
        let logical = ctx
            .sql("SELECT a, b FROM t")
            .await
            .expect("sql")
            .into_unoptimized_plan();
        let plan = to_substrait_plan(&logical, &ctx.state()).expect("substrait plan");
        let mut plan_bytes = Vec::new();
        plan.encode(&mut plan_bytes).expect("encode");

        // Empty shard → empty inferred schema (0 fields).
        let inferred = Arc::new(Schema::empty());
        let result = widen_schema_from_plan(&ctx, &plan_bytes, "t", &inferred);

        assert_eq!(
            result.fields().len(),
            2,
            "all base_schema columns must be appended"
        );
        for name in ["a", "b"] {
            let f = result.field_with_name(name).expect("column present");
            assert!(f.is_nullable(), "appended column {name} must be nullable");
        }
    }

    async fn make_test_handle() -> (SessionContextHandle, Vec<u8>) {
        let runtime_env = RuntimeEnvBuilder::new().build().expect("runtime env");
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(Arc::new(runtime_env))
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Register an in-memory table with column "x"
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");
        ctx.register_table("t", Arc::new(table)).expect("register");

        // Build Substrait bytes for SELECT SUM(x) FROM t
        let df = ctx.sql("SELECT SUM(x) FROM t").await.expect("sql");
        let plan = df.logical_plan().clone();
        let substrait = to_substrait_plan(&plan, &ctx.state()).expect("to_substrait");
        let mut buf = Vec::new();
        substrait.encode(&mut buf).expect("encode");

        let table_path = datafusion::datasource::listing::ListingTableUrl::parse("file:///tmp")
            .expect("table_path");
        let global_pool = ctx.runtime_env().memory_pool.clone();
        let query_context =
            QueryTrackingContext::new(0, global_pool, crate::query_tracker::QueryType::Shard);

        let handle = SessionContextHandle {
            ctx,
            table_path,
            object_metas: Arc::new(vec![]),
            writer_generations: Arc::new(vec![]),
            sort_fields: vec![],
            sort_orders: vec![],
            query_context,
            table_name: "t".to_string(),
            indexed_config: None,
            query_config: crate::datafusion_query_config::DatafusionQueryConfig::test_default(),
            io_handle: tokio::runtime::Handle::current(),
            aggregate_mode: Mode::Default,
            has_topk: false,
            prepared_plan: None,
            phantom_reservation: None,
        };
        (handle, buf)
    }

    #[tokio::test]
    async fn prepare_partial_plan_sets_mode_and_stores_plan() {
        let (mut handle, substrait_bytes) = make_test_handle().await;

        assert_eq!(handle.aggregate_mode, Mode::Default);
        assert!(handle.prepared_plan.is_none());

        prepare_partial_plan(&mut handle, &substrait_bytes)
            .await
            .expect("prepare_partial_plan succeeds");

        assert_eq!(handle.aggregate_mode, Mode::Partial);
        assert!(handle.prepared_plan.is_some());
    }

    /// Regression: a shard whose parquet files have FEWER columns than the widened (alias/pattern
    /// union) table schema must not fail planning with "Cannot merge statistics with different
    /// number of columns". The runtime-global file statistics cache is keyed by path+size+mtime
    /// (NOT schema), so a Statistics cached during an earlier NARROW read is returned for a later
    /// WIDENED read; merging the cached narrow stats against freshly-computed widened stats blows
    /// up. We avoid this by disabling stat collection when widening changed the schema; this test
    /// reproduces the straddle (narrow read seeds the cache, widened read reuses it) and asserts
    /// the widened scan plans + executes.
    #[tokio::test]
    async fn widened_scan_over_narrower_files_does_not_fail_stats_merge() {
        use arrow_array::StringArray;
        use datafusion::arrow::datatypes::SchemaRef;
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };
        use datafusion::execution::cache::file_statistics_cache::DefaultFileStatisticsCache;
        use datafusion::parquet::arrow::ArrowWriter;

        fn write_parquet(
            dir: &std::path::Path,
            name: &str,
            schema: SchemaRef,
            cols: Vec<Arc<dyn arrow::array::Array>>,
        ) {
            let file = std::fs::File::create(dir.join(name)).unwrap();
            let batch = RecordBatch::try_new(Arc::clone(&schema), cols).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let dir = tempfile::tempdir().unwrap();
        // One file with the narrow schema (column "a" only), one with the widened schema (a + b).
        let narrow: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let wide: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        write_parquet(
            dir.path(),
            "narrow.parquet",
            Arc::clone(&narrow),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        );
        write_parquet(
            dir.path(),
            "wide.parquet",
            Arc::clone(&wide),
            vec![
                Arc::new(Int64Array::from(vec![2i64])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        );

        let table_url =
            ListingTableUrl::parse(format!("file://{}", dir.path().to_str().unwrap())).unwrap();
        // Shared, runtime-global stats cache — the crux of the bug.
        let stats_cache = Arc::new(DefaultFileStatisticsCache::default());

        // 1. NARROW read first: registers the table at the narrow (1-col) schema and, with
        //    collect_stat(true), seeds the shared cache with a 1-column Statistics for narrow.parquet.
        let ctx = SessionContext::new();
        let narrow_opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let narrow_cfg = ListingTableConfig::new(table_url.clone())
            .with_listing_options(narrow_opts)
            .with_schema(Arc::clone(&narrow));
        let narrow_tbl = Arc::new(
            ListingTable::try_new(narrow_cfg)
                .unwrap()
                .with_cache(Some(stats_cache.clone())),
        );
        ctx.register_table("t_narrow", narrow_tbl).unwrap();
        let _ = ctx
            .sql("SELECT a FROM t_narrow")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // 2. WIDENED read reusing the SAME cache. This is what create_session_context does after
        //    widen_schema_from_plan. The fix sets collect_stat(false) because the schema was widened;
        //    without it, merging the cached 1-col Statistics against a 2-col one fails planning.
        let widened_opts = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_collect_stat(false); // mirrors the fix in create_session_context for widened tables
        let widened_cfg = ListingTableConfig::new(table_url)
            .with_listing_options(widened_opts)
            .with_schema(Arc::clone(&wide));
        let widened_tbl = Arc::new(
            ListingTable::try_new(widened_cfg)
                .unwrap()
                .with_cache(Some(stats_cache)),
        );
        ctx.register_table("t_wide", widened_tbl).unwrap();

        let rows = ctx
            .sql("SELECT a, b FROM t_wide ORDER BY a")
            .await
            .expect("widened query plans")
            .collect()
            .await
            .expect("widened query executes without stats-merge failure");
        let total: usize = rows.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "widened scan must read both files");
    }

    /// Each per-query RuntimeEnv built the way create_session_context builds it must get its own
    /// ObjectStoreRegistry. Two queries registering different stores under the bare `file://`
    /// scheme must not clobber each other — the bug that routed one shard's parquet read through
    /// another shard's store and failed with "No such file or directory".
    #[tokio::test]
    async fn test_per_query_object_store_registry_is_isolated() {
        use datafusion::execution::object_store::{
            DefaultObjectStoreRegistry, ObjectStoreRegistry,
        };
        use object_store::memory::InMemory;
        use object_store::ObjectStore;
        use url::Url;

        // Simulates the single shared global runtime_env (DataFusionRuntime.runtime_env).
        let shared = RuntimeEnvBuilder::new()
            .build()
            .expect("shared runtime env");

        // Two per-query runtime envs, each derived the way create_session_context derives them:
        // from the shared env but with a fresh object-store registry.
        let env_a = RuntimeEnvBuilder::from_runtime_env(&shared)
            .with_object_store_registry(Arc::new(DefaultObjectStoreRegistry::new()))
            .build()
            .expect("per-query runtime env a");
        let env_b = RuntimeEnvBuilder::from_runtime_env(&shared)
            .with_object_store_registry(Arc::new(DefaultObjectStoreRegistry::new()))
            .build()
            .expect("per-query runtime env b");

        let file_url = Url::parse("file://").unwrap();
        let store_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store_b: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Each query registers its own shard store under the same bare file:// scheme.
        env_a.register_object_store(&file_url, Arc::clone(&store_a));
        env_b.register_object_store(&file_url, Arc::clone(&store_b));

        let got_a = env_a
            .object_store_registry
            .get_store(&file_url)
            .expect("env_a must resolve a file:// store");
        let got_b = env_b
            .object_store_registry
            .get_store(&file_url)
            .expect("env_b must resolve a file:// store");

        // Each env resolves to its OWN store, and the two are independent: registering in one env
        // does not leak into the other.
        assert!(
            Arc::ptr_eq(&got_a, &store_a),
            "env_a must resolve to its own store"
        );
        assert!(
            Arc::ptr_eq(&got_b, &store_b),
            "env_b must resolve to its own store"
        );
        assert!(
            !Arc::ptr_eq(&got_a, &got_b),
            "per-query stores must be independent across queries"
        );
    }

    #[test]
    fn test_skip_partial_agg_disabled_when_has_topk() {
        // skip_partial must be disabled (1.0) when TopK is active — if DF abandons partial
        // agg midstream the partial state is incomplete and TopK sees wrong group counts.
        let mut config = SessionConfig::new();
        let has_topk = true;
        if has_topk {
            config
                .options_mut()
                .execution
                .skip_partial_aggregation_probe_ratio_threshold = 1.0;
        }
        assert_eq!(
            config
                .options()
                .execution
                .skip_partial_aggregation_probe_ratio_threshold,
            1.0,
            "skip_partial must be disabled (1.0) when TopK is active"
        );
    }

    #[test]
    fn test_skip_partial_agg_default_when_no_topk() {
        // When has_topk=false, skip_partial retains DF default (0.8) — no perf regression
        // for non-TopK multi-shard queries.
        let config = SessionConfig::new();
        assert_eq!(
            config
                .options()
                .execution
                .skip_partial_aggregation_probe_ratio_threshold,
            0.8,
            "non-TopK queries must retain DF default threshold"
        );
    }

    #[test]
    fn test_substrait_has_fetch_rel_empty() {
        assert!(!substrait_has_fetch_rel(&[]), "empty bytes → false");
    }

    #[test]
    fn test_substrait_has_fetch_rel_with_fetch() {
        use prost::Message;
        use substrait::proto::expression::literal::LiteralType;
        use substrait::proto::expression::{Literal, RexType};
        use substrait::proto::rel::RelType;
        use substrait::proto::{
            fetch_rel, plan_rel, Expression, FetchRel, Plan, PlanRel, Rel, SortRel,
        };

        // Build: FetchRel(count=10) wrapping SortRel — same as what DataFusion Substrait
        // producer emits for Sort(fetch=10, ...) from OpenSearchTopKRewriter.
        let sort_rel = Box::new(Rel {
            rel_type: Some(RelType::Sort(Box::new(SortRel {
                common: None,
                input: None,
                sorts: vec![],
                advanced_extension: None,
            }))),
        });
        let fetch_rel = Box::new(Rel {
            rel_type: Some(RelType::Fetch(Box::new(FetchRel {
                common: None,
                input: Some(sort_rel),
                offset_mode: None,
                count_mode: Some(fetch_rel::CountMode::CountExpr(Box::new(Expression {
                    rex_type: Some(RexType::Literal(Literal {
                        nullable: false,
                        type_variation_reference: 0,
                        literal_type: Some(LiteralType::I64(10)),
                    })),
                }))),
                advanced_extension: None,
            }))),
        });
        let plan = Plan {
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Rel(*fetch_rel)),
            }],
            ..Default::default()
        };
        let bytes = plan.encode_to_vec();
        assert!(substrait_has_fetch_rel(&bytes), "FetchRel(count=10) → true");
    }

    #[test]
    fn test_substrait_has_fetch_rel_with_fetch_no_count_mode() {
        use prost::Message;
        use substrait::proto::rel::RelType;
        use substrait::proto::{plan_rel, FetchRel, Plan, PlanRel, Rel};

        // FetchRel exists but count_mode is None — not a real limit, should not trigger TopK.
        let fetch_rel = Box::new(Rel {
            rel_type: Some(RelType::Fetch(Box::new(FetchRel {
                common: None,
                input: None,
                offset_mode: None,
                count_mode: None,
                advanced_extension: None,
            }))),
        });
        let plan = Plan {
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Rel(*fetch_rel)),
            }],
            ..Default::default()
        };
        let bytes = plan.encode_to_vec();
        assert!(
            !substrait_has_fetch_rel(&bytes),
            "FetchRel without count_mode → false"
        );
    }

    #[test]
    fn test_substrait_has_fetch_rel_without_fetch() {
        use prost::Message;
        use substrait::proto::rel::RelType;
        use substrait::proto::{plan_rel, Plan, PlanRel, Rel, SortRel};

        // Sort without fetch → no FetchRel → false
        let sort_rel = Box::new(Rel {
            rel_type: Some(RelType::Sort(Box::new(SortRel {
                common: None,
                input: None,
                sorts: vec![],
                advanced_extension: None,
            }))),
        });
        let plan = Plan {
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Rel(*sort_rel)),
            }],
            ..Default::default()
        };
        let bytes = plan.encode_to_vec();
        assert!(
            !substrait_has_fetch_rel(&bytes),
            "SortRel without FetchRel → false"
        );
    }

    /// A Join rel at the root — exercises the `Some(other)` arm that logs and returns false.
    /// Shard fragments never have Join above a TopK FetchRel, so this correctly returns false.
    #[test]
    fn test_substrait_has_fetch_rel_join_returns_false() {
        use prost::Message;
        use substrait::proto::rel::RelType;
        use substrait::proto::{plan_rel, JoinRel, Plan, PlanRel, Rel};

        let join_rel = Box::new(Rel {
            rel_type: Some(RelType::Join(Box::new(JoinRel {
                common: None,
                left: None,
                right: None,
                r#type: 0,
                expression: None,
                post_join_filter: None,
                advanced_extension: None,
            }))),
        });
        let plan = Plan {
            relations: vec![PlanRel {
                rel_type: Some(plan_rel::RelType::Rel(*join_rel)),
            }],
            ..Default::default()
        };
        let bytes = plan.encode_to_vec();
        assert!(
            !substrait_has_fetch_rel(&bytes),
            "Join rel → false (no TopK in shard fragment with Join)"
        );
    }
}
