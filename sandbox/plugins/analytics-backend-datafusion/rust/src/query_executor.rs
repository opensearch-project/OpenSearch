/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use native_bridge_common::log_debug;
use datafusion::{
    common::DataFusionError,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    execution::context::SessionContext,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::SessionStateBuilder,
    physical_plan::displayable,
    physical_plan::execute_stream,
    prelude::*,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::execution::cache::cache_manager::{CacheManagerConfig, CachedFileList};
use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use log::error;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use prost::Message;
use substrait::proto::Plan;

use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::api::{DataFusionRuntime, ShardFileInfo};
use crate::session_context::SessionContextHandle;

/// Execute a vanilla parquet query: substrait plan → DataFusion → CrossRtStream.
/// File access goes through DataFusion's registered object store.
///
/// Deprecated: Production now uses the decomposed `create_session_context` +
/// `execute_with_context` path (via `api::execute_query`).
/// TODO: Remove this function and migrate benchmarks to the decomposed path.
/// Retained only for benchmarks. TODO: migrate benchmarks and remove.
pub async fn execute_query(
    table_path: ListingTableUrl,
    object_metas: Arc<Vec<ObjectMeta>>,
    table_name: String,
    plan_bytes: Vec<u8>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    query_memory_pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
    query_config: &crate::datafusion_query_config::DatafusionQueryConfig,
    context_id: i64,
    shard_store: Arc<dyn ObjectStore>,
    phantom_corrector: Option<Arc<crate::phantom_corrector::PhantomCorrector>>,
    sort_fields: &[String],
    sort_orders: &[String],
) -> Result<i64, DataFusionError> {
    // Build per-query RuntimeEnv with list-files cache pre-populated.
    let runtime_env = build_query_runtime_env(runtime, &table_path, object_metas.as_ref())?;

    // If a per-query memory pool is provided, rebuild with it overlaid.
    // The per-query pool wraps the global pool, so global limits are still enforced.
    let runtime_env = if let Some(pool) = query_memory_pool {
        Arc::from(
            RuntimeEnvBuilder::from_runtime_env(&runtime_env)
                .with_memory_pool(pool)
                .build()
                .map_err(|e| {
                    error!("Failed to build runtime env with query pool: {}", e);
                    e
                })?,
        )
    } else {
        runtime_env
    };

    // Register shard-specific object store on file:// scheme for this query.
    // Routes reads through TieredObjectStore (local + remote) or default LocalFileSystem.
    runtime_env.register_object_store(
        &url::Url::parse("file://").unwrap(),
        shard_store,
    );

    // Build a fresh session state per query. TODO : Tune this during planning per query
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = query_config.parquet_pushdown_filters;
    config.options_mut().execution.target_partitions = query_config.target_partitions;
    config.options_mut().execution.batch_size = query_config.batch_size;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);
    crate::udf::register_all(&ctx);
    crate::udaf::register_all(&ctx);

    // Register table provider based on strategy.
    //
    // Note: api::execute_query only routes to this function when the plan does NOT
    // request row IDs (otherwise it dispatches to the indexed executor). The strategy
    // therefore matters only for distinguishing the ShardTableProvider rewrite
    // (ListingTable) from the plain ListingTable scan (None / IndexedPredicateOnly).
    use crate::datafusion_query_config::QueryStrategy;
    match query_config.query_strategy {
        QueryStrategy::ListingTable => {
            use crate::shard_table_provider::{ShardTableConfig, ShardTableProvider};

            // Infer schema from the first file
            let file_format = ParquetFormat::new();
            let listing_options = ListingOptions::new(Arc::new(file_format))
                .with_file_extension(".parquet")
                .with_collect_stat(true);
            let resolved_schema = listing_options
                .infer_schema(&ctx.state(), &table_path)
                .await
                .map_err(|e| { error!("Failed to infer schema: {}", e); e })?;
            let resolved_schema = crate::schema_coerce::coerce_inferred_schema(resolved_schema);

            // Build ShardFileInfo with row_base from cumulative row counts
            let store = ctx.state().runtime_env().object_store(&table_path)?;
            let files = build_shard_file_infos(&store, object_metas.as_ref()).await?;

            let store_url = store_url_from_table_path(&table_path)?;

            let provider = Arc::new(ShardTableProvider::new(ShardTableConfig {
                file_schema: resolved_schema,
                files,
                store_url,
            }));
            ctx.register_table(&table_name, provider)
                .map_err(|e| { error!("Failed to register table: {}", e); e })?;
        }
        _ => {
            // Baseline: use standard ListingTable
            let file_format = ParquetFormat::new();
            let mut listing_options = ListingOptions::new(Arc::new(file_format))
                .with_file_extension(".parquet")
                .with_collect_stat(true);
            // Declare per-file sort order to DataFusion if the index has `index.sort.field`.
            // See `session_context::build_file_sort_order` for what the declaration buys us
            // and the case/nulls/non-sort caveats.
            if let Some(sort_exprs) = crate::session_context::build_file_sort_order(sort_fields, sort_orders) {
                listing_options = listing_options.with_file_sort_order(vec![sort_exprs]);
            }
            let resolved_schema = listing_options
                .infer_schema(&ctx.state(), &table_path)
                .await
                .map_err(|e| { error!("Failed to infer schema: {}", e); e })?;
            let resolved_schema = crate::schema_coerce::coerce_inferred_schema(resolved_schema);
            let table_config = ListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .with_schema(resolved_schema);
            let provider = Arc::new(ListingTable::try_new(table_config)
                .map_err(|e| { error!("Failed to create listing table: {}", e); e })?);
            ctx.register_table(&table_name, provider)
                .map_err(|e| { error!("Failed to register table: {}", e); e })?;
        }
    }

    // Decode substrait → logical plan → physical plan → stream
    let substrait_plan = Plan::decode(plan_bytes.as_slice()).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
    })?;

    let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;

    // Retag any physical-plan output columns whose type tags differ from what Substrait
    // declared on bit-compatible Int↔UInt pairs (see crate::relabel_exec). The target is
    // schema_coerce::coerce_inferred_schema(physical_schema) — the same narrowing the
    // partition-stream registration uses, so the consumer's StreamingTable and the
    // batches arriving from this producer agree by construction.
    let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
    let physical_plan = crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;

    // Apply row ID optimizer when ShardTableProvider injected `row_base`.
    // For other strategies the vanilla scan output is already what the plan expects.
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    let physical_plan = match query_config.query_strategy {
        QueryStrategy::ListingTable => {
            // Rewrites ___row_id to ___row_id + row_base.
            let optimizer = crate::project_row_id_optimizer::ProjectRowIdOptimizer;
            let config = datafusion::common::config::ConfigOptions::default();
            optimizer.optimize(physical_plan, &config)?
        }
        _ => physical_plan,
    };

    let df_stream = execute_stream(physical_plan, ctx.task_ctx()).map_err(|e| {
        error!("Failed to create execution stream: {}", e);
        e
    })?;

    // Wrap in CrossRtStream — CPU work runs on DedicatedExecutor
    let (cross_rt_stream, abort_handle) =
        CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor);

    if let Some(h) = abort_handle {
        crate::query_tracker::set_abort_handle(context_id, h);
    }

    // Attach phantom corrector for self-correcting budget (if provided)
    let cross_rt_stream = match phantom_corrector {
        Some(corrector) => cross_rt_stream.with_phantom_corrector(corrector),
        None => cross_rt_stream,
    };

    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Ok(Box::into_raw(Box::new(wrapped)) as i64)
}

/// Executes a Substrait plan against a pre-configured SessionContext.
///
/// Takes ownership of the handle by value. The ownership transfer (consuming the
/// raw Java pointer) happens at the FFM entry in `df_execute_with_context`, so
/// by the time this function is reached the pointer is already invalidated from
/// Java's perspective and cleanup is pure RAII.
///
/// When the plan requests row IDs and a `QueryStrategy` is configured, this
/// function routes to the appropriate execution path:
/// - `ListingTable`: applies `ProjectRowIdOptimizer` to the physical plan
/// - `IndexedPredicateOnly`: delegates to the indexed executor with `emit_row_ids=true`
pub async fn execute_with_context(
    handle: SessionContextHandle,
    plan_bytes: &[u8],
    cpu_executor: DedicatedExecutor,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> Result<i64, DataFusionError> {
    // Permit was acquired by the caller (ffm.rs) on the IO runtime before
    // spawning on the CPU runtime, so the Java search thread blocks at the
    // gate when it is full — creating backpressure at the Java threadpool level.
    use crate::datafusion_query_config::QueryStrategy;

    let context_id = handle.query_context.context_id();
    let token = crate::query_tracker::get_cancellation_token(context_id);

    let query_strategy = handle.query_config.query_strategy;

    // If ListingTable strategy: replace the default ListingTable with ShardTableProvider
    // that adds row_base partition column for ProjectRowIdOptimizer.
    // Also register the ProjectRowIdAnalyzer to ensure __row_id__ survives logical optimization.
    if query_strategy == QueryStrategy::ListingTable {
        use crate::shard_table_provider::{ShardTableConfig, ShardTableProvider};

        handle.ctx.deregister_table(&handle.table_name)?;

        let store = handle.ctx.state().runtime_env().object_store(&handle.table_path)?;

        // Infer schema from existing files
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let resolved_schema = listing_options
            .infer_schema(&handle.ctx.state(), &handle.table_path)
            .await?;
        let resolved_schema = crate::schema_coerce::coerce_inferred_schema(resolved_schema);

        // Build ShardFileInfo with cumulative row_base from parquet metadata.
        let files = build_shard_file_infos(&store, handle.object_metas.as_ref()).await?;

        let store_url = store_url_from_table_path(&handle.table_path)?;

        let provider = Arc::new(ShardTableProvider::new(ShardTableConfig {
            file_schema: resolved_schema,
            files,
            store_url,
        }));
        handle.ctx.register_table(&handle.table_name, provider)?;
    }

    let query_future = async {
        // If prepare_partial_plan stored a stripped plan on this handle (engine-native-merge
        // PARTIAL stage triggered by SETUP_PARTIAL_AGGREGATE), skip the substrait re-decode
        // and run the prepared plan directly. This activates `force_aggregate_mode(Partial)`
        // semantics — only AggregateExec(Mode::Partial) executes, emitting state-suffixed
        // columns on the wire (e.g. `dc[hll_registers]: Binary` for HLL sketch state). The
        // FINAL substrait declares VARBINARY (resolver's overrideExchangeType), so the wire
        // matches the exchange contract. Non-engine-native paths leave `prepared_plan` as None
        // and fall through to the standard decode + execute below.
        if let Some(prepared) = handle.prepared_plan.as_ref() {
            let physical_plan = std::sync::Arc::clone(prepared);
            let df_stream = execute_stream(physical_plan, handle.ctx.task_ctx()).map_err(|e| {
                error!("execute_with_context: failed to execute prepared plan: {}", e);
                e
            })?;
            let (cross_rt_stream, abort_handle) =
                CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor);
            if let Some(h) = abort_handle {
                crate::query_tracker::set_abort_handle(context_id, h);
            }
            let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                cross_rt_stream.schema(),
                cross_rt_stream,
            );
            // Prepared (engine-native PARTIAL) path carries no physical_plan handle — match the
            // tuple shape of the other exits so the closure's return type stays consistent.
            return Ok::<(i64, Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>), DataFusionError>(
                (Box::into_raw(Box::new(wrapped)) as i64, None),
            );
        }

        let substrait_plan = Plan::decode(plan_bytes).map_err(|e| {
            DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
        })?;

        // Union schema widening was applied at table registration (session_context::widen_to_union_schema).
        let logical_plan = from_substrait_plan(&handle.ctx.state(), &substrait_plan).await?;
        log_debug!("DataFusion logical plan:\n{}", logical_plan.display_indent());

        // Empty shard: skip physical planning (ParquetExec errors on zero files)
        // and emit an EmptyExec stream with the logical plan's output schema.
        if handle.object_metas.is_empty() {
            use datafusion::physical_plan::empty::EmptyExec;
            use datafusion::physical_plan::ExecutionPlan;
            let plan_schema: arrow::datatypes::SchemaRef =
                Arc::new(logical_plan.schema().as_arrow().clone());
            let plan_schema =
                crate::schema_coerce::coerce_inferred_schema(plan_schema);
            let empty_exec = EmptyExec::new(Arc::clone(&plan_schema));
            let df_stream = empty_exec.execute(0, handle.ctx.task_ctx()).map_err(|e| {
                error!("execute_with_context: failed to create empty stream: {}", e);
                e
            })?;

            let (cross_rt_stream, abort_handle) =
                CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor);
            if let Some(h) = abort_handle {
                crate::query_tracker::set_abort_handle(context_id, h);
            }
            let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                cross_rt_stream.schema(),
                cross_rt_stream,
            );
            return Ok::<(i64, Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>), DataFusionError>((Box::into_raw(Box::new(wrapped)) as i64, None));
        }

        let dataframe = handle.ctx.execute_logical_plan(logical_plan).await?;
        // create_physical_plan runs all registered physical optimizer rules including
        // ProjectRowIdOptimizer (registered in session_context when strategy=ListingTable).
        let physical_plan = dataframe.create_physical_plan().await?;

        let target_schema = crate::schema_coerce::coerce_inferred_schema(physical_plan.schema());
        let physical_plan = crate::relabel_exec::wrap_if_relabel_needed(physical_plan, target_schema)?;

        let df_stream = execute_stream(physical_plan.clone(), handle.ctx.task_ctx()).map_err(|e| {
            error!("execute_with_context: failed to create stream: {}", e);
            e
        })?;

        let (cross_rt_stream, abort_handle) =
            CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor);

        if let Some(h) = abort_handle {
            crate::query_tracker::set_abort_handle(context_id, h);
        }

        let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
            cross_rt_stream.schema(),
            cross_rt_stream,
        );

        Ok::<(i64, Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>), DataFusionError>((Box::into_raw(Box::new(wrapped)) as i64, Some(physical_plan)))
    };

    let (stream_ptr, physical_plan) = crate::cancellation::cancellable(token.as_ref(), context_id, query_future)
        .await
        .map_err(|e| DataFusionError::Execution(e))?;

    // Reconstruct the stream from the raw pointer
    let stream = unsafe { *Box::from_raw(stream_ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<CrossRtStream>) };
    // Permit is held until the QueryStreamHandle is dropped (query complete).
    // If cancellation fires → stream drops → handle drops → permit drops → gate releases.
    let stream_handle = match physical_plan {
        Some(plan) => crate::api::QueryStreamHandle::with_physical_plan(stream, handle.query_context, handle.ctx, Some(permit), plan),
        None => crate::api::QueryStreamHandle::with_session_context(stream, handle.query_context, handle.ctx, Some(permit)),
    };
    Ok(Box::into_raw(Box::new(stream_handle)) as i64)
}

// ── Shared helpers ──────────────────────────────────────────────────────────

/// Build a per-query RuntimeEnv sharing global caches, with a fresh list-files
/// cache pre-populated for the given table path and object metas.
pub fn build_query_runtime_env(
    runtime: &DataFusionRuntime,
    table_path: &ListingTableUrl,
    object_metas: &[ObjectMeta],
) -> Result<Arc<datafusion::execution::runtime_env::RuntimeEnv>, DataFusionError> {
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    let table_scoped_path = datafusion::execution::cache::TableScopedPath {
        table: None,
        path: table_path.prefix().clone(),
    };
    list_file_cache.put(&table_scoped_path, CachedFileList::new(object_metas.to_vec()));

    let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_file_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        )
        .build()?;
    Ok(Arc::from(runtime_env))
}

/// Build ShardFileInfo list from object metas by reading parquet footers.
/// Each file gets a cumulative `row_base` and per-RG row counts.
pub async fn build_shard_file_infos(
    store: &Arc<dyn object_store::ObjectStore>,
    object_metas: &[ObjectMeta],
) -> Result<Vec<ShardFileInfo>, DataFusionError> {
    let mut files: Vec<ShardFileInfo> = Vec::new();
    let mut cumulative_rows: i64 = 0;
    for meta in object_metas {
        let reader = datafusion::parquet::arrow::async_reader::ParquetObjectReader::new(
            Arc::clone(store), meta.location.clone(),
        ).with_file_size(meta.size);
        let builder = datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| DataFusionError::Execution(format!("parquet metadata: {}", e)))?;
        let pq_meta = builder.metadata().clone();
        let num_rows: i64 = (0..pq_meta.num_row_groups())
            .map(|i| pq_meta.row_group(i).num_rows())
            .sum();

        files.push(ShardFileInfo {
            object_meta: meta.clone(),
            row_base: cumulative_rows,
            num_rows: num_rows as u64,
            row_group_row_counts: (0..pq_meta.num_row_groups())
                .map(|i| pq_meta.row_group(i).num_rows() as u64)
                .collect(),
            access_plan: None,
        });
        cumulative_rows += num_rows;
    }
    Ok(files)
}

/// Parse a ListingTableUrl into an ObjectStoreUrl (scheme + authority).
pub fn store_url_from_table_path(table_path: &ListingTableUrl) -> Result<datafusion::execution::object_store::ObjectStoreUrl, DataFusionError> {
    let url_str = table_path.as_str();
    let parsed = url::Url::parse(url_str)
        .map_err(|e| DataFusionError::Execution(format!("parse URL: {}", e)))?;
    datafusion::execution::object_store::ObjectStoreUrl::parse(
        format!("{}://{}", parsed.scheme(), parsed.authority()),
    )
}

/// Wrap a DataFusion stream in CrossRtStream and package as a QueryStreamHandle pointer.
pub fn wrap_stream_as_handle(
    df_stream: datafusion::execution::SendableRecordBatchStream,
    cpu_executor: DedicatedExecutor,
    runtime: &DataFusionRuntime,
    context_id: i64,
) -> i64 {
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );
    let query_context = crate::query_tracker::QueryTrackingContext::new(
        context_id,
        runtime.runtime_env.memory_pool.clone(),
        crate::query_tracker::QueryType::Shard,
    );
    let handle = crate::api::QueryStreamHandle::new(wrapped, query_context, None);
    Box::into_raw(Box::new(handle)) as i64
}
