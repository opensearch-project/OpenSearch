/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use datafusion::{
    common::DataFusionError,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    execution::context::SessionContext,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::SessionStateBuilder,
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
    // Per-query memory pool, or None when context_id is 0 (tracking disabled).
    // Not all query flows pass a context_id yet; this fallback allows queries
    // to execute using the global pool. Can be made required once all flows
    // wire up context_id correctly.
    query_memory_pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
    query_config: &crate::datafusion_query_config::DatafusionQueryConfig,
    shard_store: Arc<dyn ObjectStore>,
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

    // Register table provider based on strategy
    use crate::datafusion_query_config::FetchStrategy;
    match query_config.fetch_strategy {
        FetchStrategy::IndexedPredicateOnly => {
            return Err(DataFusionError::Execution(
                "IndexedPredicateOnly strategy requires the indexed executor path \
                 (execute_indexed_with_context). It cannot be used via execute_query \
                 because it needs segment metadata and PositionMap for position-based \
                 row ID computation.".into(),
            ));
        }
        FetchStrategy::ListingTable => {
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
            // Baseline / IndexedPredicateOnly: use standard ListingTable
            let file_format = ParquetFormat::new();
            let listing_options = ListingOptions::new(Arc::new(file_format))
                .with_file_extension(".parquet")
                .with_collect_stat(true);
            let resolved_schema = listing_options
                .infer_schema(&ctx.state(), &table_path)
                .await
                .map_err(|e| { error!("Failed to infer schema: {}", e); e })?;
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

    // Apply row ID optimizer based on strategy if configured
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    let physical_plan = match query_config.fetch_strategy {
        FetchStrategy::None => {
            // Baseline: no optimizer, ___row_id read as regular column (local IDs)
            physical_plan
        }
        FetchStrategy::ListingTable => {
            // Apply ProjectRowIdOptimizer: rewrites ___row_id to ___row_id + row_base
            let optimizer = crate::project_row_id_optimizer::ProjectRowIdOptimizer;
            let config = datafusion::common::config::ConfigOptions::default();
            optimizer.optimize(physical_plan, &config)?
        }
        FetchStrategy::IndexedPredicateOnly => {
            // Unreachable — we return an error above before reaching here
            unreachable!()
        }
    };

    let df_stream = execute_stream(physical_plan, ctx.task_ctx()).map_err(|e| {
        error!("Failed to create execution stream: {}", e);
        e
    })?;

    // Wrap in CrossRtStream — CPU work runs on DedicatedExecutor
    let cross_rt_stream =
        CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
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
/// When the plan requests row IDs and a `FetchStrategy` is configured, this
/// function routes to the appropriate execution path:
/// - `ListingTable`: applies `ProjectRowIdOptimizer` to the physical plan
/// - `IndexedPredicateOnly`: delegates to the indexed executor with `emit_row_ids=true`
pub async fn execute_with_context(
    handle: SessionContextHandle,
    plan_bytes: &[u8],
    cpu_executor: DedicatedExecutor,
) -> Result<i64, DataFusionError> {
    use crate::datafusion_query_config::FetchStrategy;

    let fetch_strategy = handle.query_config.fetch_strategy;

    // If ListingTable strategy: replace the default ListingTable with ShardTableProvider
    // that adds row_base partition column for ProjectRowIdOptimizer.
    // Also register the ProjectRowIdAnalyzer to ensure __row_id__ survives logical optimization.
    if fetch_strategy == FetchStrategy::ListingTable {
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

    let substrait_plan = Plan::decode(plan_bytes).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
    })?;

    let logical_plan = from_substrait_plan(&handle.ctx.state(), &substrait_plan).await?;

    let requests_row_ids = crate::indexed_table::substrait_to_tree::plan_requests_row_ids(&logical_plan);

    let dataframe = handle.ctx.execute_logical_plan(logical_plan).await?;
    // create_physical_plan runs all registered physical optimizer rules including
    // ProjectRowIdOptimizer (registered in session_context when strategy=ListingTable).
    let physical_plan = dataframe.create_physical_plan().await?;

    // No post-hoc optimizer needed — ProjectRowIdOptimizer runs inside create_physical_plan.
    let physical_plan = if requests_row_ids {
        match fetch_strategy {
            FetchStrategy::None => physical_plan,
            FetchStrategy::ListingTable => physical_plan,
            FetchStrategy::IndexedPredicateOnly => physical_plan,
        }
    } else {
        physical_plan
    };

    let df_stream = execute_stream(physical_plan, handle.ctx.task_ctx()).map_err(|e| {
        error!("execute_with_context: failed to create stream: {}", e);
        e
    })?;

    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    let stream_handle = crate::api::QueryStreamHandle::with_session_context(wrapped, handle.query_context, handle.ctx);
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
                .with_files_statistics_cache(
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
) -> i64 {
    let cross_rt_stream = CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );
    let query_context = crate::query_tracker::QueryTrackingContext::new(
        0,
        runtime.runtime_env.memory_pool.clone(),
    );
    let handle = crate::api::QueryStreamHandle::new(wrapped, query_context);
    Box::into_raw(Box::new(handle)) as i64
}
