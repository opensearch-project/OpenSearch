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
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use log::error;
use object_store::ObjectMeta;
use prost::Message;
use substrait::proto::Plan;

use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::api::DataFusionRuntime;
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
    phantom_corrector: Option<Arc<crate::phantom_corrector::PhantomCorrector>>,
) -> Result<i64, DataFusionError> {
    // Pre-populate the list-files cache so DataFusion doesn't re-list the directory
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    let table_scoped_path = datafusion::execution::cache::TableScopedPath {
        table: None,
        path: table_path.prefix().clone(),
    };
    list_file_cache.put(&table_scoped_path, object_metas);

    // Build a per-query RuntimeEnv sharing the global memory pool + caches,
    // but with a fresh list-files cache for this query's shard files.
    let mut runtime_env_builder = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_files_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        );

    // If a per-query memory pool is provided, set it on the same builder.
    // The per-query pool wraps the global pool, so global limits are still enforced.
    if let Some(pool) = query_memory_pool {
        runtime_env_builder = runtime_env_builder.with_memory_pool(pool);
    }

    let runtime_env = runtime_env_builder
        .build()
        .map_err(|e| {
            error!("Failed to build runtime env: {}", e);
            e
        })?;

    // Build a fresh session state per query. TODO : Tune this during planning per query
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = query_config.parquet_pushdown_filters;
    config.options_mut().execution.target_partitions = query_config.target_partitions;
    config.options_mut().execution.batch_size = query_config.batch_size;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);
    crate::udf::register_all(&ctx);

    // Register table via ListingTable — all IO goes through object store
    let file_format = ParquetFormat::new();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await
        .map_err(|e| {
            error!("Failed to infer schema: {}", e);
            e
        })?;

    let table_config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let provider = Arc::new(ListingTable::try_new(table_config).map_err(|e| {
        error!("Failed to create listing table: {}", e);
        e
    })?);

    ctx.register_table(&table_name, provider).map_err(|e| {
        error!("Failed to register table: {}", e);
        e
    })?;

    // Decode substrait → logical plan → physical plan → stream
    let substrait_plan = Plan::decode(plan_bytes.as_slice()).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
    })?;

    let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;

    let df_stream = execute_stream(physical_plan, ctx.task_ctx()).map_err(|e| {
        error!("Failed to create execution stream: {}", e);
        e
    })?;

    // Wrap in CrossRtStream — CPU work runs on DedicatedExecutor
    let cross_rt_stream =
        CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
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
pub async fn execute_with_context(
    handle: SessionContextHandle,
    plan_bytes: &[u8],
    cpu_executor: DedicatedExecutor,
) -> Result<i64, DataFusionError> {
    let substrait_plan = Plan::decode(plan_bytes).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
    })?;

    let logical_plan = from_substrait_plan(&handle.ctx.state(), &substrait_plan).await?;
    let dataframe = handle.ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;

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
