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
    physical_plan::execute_stream,
    prelude::*,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use jni::sys::jlong;
use log::error;
use object_store::ObjectMeta;
use prost::Message;
use substrait::proto::Plan;

use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::DataFusionRuntime;

/// Execute a vanilla parquet query: substrait plan → DataFusion → CrossRtStream.
/// File access goes through DataFusion's registered object store.
pub async fn execute_query(
    table_path: ListingTableUrl,
    object_metas: Arc<Vec<ObjectMeta>>,
    table_name: String,
    plan_bytes: Vec<u8>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<jlong, DataFusionError> {
    // Pre-populate the list-files cache so DataFusion doesn't re-list the directory
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    let table_scoped_path = datafusion::execution::cache::TableScopedPath {
        table: None,
        path: table_path.prefix().clone(),
    };
    list_file_cache.put(&table_scoped_path, object_metas);

    // Build a per-query RuntimeEnv sharing the global memory pool + caches,
    // but with a fresh list-files cache for this query's shard files.
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
        .build()
        .map_err(|e| {
            error!("Failed to build runtime env: {}", e);
            e
        })?;

    // Clone the pre-built session state template (cheap Arc increments for all
    // registered functions, optimizer rules, and planner rules) and swap in the
    // per-query RuntimeEnv.
    let state = runtime.session_state_template.clone()
        .with_runtime_env(Arc::from(runtime_env));

    let ctx = SessionContext::new_with_state(state);

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
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Ok(Box::into_raw(Box::new(wrapped)) as jlong)
}
