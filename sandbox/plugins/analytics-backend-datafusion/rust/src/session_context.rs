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
    execution::cache::cache_manager::CacheManagerConfig,
    execution::cache::{CacheAccessor, DefaultListFilesCache},
    execution::context::SessionContext,
    execution::memory_pool::MemoryPool,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::SessionStateBuilder,
    prelude::*,
};
use log::error;
use object_store::ObjectMeta;

use crate::api::{DataFusionRuntime, ShardView};
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::query_tracker::QueryTrackingContext;

/// Opaque handle holding a configured SessionContext between FFM calls.
pub struct SessionContextHandle {
    pub ctx: SessionContext,
    pub table_path: ListingTableUrl,
    pub object_metas: Arc<Vec<ObjectMeta>>,
    pub query_context: QueryTrackingContext,
    pub table_name: String,
    /// When set, indicates this session uses the indexed execution path with filter delegation.
    pub indexed_config: Option<IndexedExecutionConfig>,
    /// Per-query tuning knobs (batch size, partitions, filter strategies, etc.)
    pub query_config: DatafusionQueryConfig,
}

/// Configuration for indexed execution with filter delegation, provided by Java.
pub struct IndexedExecutionConfig {
    pub tree_shape: i32,
    pub delegated_predicate_count: i32,
}

/// Creates a SessionContext with per-query RuntimeEnv and registers the default
/// ListingTable provider for parquet scans.
pub async unsafe fn create_session_context(
    runtime_ptr: i64,
    shard_view_ptr: i64,
    table_name: &str,
    context_id: i64,
    query_config: DatafusionQueryConfig,
) -> Result<i64, DataFusionError> {
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let shard_view = &*(shard_view_ptr as *const ShardView);

    let global_pool = runtime.runtime_env.memory_pool.clone();
    let query_context = QueryTrackingContext::new(context_id, global_pool);
    let query_memory_pool = query_context
        .memory_pool()
        .map(|p| p as Arc<dyn MemoryPool>);

    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    list_file_cache.put(
        &datafusion::execution::cache::TableScopedPath {
            table: None,
            path: shard_view.table_path.prefix().clone(),
        },
        shard_view.object_metas.clone(),
    );

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

    if let Some(pool) = query_memory_pool {
        runtime_env_builder = runtime_env_builder.with_memory_pool(pool);
    }

    let runtime_env = runtime_env_builder.build().map_err(|e| {
        error!("create_session_context: failed to build runtime env: {}", e);
        e
    })?;

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
    // Register OpenSearch UDFs (mvappend, mvfind, mvzip, convert_tz, …) on this session
    // so the substrait converter at execute_with_context can resolve their function names.
    // Without this, fragment execution fails with "Unsupported function name" because
    // df_execute_with_context reuses this handle's ctx instead of building a fresh one.
    crate::udf::register_all(&ctx);

    // Register default ListingTable for parquet scans
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &shard_view.table_path)
        .await
        .map_err(|e| {
            error!("create_session_context: failed to infer schema: {}", e);
            e
        })?;

    let table_config = ListingTableConfig::new(shard_view.table_path.clone())
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let provider = Arc::new(ListingTable::try_new(table_config).map_err(|e| {
        error!(
            "create_session_context: failed to create listing table: {}",
            e
        );
        e
    })?);

    ctx.register_table(table_name, provider).map_err(|e| {
        error!(
            "create_session_context: failed to register table '{}': {}",
            table_name, e
        );
        e
    })?;

    error!(
        "create_session_context: successfully registered table '{}', table_name_len={}",
        table_name,
        table_name.len()
    );

    let handle = SessionContextHandle {
        ctx,
        table_path: shard_view.table_path.clone(),
        object_metas: shard_view.object_metas.clone(),
        query_context,
        table_name: table_name.to_string(),
        indexed_config: None,
        query_config,
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
    query_config: DatafusionQueryConfig,
) -> Result<i64, DataFusionError> {
    // Create base session context (same as non-indexed path)
    let ptr = create_session_context(runtime_ptr, shard_view_ptr, table_name, context_id, query_config).await?;

    // Augment with indexed config and UDF registration
    let handle = &mut *(ptr as *mut SessionContextHandle);
    handle.ctx.register_udf(crate::indexed_table::substrait_to_tree::create_index_filter_udf());
    handle.indexed_config = Some(IndexedExecutionConfig {
        tree_shape,
        delegated_predicate_count,
    });

    Ok(ptr)
}
