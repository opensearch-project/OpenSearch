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
    execution::cache::cache_manager::{CacheManagerConfig, CachedFileList},
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
    /// Aggregate execution mode for distributed partial/final stripping.
    pub(crate) aggregate_mode: crate::agg_mode::Mode,
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
    let query_context = QueryTrackingContext::new(context_id, global_pool.clone());
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

    // Register shard-specific object store on file:// scheme for this query.
    // This is the instruction-based execution path (ShardScanInstructionHandler).
    // Without this, queries use default LocalFileSystem and fail on warm.
    runtime_env.register_object_store(
        &url::Url::parse("file://").unwrap(),
        Arc::clone(&shard_view.store),
    );

    // Acquire memory budget from cached parquet metadata (zero I/O).
    // On cache miss (first query for this shard), skip — subsequent queries benefit.
    let phantom_reservation = try_acquire_budget(
        runtime, &global_pool, &shard_view, &query_config,
    );
    let effective_partitions = phantom_reservation.as_ref()
        .map(|b| b.target_partitions)
        .unwrap_or(query_config.target_partitions);
    let effective_batch_size = phantom_reservation.as_ref()
        .map(|b| b.batch_size)
        .unwrap_or(query_config.batch_size);
    let phantom = phantom_reservation.map(|b| b.phantom_reservation);

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = query_config.parquet_pushdown_filters;
    config.options_mut().execution.target_partitions = effective_partitions;
    config.options_mut().execution.batch_size = effective_batch_size;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .with_physical_optimizer_rules(crate::agg_mode::physical_optimizer_rules_without_combine())
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
    // Substrait's type system is narrower than Arrow's; normalize the inferred
    // schema to forms the Substrait consumer can bind against. See crate::schema_coerce.
    let resolved_schema = crate::schema_coerce::coerce_inferred_schema(resolved_schema);

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
        aggregate_mode: crate::agg_mode::Mode::Default,
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
    let stripped = crate::agg_mode::apply_aggregate_mode(physical_plan, crate::agg_mode::Mode::Partial)?;
    handle.prepared_plan = Some(stripped);
    Ok(())
}

/// Attempt to acquire a memory budget using cached parquet metadata.
/// Returns None on cache miss or if the budget system is not configured.
fn try_acquire_budget(
    runtime: &DataFusionRuntime,
    pool: &Arc<dyn MemoryPool>,
    shard_view: &ShardView,
    config: &DatafusionQueryConfig,
) -> Option<crate::query_budget::QueryMemoryBudget> {
    use datafusion::execution::cache::CacheAccessor;
    use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
    use parquet::arrow::parquet_to_arrow_schema;

    let first_meta = shard_view.object_metas.first()?;
    let cache = runtime.runtime_env.cache_manager.get_file_metadata_cache();
    let cached = cache.get(&first_meta.location)?;
    let cached_parquet = cached.file_metadata.as_any().downcast_ref::<CachedParquetMetaData>()?;
    let parquet_meta = cached_parquet.parquet_metadata();

    let schema = parquet_to_arrow_schema(
        parquet_meta.file_metadata().schema_descr(),
        parquet_meta.file_metadata().key_value_metadata(),
    ).ok().map(Arc::new)?;

    crate::query_budget::acquire_budget_from_metadata(
        pool,
        &schema,
        parquet_meta,
        config.target_partitions,
        config.batch_size,
    ).ok()
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

    async fn make_test_handle() -> (SessionContextHandle, Vec<u8>) {
        let runtime_env = RuntimeEnvBuilder::new().build().expect("runtime env");
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(Arc::new(runtime_env))
            .with_default_features()
            .with_physical_optimizer_rules(crate::agg_mode::physical_optimizer_rules_without_combine())
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
        let query_context = QueryTrackingContext::new(0, global_pool);

        let handle = SessionContextHandle {
            ctx,
            table_path,
            object_metas: Arc::new(vec![]),
            query_context,
            table_name: "t".to_string(),
            indexed_config: None,
            query_config: crate::datafusion_query_config::DatafusionQueryConfig::test_default(),
            aggregate_mode: Mode::Default,
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
}
