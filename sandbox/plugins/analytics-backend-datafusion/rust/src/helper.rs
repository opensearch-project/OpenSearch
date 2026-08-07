/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Small shared helpers for the query execution paths.

use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use log::error;
use object_store::{ObjectMeta, ObjectStore};

use crate::agg_mode::physical_optimizer_rules_without_combine;
use crate::api::DataFusionRuntime;
use crate::datafusion_query_config::DatafusionQueryConfig;
use crate::indexed_table::substrait_to_tree::{
    create_delegation_possible_udf, create_index_filter_udf,
};
use crate::query_executor::build_query_runtime_env;
use crate::query_tracker::{QueryTrackingContext, QueryType};
use crate::schema_coerce::coerce_inferred_schema;
use crate::session_context::build_file_sort_order;
use crate::udaf;
use crate::udf;

/// Creates a per-query [`QueryTrackingContext`] (auto-registered in the global
/// registry) and extracts its per-query memory pool as a `dyn MemoryPool`.
///
/// The returned pool is `Some` only when tracking is active; pass it to the
/// executor so the query's allocations are charged against a pool that wraps
/// `global_pool` (global limits stay enforced). Returns the context too so the
/// caller can attach phantom reservations/correctors before execution.
pub fn new_query_tracking_context(
    context_id: i64,
    global_pool: Arc<dyn MemoryPool>,
    query_type: QueryType,
) -> (QueryTrackingContext, Option<Arc<dyn MemoryPool>>) {
    let query_context = QueryTrackingContext::new(context_id, global_pool, query_type);
    let query_memory_pool = query_context
        .memory_pool()
        .map(|p| p as Arc<dyn MemoryPool>);
    (query_context, query_memory_pool)
}

/// Builds the per-query `RuntimeEnv` and registers the shard-specific object store.
///
/// Combines the three steps every query-execution entry point shares:
/// 1. Build a per-query `RuntimeEnv` with the list-files cache pre-populated for
///    `table_path` / `object_metas` (via [`build_query_runtime_env`]).
/// 2. If `query_memory_pool` is `Some`, rebuild the env with that pool overlaid.
///    The per-query pool wraps the global pool, so global limits stay enforced.
/// 3. Register `shard_store` on the `file://` scheme so this query's reads route
///    through `TieredObjectStore` (local + remote) or the default `LocalFileSystem`.
pub fn build_query_runtime_env_with_store(
    runtime: &DataFusionRuntime,
    table_path: &ListingTableUrl,
    object_metas: &[ObjectMeta],
    shard_store: Arc<dyn ObjectStore>,
    query_memory_pool: Option<Arc<dyn MemoryPool>>,
) -> Result<Arc<RuntimeEnv>, DataFusionError> {
    // Build per-query RuntimeEnv with list-files cache pre-populated.
    let runtime_env = build_query_runtime_env(runtime, table_path, object_metas)?;

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
    runtime_env.register_object_store(&url::Url::parse("file://").unwrap(), shard_store);

    Ok(runtime_env)
}

/// Registers a standard DataFusion `ListingTable` for `table_path` under
/// `table_name` so the substrait consumer can resolve the table.
///
/// Shared by the vanilla and indexed execution paths. Infers + coerces the
/// schema, and — when `sort_fields`/`sort_orders` describe an `index.sort.field`
/// — declares the per-file sort order to DataFusion (see
/// [`build_file_sort_order`] for what that buys and its case/nulls caveats).
/// Pass empty slices to skip the sort-order declaration.
pub async fn register_listing_table(
    ctx: &SessionContext,
    table_name: &str,
    table_path: ListingTableUrl,
    sort_fields: &[String],
    sort_orders: &[String],
) -> Result<(), DataFusionError> {
    let mut listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
        .with_file_extension(".parquet")
        .with_collect_stat(true);
    if let Some(sort_exprs) = build_file_sort_order(sort_fields, sort_orders) {
        listing_options = listing_options.with_file_sort_order(vec![sort_exprs]);
    }

    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await
        .map_err(|e| {
            error!("Failed to infer schema: {}", e);
            e
        })?;
    let resolved_schema = coerce_inferred_schema(resolved_schema);

    let table_config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);
    let provider = Arc::new(ListingTable::try_new(table_config).map_err(|e| {
        error!("Failed to create listing table: {}", e);
        e
    })?);
    ctx.register_table(table_name, provider).map_err(|e| {
        error!("Failed to register listing table: {}", e);
        e
    })?;
    Ok(())
}

/// Builds a per-query [`SessionContext`] shared by the vanilla and indexed paths.
///
/// Sets the common execution options (parquet pushdown, target partitions, batch
/// size) from `query_config`, installs `runtime_env`, and registers the base
/// scalar/aggregate UDFs.
///
/// `target_partitions` is passed explicitly because the paths differ: the vanilla
/// path uses `query_config.target_partitions`, while the indexed path fans out via
/// its own IndexedExec partitions and only needs a sane value here for any
/// post-scan operators.
///
/// When `indexed_path` is true, the context is built for the indexed executor:
/// the physical optimizer drops the combine-partial-final pass, and the indexed
/// `index_filter` / `delegation_possible` UDFs are registered on top of the base
/// UDFs. When false, the vanilla path's defaults are used.
pub fn build_query_session_context(
    query_config: &DatafusionQueryConfig,
    runtime_env: Arc<RuntimeEnv>,
    target_partitions: usize,
    indexed_path: bool,
) -> SessionContext {
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters =
        query_config.listing_table_pushdown_filters;
    config.options_mut().execution.target_partitions = target_partitions.max(1);
    config.options_mut().execution.batch_size = query_config.batch_size;

    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime_env)
        .with_default_features();
    if indexed_path {
        // Indexed executor drives partial/final aggregation itself; drop the
        // combine-partial-final physical optimizer pass.
        builder = builder.with_physical_optimizer_rules(physical_optimizer_rules_without_combine());
    }
    let state = builder.build();

    let ctx = SessionContext::new_with_state(state);
    udf::register_all(&ctx);
    udaf::register_all(&ctx);
    if indexed_path {
        // Indexed-path-only UDFs, on top of the base UDFs above.
        ctx.register_udf(create_index_filter_udf());
        ctx.register_udf(create_delegation_possible_udf());
    }
    ctx
}
