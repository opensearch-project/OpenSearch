/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Physical optimizer rule that installs the scoped page-index reader factory on
//! **every** parquet scan in the plan — provider-agnostic.
//!
//! # Why a rule (not a TableProvider)
//!
//! The scoped page-index loader is a property of *how we read parquet*, not of
//! *which TableProvider* produced the scan. Wiring it into a specific provider
//! (e.g. `ShardTableProvider`) leaves other scan paths — the stock
//! `ListingTable` used for `QueryStrategy::None` plain scans — on DataFusion's
//! default reader, which loads the full all-column page index every query and
//! caches none of it.
//!
//! This rule walks the physical plan, finds each parquet `DataSourceExec`, reads
//! the predicate already pushed onto its `ParquetSource`, derives the predicate
//! columns, and swaps in a [`ScopedPageIndexReaderFactory`] scoped to those
//! columns. It runs after DataFusion's own optimizers (which is when filter
//! pushdown has populated `ParquetSource::predicate`), so it works uniformly for
//! `ListingTable`, `ShardTableProvider`, and any future parquet provider.
//!
//! # No-op cases (left exactly as DataFusion would run them)
//!
//!  - A `DataSourceExec` that isn't parquet, or whose source already has a custom
//!    reader factory (e.g. the indexed path's `CachedMetadataReaderFactory`, or
//!    `ShardTableProvider` once it installs its own scoped factory) — skipped, so
//!    we never double-wrap.
//!  - A parquet scan with no predicate, or whose predicate references no file
//!    columns — skipped (nothing to scope; the opener loads on demand as today).
//!
//! Correctness of the scoped index itself (real OffsetIndex for all columns) is
//! handled in `page_index_loader::build_augmented_metadata`; this rule only
//! decides *where* to attach the loader.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use object_store::ObjectStore;

use crate::shard_scoped_reader::ScopedPageIndexReaderFactory;

/// Installs the scoped page-index reader factory on parquet scans.
///
/// Carries the object store and shared metadata cache because a
/// `PhysicalOptimizerRule` has no access to the session; the caller constructs
/// it from the query's `RuntimeEnv`.
#[derive(Debug)]
pub struct ScopedPageIndexOptimizer {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
}

impl ScopedPageIndexOptimizer {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
    ) -> Self {
        Self { store, metadata_cache }
    }
}

impl PhysicalOptimizerRule for ScopedPageIndexOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let rewritten = plan.transform_up(|node| {
            let Some(dse) = node.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(config) = dse.data_source().as_ref().downcast_ref::<FileScanConfig>() else {
                return Ok(Transformed::no(node));
            };
            let Some(parquet) = (config.file_source().as_ref() as &dyn std::any::Any)
                .downcast_ref::<ParquetSource>()
            else {
                return Ok(Transformed::no(node));
            };

            // Need a predicate to scope to. No predicate → nothing to do.
            //
            // Note: we intentionally do NOT skip when a factory is already set.
            // DataFusion's `ParquetFormat::create_physical_plan` ALWAYS installs
            // its own `CachedParquetFileReaderFactory` (which loads the full
            // all-column page index) — that is exactly the factory we want to
            // replace. Replacing it with our scoped factory is the whole point.
            // The indexed path does not run this rule (it uses its own executor),
            // and `ShardTableProvider` installs an equivalent scoped factory at
            // construction, so replacing that with another scoped factory is
            // harmless/idempotent.
            let Some(predicate) = parquet.predicate() else {
                return Ok(Transformed::no(node));
            };

            // Derive predicate column NAMES that exist in the file schema. The
            // reader resolves them to per-file parquet leaf indices.
            let file_schema = config.file_schema();
            let mut names: Vec<String> = collect_columns(predicate)
                .into_iter()
                .map(|c| c.name().to_string())
                .filter(|n| file_schema.index_of(n).is_ok())
                .collect();
            names.sort();
            names.dedup();
            if names.is_empty() {
                return Ok(Transformed::no(node));
            }

            // Build the scoped factory and reinstall the source.
            let factory = Arc::new(ScopedPageIndexReaderFactory::new(
                Arc::clone(&self.store),
                Arc::clone(&self.metadata_cache),
                names,
                Arc::clone(file_schema),
            ));
            let new_source = parquet.clone().with_parquet_file_reader_factory(factory);
            let new_config = FileScanConfigBuilder::from(config.clone())
                .with_source(Arc::new(new_source))
                .build();
            let new_dse: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(new_config);
            Ok(Transformed::yes(new_dse))
        })?;
        Ok(rewritten.data)
    }

    fn name(&self) -> &str {
        "ScopedPageIndexOptimizer"
    }

    /// We swap a reader factory only; the scan's output schema is unchanged.
    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::cache::DefaultFilesMetadataCache;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::expressions::{lit, BinaryExpr, Column};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion_datasource::table_schema::TableSchema;
    use object_store::memory::InMemory;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]))
    }

    fn deps() -> (Arc<dyn ObjectStore>, Arc<dyn FileMetadataCache>) {
        (
            Arc::new(InMemory::new()),
            Arc::new(DefaultFilesMetadataCache::new(64 * 1024 * 1024)),
        )
    }

    fn datasource_exec(parquet: ParquetSource) -> Arc<dyn ExecutionPlan> {
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(parquet),
        )
        .build();
        DataSourceExec::from_data_source(config)
    }

    fn predicate_on_a() -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            lit(5i32),
        ))
    }

    fn parquet_for(sch: &Arc<Schema>) -> ParquetSource {
        ParquetSource::new(TableSchema::new(sch.clone(), vec![]))
    }

    fn get_factory(plan: &Arc<dyn ExecutionPlan>) -> Option<bool> {
        let dse = plan.downcast_ref::<DataSourceExec>()?;
        let cfg = (dse.data_source().as_ref() as &dyn std::any::Any)
            .downcast_ref::<FileScanConfig>()?;
        let pq = (cfg.file_source().as_ref() as &dyn std::any::Any)
            .downcast_ref::<ParquetSource>()?;
        Some(pq.parquet_file_reader_factory().is_some())
    }

    #[test]
    fn installs_factory_when_predicate_present() {
        let sch = schema();
        let (store, cache) = deps();
        let parquet = parquet_for(&sch).with_predicate(predicate_on_a());
        let plan = datasource_exec(parquet);
        assert_eq!(get_factory(&plan), Some(false), "precondition: no factory yet");

        let rule = ScopedPageIndexOptimizer::new(store, cache);
        let out = rule.optimize(plan, &ConfigOptions::default()).unwrap();
        assert_eq!(
            get_factory(&out),
            Some(true),
            "optimizer must install a scoped reader factory when a predicate is present"
        );
    }

    #[test]
    fn noop_without_predicate() {
        let sch = schema();
        let (store, cache) = deps();
        let plan = datasource_exec(parquet_for(&sch)); // no predicate
        let rule = ScopedPageIndexOptimizer::new(store, cache);
        let out = rule.optimize(plan, &ConfigOptions::default()).unwrap();
        assert_eq!(
            get_factory(&out),
            Some(false),
            "no predicate → nothing to scope → no factory installed"
        );
    }

    /// End-to-end through a real `SessionContext` + stock `ListingTable` (the
    /// provider used by `QueryStrategy::None` plain scans): write a parquet file,
    /// register it, build the physical plan for `SELECT s0 WHERE n1 > k`, apply
    /// `ScopedPageIndexOptimizer`, execute, and assert (a) results are correct and
    /// (b) the shared scoped page-index cache filled — proving the rule installs a
    /// working scoped reader on the vanilla listing path, not just in isolation.
    #[tokio::test]
    async fn end_to_end_listing_scan_fills_scoped_cache() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
        use datafusion::physical_optimizer::PhysicalOptimizerRule;
        use datafusion::prelude::SessionContext;
        use futures::StreamExt;

        crate::indexed_table::page_index_loader::clear_scoped_cache_for_test();

        // 2 numeric + 2 wide-string columns, many pages → real page index.
        let sch = Arc::new(Schema::new(vec![
            Field::new("n0", DataType::Int32, false),
            Field::new("n1", DataType::Int32, false),
            Field::new("s0", DataType::Utf8, false),
            Field::new("s1", DataType::Utf8, false),
        ]));
        const ROWS: i32 = 4096;
        let n0: Vec<i32> = (0..ROWS).collect();
        let n1: Vec<i32> = (0..ROWS).collect();
        let s0: Vec<String> = (0..ROWS).map(|r| format!("s0_{r:06}_padding_padding")).collect();
        let s1: Vec<String> = (0..ROWS).map(|r| format!("s1_{r:06}_padding_padding")).collect();
        let batch = RecordBatch::try_new(
            sch.clone(),
            vec![
                Arc::new(Int32Array::from(n0)),
                Arc::new(Int32Array::from(n1)),
                Arc::new(StringArray::from(s0)),
                Arc::new(StringArray::from(s1)),
            ],
        )
        .unwrap();

        let dir = std::env::temp_dir().join(format!("scoped_e2e_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let file_path = dir.join("data.parquet");
        {
            let props = WriterProperties::builder()
                .set_data_page_row_count_limit(256)
                .set_write_batch_size(256)
                .set_statistics_enabled(EnabledStatistics::Page)
                .build();
            let f = std::fs::File::create(&file_path).unwrap();
            let mut w = ArrowWriter::try_new(f, sch.clone(), Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        // Stock ListingTable over the temp dir — the QueryStrategy::None provider.
        let ctx = SessionContext::new();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let table_url =
            ListingTableUrl::parse(format!("file://{}", dir.to_str().unwrap())).unwrap();
        ctx.register_object_store(table_url.as_ref(), Arc::clone(&store));
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let resolved = listing_options.infer_schema(&ctx.state(), &table_url).await.unwrap();
        let config = ListingTableConfig::new(table_url.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved);
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        ctx.register_table("t", provider).unwrap();

        // Predicate on n1 (parquet col 1), project the non-predicate string col s0.
        // n1 >= 4080 keeps rows 4080..4096 (16 rows).
        let df = ctx.sql("SELECT s0 FROM t WHERE n1 >= 4080").await.unwrap();
        let physical = df.create_physical_plan().await.unwrap();

        // Apply our rule exactly as query_executor does, with this ctx's store + cache.
        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let rule = ScopedPageIndexOptimizer::new(Arc::clone(&store), metadata_cache);
        let physical = rule.optimize(physical, &ConfigOptions::default()).unwrap();

        // Execute.
        let mut stream =
            datafusion::physical_plan::execute_stream(physical, ctx.task_ctx()).unwrap();
        let mut rows = 0usize;
        while let Some(b) = stream.next().await {
            rows += b.unwrap().num_rows();
        }

        // (a) Correct result: 16 rows survive n1 >= 4080.
        assert_eq!(rows, 16, "predicate n1>=4080 must keep 16 rows");

        // (b) The scoped page-index cache filled — the rule installed a working
        // scoped reader on the stock ListingTable scan.
        let stats = crate::indexed_table::page_index_loader::scoped_cache_stats();
        assert!(
            stats.entries >= 1 && stats.used_bytes > 0,
            "scoped cache must have filled on the listing path: {stats:?}"
        );
        assert!(stats.misses >= 1, "first scan must register a scoped-cache miss: {stats:?}");

        crate::indexed_table::page_index_loader::clear_scoped_cache_for_test();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The rule REPLACES an already-installed factory when a predicate is present.
    /// This is required: DataFusion's `ParquetFormat::create_physical_plan` always
    /// pre-installs its own `CachedParquetFileReaderFactory` (full-page-index
    /// loader) — that is precisely the factory we must swap out for our scoped one.
    /// (A skip-if-present guard was the original bug that made the end-to-end
    /// listing scan never use the scoped reader.)
    #[test]
    fn replaces_existing_default_factory() {
        let sch = schema();
        let (store, cache) = deps();
        // Pre-install some factory + a predicate (mirrors what ParquetFormat does).
        let pre = Arc::new(ScopedPageIndexReaderFactory::new(
            Arc::clone(&store),
            Arc::clone(&cache),
            vec!["a".to_string()],
            sch.clone(),
        ));
        let parquet = parquet_for(&sch)
            .with_predicate(predicate_on_a())
            .with_parquet_file_reader_factory(pre);
        let plan = datasource_exec(parquet);
        assert_eq!(get_factory(&plan), Some(true), "precondition: a factory is present");

        let rule = ScopedPageIndexOptimizer::new(store, cache);
        let out = rule.optimize(Arc::clone(&plan), &ConfigOptions::default()).unwrap();

        // The rule rewrites the node (does not return the same Arc) and the result
        // still carries a (scoped) factory.
        assert_eq!(get_factory(&out), Some(true), "scoped factory present after rule");
        assert!(
            !Arc::ptr_eq(&plan, &out),
            "rule must rewrite the scan to install the scoped factory, replacing the default"
        );
    }
}
