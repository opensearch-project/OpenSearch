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
//! leaves other scan paths on DataFusion's default reader, which loads the full
//! all-column page index every query and caches none of it.
//!
//! This rule walks the physical plan, finds each parquet `DataSourceExec`, reads
//! the predicate already pushed onto its `ParquetSource`, derives the predicate
//! columns, and swaps in a [`ScopedPageIndexReaderFactory`] scoped to those
//! columns. It runs after DataFusion's own optimizers (which is when filter
//! pushdown has populated `ParquetSource::predicate`), so it works uniformly for
//! `ListingTable`, `ShardTableProvider`, and any future parquet provider.
//!
//! # Replace, do NOT skip-if-present
//!
//! DataFusion's `ParquetFormat::create_physical_plan` ALWAYS pre-installs its own
//! `CachedParquetFileReaderFactory` (the full all-column page-index loader). That
//! is exactly the factory we want to replace, so this rule does not skip a scan
//! just because a factory is already set. (A skip-if-present guard was the
//! original bug that made the end-to-end listing scan never use the scoped
//! reader.) The indexed path does not run this rule — it uses its own executor.
//!
//! # No-op cases (left exactly as DataFusion would run them)
//!
//!  - A `DataSourceExec` that isn't parquet — skipped.
//!  - A parquet scan with no predicate, or whose predicate references no file
//!    columns — skipped (nothing to scope; the opener loads on demand as today).

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::datasource::physical_plan::{FileSource, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use object_store::ObjectStore;

use crate::scoped_page_index_reader::ScopedPageIndexReaderFactory;

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
    pub fn new(store: Arc<dyn ObjectStore>, metadata_cache: Arc<dyn FileMetadataCache>) -> Self {
        Self {
            store,
            metadata_cache,
        }
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

            let file_schema = config.file_schema();
            let predicate = parquet.filter();

            // Predicate column NAMES — empty when there's no pushed-down filter.
            let mut predicate_names: Vec<String> = predicate
                .as_ref()
                .map(|p| {
                    let mut names: Vec<String> = collect_columns(p)
                        .into_iter()
                        .map(|c| c.name().to_string())
                        .filter(|n| file_schema.index_of(n).is_ok())
                        .collect();
                    names.sort();
                    names.dedup();
                    names
                })
                .unwrap_or_default();

            // Projected column NAMES — the file-schema columns this scan actually READS.
            //
            // We must derive these from the projection's underlying column references, NOT from
            // the projected output field names. With expression push-down the projected schema can
            // contain computed fields (e.g. `CASE WHEN status = 200 ...`) whose names are not in the
            // file schema; those expressions still read the base column (`status`). Matching output
            // field names against the file schema would drop `status`, leaving it without a real
            // OffsetIndex — and a column that is read but only has the single-page placeholder OI
            // corrupts the page decode ("provided output is too small for the decompressed data").
            // `ProjectionExprs::column_indices()` walks every projection expression and returns the
            // referenced file-schema column indices, which is exactly the read set we must scope to.
            let num_file_cols = file_schema.fields().len();
            let projection_names: Vec<String> = match config.file_source().projection() {
                Some(proj) => proj
                    .column_indices()
                    .into_iter()
                    .filter(|&i| i < num_file_cols)
                    .map(|i| file_schema.field(i).name().to_string())
                    .collect(),
                // No projection pushed → the scan reads every column.
                None => Vec::new(),
            };

            // Only scope when there's something to scope to. A full-schema scan with no predicate
            // gains nothing from scoping — skip it. A None projection (read-all) or a projection
            // that already covers every column is not a strict subset.
            let is_projected =
                !projection_names.is_empty() && projection_names.len() < num_file_cols;
            if predicate_names.is_empty() && !is_projected {
                return Ok(Transformed::no(node));
            }
            // Pass empty projection when the scan reads all columns — the factory
            // will build all-column OffsetIndex (existing behavior).
            let projection_names = if is_projected {
                projection_names
            } else {
                Vec::new()
            };

            // Build the scoped factory and reinstall the source. The predicate is
            // retained for parity but not used for RG scoping (Step 1 builds an
            // all-row-group, column-scoped page index — see the reader's docs).
            let factory = Arc::new(ScopedPageIndexReaderFactory::new(
                Arc::clone(&self.store),
                Arc::clone(&self.metadata_cache),
                predicate_names,
                projection_names,
                predicate,
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
    use crate::cache::page_index;
    use crate::parquet_page_cache::{clear_scoped_cache_for_test, scoped_cache_stats};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::cache::DefaultFilesMetadataCache;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{lit, BinaryExpr, Column};
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
        let config =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(parquet))
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
        let cfg =
            (dse.data_source().as_ref() as &dyn std::any::Any).downcast_ref::<FileScanConfig>()?;
        let pq =
            (cfg.file_source().as_ref() as &dyn std::any::Any).downcast_ref::<ParquetSource>()?;
        Some(pq.parquet_file_reader_factory().is_some())
    }

    #[test]
    fn installs_factory_when_predicate_present() {
        let sch = schema();
        let (store, cache) = deps();
        let parquet = parquet_for(&sch).with_predicate(predicate_on_a());
        let plan = datasource_exec(parquet);
        assert_eq!(
            get_factory(&plan),
            Some(false),
            "precondition: no factory yet"
        );

        let rule = ScopedPageIndexOptimizer::new(store, cache);
        let out = rule.optimize(plan, &ConfigOptions::default()).unwrap();
        assert_eq!(
            get_factory(&out),
            Some(true),
            "optimizer must install a scoped reader factory when a predicate is present"
        );
    }

    #[test]
    fn noop_without_predicate_or_projection() {
        let sch = schema();
        let (store, cache) = deps();
        // No predicate, no pushed projection — nothing to scope.
        let plan = datasource_exec(parquet_for(&sch));
        let rule = ScopedPageIndexOptimizer::new(store, cache);
        let out = rule.optimize(plan, &ConfigOptions::default()).unwrap();
        assert_eq!(
            get_factory(&out),
            Some(false),
            "no predicate and no projection → nothing to scope → no factory installed"
        );
    }

    /// A projection-only scan (no predicate pushed down) still needs a scoped
    /// OffsetIndex so the parquet reader fetches only matched-row pages instead
    /// of whole column chunks. The factory must be installed from the projected
    /// schema even when `parquet.filter()` is `None`.
    #[test]
    fn installs_factory_for_projection_only_scan() {
        use datafusion::parquet::arrow::ProjectionMask;
        use datafusion_datasource::file_scan_config::FileScanConfigBuilder;

        let sch = schema(); // fields: a(0), b(1)
        let (store, cache) = deps();

        // Project only column `a` — no predicate.
        let parquet = parquet_for(&sch);
        let config =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(parquet))
                .with_projection(Some(vec![0])) // project `a` only
                .build();
        let plan = DataSourceExec::from_data_source(config);

        let rule = ScopedPageIndexOptimizer::new(store, cache);
        let out = rule.optimize(plan, &ConfigOptions::default()).unwrap();
        assert_eq!(
            get_factory(&out),
            Some(true),
            "projection-only scan must get a scoped factory for OffsetIndex scoping"
        );
    }

    /// The rule REPLACES an already-installed factory when a predicate is present
    /// (DataFusion's `ParquetFormat` always pre-installs its own).
    #[test]
    fn replaces_existing_default_factory() {
        let sch = schema();
        let (store, cache) = deps();
        let pre = Arc::new(ScopedPageIndexReaderFactory::new(
            Arc::clone(&store),
            Arc::clone(&cache),
            vec!["a".to_string()],
            vec!["a".to_string()],
            None,
            sch.clone(),
        ));
        let parquet = parquet_for(&sch)
            .with_predicate(predicate_on_a())
            .with_parquet_file_reader_factory(pre);
        let plan = datasource_exec(parquet);
        assert_eq!(
            get_factory(&plan),
            Some(true),
            "precondition: a factory is present"
        );

        let rule = ScopedPageIndexOptimizer::new(store, cache);
        let out = rule
            .optimize(Arc::clone(&plan), &ConfigOptions::default())
            .unwrap();
        assert_eq!(
            get_factory(&out),
            Some(true),
            "scoped factory present after rule"
        );
        assert!(
            !Arc::ptr_eq(&plan, &out),
            "rule must rewrite the scan to install the scoped factory, replacing the default"
        );
    }

    /// End-to-end through a real `SessionContext` + stock `ListingTable`: write a
    /// parquet file, register it, plan `SELECT s0 WHERE n1 >= k`, apply the rule,
    /// execute, and assert (a) results are correct and (b) the shared scoped
    /// page-index cache filled — proving the rule installs a working scoped reader
    /// on the vanilla listing path.
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
        use datafusion::prelude::SessionContext;
        use futures::StreamExt;

        // Serialize on the shared guard — this asserts on the global cache.
        let _g = page_index::SCOPED_CACHE_TEST_GUARD.lock().unwrap();
        crate::cache::page_index::clear_scoped_cache_for_test();

        let sch = Arc::new(Schema::new(vec![
            Field::new("n0", DataType::Int32, false),
            Field::new("n1", DataType::Int32, false),
            Field::new("s0", DataType::Utf8, false),
            Field::new("s1", DataType::Utf8, false),
        ]));
        const ROWS: i32 = 4096;
        let n0: Vec<i32> = (0..ROWS).collect();
        let n1: Vec<i32> = (0..ROWS).collect();
        let s0: Vec<String> = (0..ROWS)
            .map(|r| format!("s0_{r:06}_padding_padding"))
            .collect();
        let s1: Vec<String> = (0..ROWS)
            .map(|r| format!("s1_{r:06}_padding_padding"))
            .collect();
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

        let ctx = SessionContext::new();
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let table_url =
            ListingTableUrl::parse(format!("file://{}", dir.to_str().unwrap())).unwrap();
        ctx.register_object_store(table_url.as_ref(), Arc::clone(&store));
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let resolved = listing_options
            .infer_schema(&ctx.state(), &table_url)
            .await
            .unwrap();
        let config = ListingTableConfig::new(table_url.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved);
        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        ctx.register_table("t", provider).unwrap();

        // n1 >= 4080 keeps rows 4080..4096 (16 rows); project the non-predicate s0.
        let df = ctx.sql("SELECT s0 FROM t WHERE n1 >= 4080").await.unwrap();
        let physical = df.create_physical_plan().await.unwrap();

        let metadata_cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
        let rule = ScopedPageIndexOptimizer::new(Arc::clone(&store), metadata_cache);
        let physical = rule.optimize(physical, &ConfigOptions::default()).unwrap();

        let mut stream =
            datafusion::physical_plan::execute_stream(physical, ctx.task_ctx()).unwrap();
        let mut rows = 0usize;
        while let Some(b) = stream.next().await {
            rows += b.unwrap().num_rows();
        }

        assert_eq!(rows, 16, "predicate n1>=4080 must keep 16 rows");

        let stats = scoped_cache_stats();
        assert!(
            stats.entries >= 1 && stats.used_bytes > 0,
            "scoped cache must have filled on the listing path: {stats:?}"
        );
        assert!(
            stats.misses >= 1,
            "first scan must register a scoped-cache miss: {stats:?}"
        );

        clear_scoped_cache_for_test();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
