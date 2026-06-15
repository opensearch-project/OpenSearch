/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Scoped page-index reader factory for the **listing-table** scan path.
//!
//! # Why this exists
//!
//! The indexed-table path already loads page index lazily and scoped to the
//! predicate columns (see [`crate::indexed_table::page_index_loader`]). The
//! listing-table path (`ShardTableProvider` / vanilla `ListingTable`) does not:
//! it uses DataFusion's default reader factory, so when page pruning is enabled
//! the `ParquetOpener` loads the **entire** page index (`ColumnIndex` +
//! `OffsetIndex` for *every* column) of each surviving file, every query, and
//! caches none of it.
//!
//! This factory closes that gap by reusing the indexed path's machinery. The
//! seam is DataFusion's [`ParquetFileReaderFactory`]: the `ParquetOpener` asks
//! the reader for metadata via `get_metadata`, and — per the trait's own
//! contract and `opener::load_page_index` — if the returned `ParquetMetaData`
//! *already* carries a page index, the opener uses it and skips the full,
//! all-column load. So our reader's `get_metadata`:
//!
//!   1. loads footer-only metadata (shared cache hit — see
//!      [`crate::indexed_table::parquet_bridge::load_parquet_metadata`]), then
//!   2. augments it with a page index scoped to the predicate columns via
//!      [`crate::indexed_table::page_index_loader::load_scoped_page_index`]
//!      (real `ColumnIndex` for predicate columns, real `OffsetIndex` for all
//!      columns — the same correctness contract the indexed path relies on), and
//!   3. returns that augmented metadata.
//!
//! The opener then finds both indexes present and never triggers a full decode.
//! The scoped `(file, predicate-columns)` cache is shared with the indexed path,
//! so repeated queries reuse the decoded index across both scan paths.
//!
//! # Fallback
//!
//! If there are no predicate columns, or scoped augmentation fails for a file
//! (no page index, decode/IO error), `get_metadata` returns the footer-only
//! metadata. The opener then loads the page index on demand exactly as it does
//! today — correct, just without the scoping benefit for that file. Never a
//! wrong result.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::parquet::{
    ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::{ObjectStore, ObjectStoreExt};
use prost::bytes::Bytes;

use crate::indexed_table::page_index_loader::{
    load_scoped_page_index, resolve_predicate_parquet_columns,
};
use crate::indexed_table::parquet_bridge::load_parquet_metadata;

/// A [`ParquetFileReaderFactory`] that, on `get_metadata`, returns metadata whose
/// page index is scoped to the query's predicate columns. Data reads go straight
/// to the object store.
///
/// Carries predicate column *names* + the file schema rather than pre-resolved
/// parquet indices: the reader resolves names → parquet leaf indices per file via
/// the same `resolve_predicate_parquet_columns` the indexed path uses, which is
/// robust to schema evolution across files (a column absent from one file is just
/// skipped there).
#[derive(Debug)]
pub struct ScopedPageIndexReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    /// File-column names referenced by the query predicate. Empty means "no
    /// scoping" — `get_metadata` returns footer-only and the opener loads the
    /// page index on demand as usual.
    predicate_column_names: Arc<Vec<String>>,
    /// File schema (no partition columns), for per-file column resolution.
    file_schema: SchemaRef,
}

impl ScopedPageIndexReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
        predicate_column_names: Vec<String>,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            store,
            metadata_cache,
            predicate_column_names: Arc::new(predicate_column_names),
            file_schema,
        }
    }
}

impl ParquetFileReaderFactory for ScopedPageIndexReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics =
            ParquetFileMetrics::new(partition_index, file.object_meta.location.as_ref(), metrics);
        Ok(Box::new(ScopedPageIndexReader {
            store: Arc::clone(&self.store),
            metadata_cache: Arc::clone(&self.metadata_cache),
            predicate_column_names: Arc::clone(&self.predicate_column_names),
            file_schema: Arc::clone(&self.file_schema),
            location: file.object_meta.location.clone(),
            metrics: file_metrics,
        }))
    }
}

struct ScopedPageIndexReader {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    predicate_column_names: Arc<Vec<String>>,
    file_schema: SchemaRef,
    location: object_store::path::Path,
    metrics: ParquetFileMetrics,
}

impl AsyncFileReader for ScopedPageIndexReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Bytes>> {
        self.metrics
            .bytes_scanned
            .add((range.end - range.start) as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        // IO-runtime dispatch is handled by the SpawnIoStore wrapper around the
        // registered store, so a plain `.await` already runs on the IO runtime.
        async move {
            store
                .get_range(&location, range)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Vec<Bytes>>> {
        let total: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        self.metrics.bytes_scanned.add(total as usize);
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        async move {
            store
                .get_ranges(&location, &ranges)
                .await
                .map_err(|e| datafusion::parquet::errors::ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
        _options: Option<&ArrowReaderOptions>,
    ) -> BoxFuture<'_, datafusion::parquet::errors::Result<Arc<ParquetMetaData>>> {
        let store = Arc::clone(&self.store);
        let metadata_cache = Arc::clone(&self.metadata_cache);
        let predicate_names = Arc::clone(&self.predicate_column_names);
        let file_schema = Arc::clone(&self.file_schema);
        let location = self.location.clone();
        async move {
            // 1. Footer-only metadata (shared cache hit if pre-seeded). This is
            //    the same loader the indexed path uses; it never retains a page
            //    index in the shared cache.
            let (_schema, _size, footer) =
                load_parquet_metadata(Arc::clone(&store), &location, Arc::clone(&metadata_cache))
                    .await
                    .map_err(|e| {
                        datafusion::parquet::errors::ParquetError::General(format!(
                            "footer metadata {}: {e}",
                            location
                        ))
                    })?;

            // 2. Resolve predicate column names → this file's parquet leaf indices
            //    (per-file, so schema evolution across files is handled), then
            //    augment with a predicate-scoped page index. On empty column set
            //    or any failure, `load_scoped_page_index` returns None and we fall
            //    back to footer-only; the opener then loads the page index on
            //    demand exactly as it does today.
            if !predicate_names.is_empty() {
                let parquet_cols =
                    resolve_predicate_parquet_columns(&file_schema, &footer, &predicate_names);
                if let Some(augmented) =
                    load_scoped_page_index(&store, &location, &footer, &parquet_cols).await
                {
                    return Ok(augmented);
                }
            }

            Ok(footer)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::cache::cache_manager::FileMetadataCache;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    /// Two int columns (`price`, `qty`), one row group, four 8-row data pages —
    /// enough to produce a real page index.
    fn two_col_parquet() -> (Bytes, SchemaRef) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("price", DataType::Int32, false),
            Field::new("qty", DataType::Int32, false),
        ]));
        let prices: Vec<i32> = (0..32).collect();
        let qtys: Vec<i32> = (100..132).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(prices)),
                Arc::new(Int32Array::from(qtys)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(32)
            .set_data_page_row_count_limit(8)
            .set_write_batch_size(8)
            .set_statistics_enabled(EnabledStatistics::Page)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        (Bytes::from(buf), schema)
    }

    async fn stage(bytes: Bytes) -> (Arc<dyn ObjectStore>, ObjPath) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = ObjPath::from("data.parquet");
        store.put(&loc, PutPayload::from_bytes(bytes)).await.unwrap();
        (store, loc)
    }

    fn fresh_cache() -> Arc<dyn FileMetadataCache> {
        Arc::new(crate::cache::MutexFileMetadataCache::new(
            datafusion::execution::cache::DefaultFilesMetadataCache::new(64 * 1024 * 1024),
        ))
    }

    /// The factory's reader must, on `get_metadata`, return metadata whose page
    /// index is scoped to the predicate column (`price`) — a real ColumnIndex for
    /// `price`, a NONE placeholder ColumnIndex for `qty` — while keeping a REAL
    /// OffsetIndex for BOTH columns (so reads of a projected non-predicate column
    /// work; this is the listing-path mirror of the indexed-path read-path fix).
    #[tokio::test]
    async fn get_metadata_returns_scoped_page_index() {
        crate::indexed_table::page_index_loader::clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes).await;
        let cache = fresh_cache();

        let factory = ScopedPageIndexReaderFactory::new(
            Arc::clone(&store),
            cache,
            vec!["price".to_string()],
            schema,
        );

        let pf = PartitionedFile::new(loc.as_ref().to_string(), {
            store.head(&loc).await.unwrap().size
        });
        let metrics = ExecutionPlanMetricsSet::new();
        let mut reader = factory
            .create_reader(0, pf, None, &metrics)
            .expect("create_reader");

        let meta = reader.get_metadata(None).await.expect("get_metadata");

        // Page index present and scoped.
        let ci = meta.column_index().expect("augmented has column index");
        let oi = meta.offset_index().expect("augmented has offset index");
        // price (col 0) is the predicate column → real ColumnIndex.
        assert!(
            !matches!(ci[0][0], ColumnIndexMetaData::NONE),
            "predicate column (price) must have a real ColumnIndex"
        );
        // qty (col 1) is NOT a predicate column → NONE ColumnIndex placeholder.
        assert!(
            matches!(ci[0][1], ColumnIndexMetaData::NONE),
            "non-predicate column (qty) ColumnIndex must be the NONE placeholder"
        );
        // BOTH columns must keep a real OffsetIndex (the read-path requirement).
        assert!(
            !oi[0][0].page_locations.is_empty(),
            "price must keep a real OffsetIndex"
        );
        assert!(
            !oi[0][1].page_locations.is_empty(),
            "qty (non-predicate) must keep a real OffsetIndex, not an empty placeholder"
        );
    }

    /// DECISIVE EXPERIMENT: build a `DataSourceExec` with the factory installed at
    /// `ParquetSource` construction (exactly like the working indexed path,
    /// `parquet_bridge::create_stream_with_access_plan`) and execute it directly.
    /// If the scoped cache fills here, the factory-at-construction approach works
    /// end-to-end and the bug is specifically the post-planning source swap in the
    /// optimizer rule.
    #[tokio::test]
    async fn factory_at_construction_executes_through_scoped_reader() {
        use datafusion::datasource::physical_plan::ParquetSource;
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::physical_expr::expressions::{lit, BinaryExpr, Column};
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion::physical_plan::{execute_stream, ExecutionPlan};
        use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
        use datafusion_datasource::table_schema::TableSchema;
        use datafusion_datasource::PartitionedFile;
        use futures::StreamExt;

        crate::indexed_table::page_index_loader::clear_scoped_cache_for_test();

        // Stage a real file on local FS (InMemory has no offset-index range reads
        // through get_ranges in the same way; local mirrors production closely).
        let (bytes, schema) = two_col_parquet();
        let dir = std::env::temp_dir().join(format!("scoped_ctor_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let file_path = dir.join("data.parquet");
        std::fs::write(&file_path, &bytes).unwrap();

        let store: Arc<dyn ObjectStore> = Arc::new(object_store::local::LocalFileSystem::new());
        let cache = fresh_cache();

        // Factory at construction — predicate column `price`.
        let factory = Arc::new(ScopedPageIndexReaderFactory::new(
            Arc::clone(&store),
            cache,
            vec!["price".to_string()],
            schema.clone(),
        ));

        // Predicate `price >= 16` (keeps the last two 8-row pages → 16 rows).
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("price", 0)),
            Operator::GtEq,
            lit(16i32),
        ));

        let parquet = ParquetSource::new(TableSchema::new(schema.clone(), vec![]))
            .with_predicate(predicate)
            .with_parquet_file_reader_factory(factory);

        let loc_str = file_path.to_string_lossy().to_string();
        let pf = PartitionedFile::new(loc_str, std::fs::metadata(&file_path).unwrap().len());
        let store_url = ObjectStoreUrl::local_filesystem();
        let config = FileScanConfigBuilder::new(store_url.clone(), Arc::new(parquet))
            .with_file(pf)
            .build();
        let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

        // Register the store on a TaskContext's runtime.
        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));

        // Wrap in CoalesceBatches + Repartition to mirror the e2e plan shape and
        // rule out "parent operators break it".
        use datafusion::physical_plan::repartition::RepartitionExec;
        use datafusion::physical_plan::Partitioning;
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(exec, Partitioning::RoundRobinBatch(4)).unwrap(),
        );

        let mut stream = execute_stream(exec, ctx.task_ctx()).unwrap();
        let mut rows = 0usize;
        while let Some(b) = stream.next().await {
            rows += b.unwrap().num_rows();
        }

        let stats = crate::indexed_table::page_index_loader::scoped_cache_stats();
        eprintln!("CTOR EXPERIMENT (with repartition): rows={rows} stats={stats:?}");

        assert_eq!(rows, 16, "price>=16 must keep 16 rows");
        assert!(
            stats.misses >= 1 && stats.entries >= 1 && stats.used_bytes > 0,
            "factory-at-construction must route the scan through our scoped reader: {stats:?}"
        );

        crate::indexed_table::page_index_loader::clear_scoped_cache_for_test();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// With no predicate columns, the reader returns footer-only metadata (no page
    /// index) — the opener then loads on demand exactly as it does today.
    #[tokio::test]
    async fn get_metadata_no_predicate_returns_footer_only() {
        crate::indexed_table::page_index_loader::clear_scoped_cache_for_test();
        let (bytes, schema) = two_col_parquet();
        let (store, loc) = stage(bytes).await;
        let cache = fresh_cache();

        let factory = ScopedPageIndexReaderFactory::new(
            Arc::clone(&store),
            cache,
            vec![], // no predicate columns
            schema,
        );
        let pf = PartitionedFile::new(loc.as_ref().to_string(), {
            store.head(&loc).await.unwrap().size
        });
        let metrics = ExecutionPlanMetricsSet::new();
        let mut reader = factory.create_reader(0, pf, None, &metrics).unwrap();
        let meta = reader.get_metadata(None).await.unwrap();
        assert!(
            meta.column_index().is_none() && meta.offset_index().is_none(),
            "no-predicate reader must return footer-only metadata"
        );
    }
}
