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
//! The listing-table path (`ShardTableProvider` / vanilla `ListingTable`) uses
//! DataFusion's default reader factory, so when page pruning is enabled the
//! `ParquetOpener` loads the **entire** page index (`ColumnIndex` + `OffsetIndex`
//! for *every* column) of each surviving file, every query, and caches none of
//! it. On wide schemas the `ColumnIndex` (per-page string min/max) dominates the
//! native heap.
//!
//! This factory closes that gap using the unified scoped cache
//! ([`crate::cache::page_index`]). The seam is DataFusion's
//! [`ParquetFileReaderFactory`]: the `ParquetOpener` asks the reader for metadata
//! via `get_metadata`, and — per `opener::load_page_index` — if the returned
//! `ParquetMetaData` *already* carries a page index, the opener uses it and skips
//! the full, all-column load. So our reader's `get_metadata`:
//!
//!   1. loads footer-only metadata (shared metadata-cache hit — see
//!      [`crate::indexed_table::parquet_bridge::load_parquet_metadata`]), then
//!   2. augments it with a page index scoped to the predicate columns via
//!      [`crate::cache::page_index::load_scoped_page_index`]
//!      (real `ColumnIndex` for predicate columns, real `OffsetIndex` for all
//!      columns), and
//!   3. returns that augmented metadata.
//!
//! The scoped `(file, predicate-columns)` cache is shared with the indexed path,
//! so repeated queries reuse the decoded index across both scan paths.
//!
//! # Why all row groups (no RG scoping here)
//!
//! `ScopedPageIndexOptimizer` only swaps the reader factory; DataFusion still
//! selects which row groups to scan via its OWN RG-statistics pruning, and its
//! page-pruner + reader then dereference `column_index[rg][col]` /
//! `offset_index[rg][col]` for *its* chosen RGs — a set independent of anything
//! we could compute here. Leaving a placeholder entry on an RG DataFusion still
//! touches panics (`page_row_counts.first().unwrap()` on an empty `OffsetIndex`),
//! and its page-index gate is per-FILE (both indexes must be `Some`), so a
//! partial page index would lie to it. So the page index is built for ALL row
//! groups, column-scoped only — heap stays bounded because the heavy
//! `ColumnIndex` is scoped to predicate columns and only the cheap all-column
//! `OffsetIndex` spans every RG (and that is required for correctness at read
//! time anyway).
//!
//! # Fallback
//!
//! If there are no predicate columns, or scoped augmentation fails for a file
//! (no page index, decode/IO error), `get_metadata` returns the footer-only
//! metadata and the opener loads the page index on demand exactly as today —
//! correct, just without the scoping benefit for that file. Never a wrong result.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::parquet::{
    ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::parquet::arrow::arrow_reader::ArrowReaderOptions;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::errors::ParquetError;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_datasource::PartitionedFile;
use futures::future::BoxFuture;
use futures::FutureExt;
use object_store::{ObjectStore, ObjectStoreExt};
use prost::bytes::Bytes;

use crate::cache::page_index::{
    load_scoped_page_index_cols, resolve_predicate_parquet_columns_pair,
};
use crate::indexed_table::parquet_bridge::load_parquet_metadata_with_meta;

/// A [`ParquetFileReaderFactory`] that, on `get_metadata`, returns metadata whose
/// page index is scoped to the query's predicate columns. Data reads go straight
/// to the object store.
///
/// Carries predicate column *names* + the file schema rather than pre-resolved
/// parquet indices: the reader resolves names → parquet leaf indices per file via
/// the same `resolve_predicate_parquet_columns` the indexed path uses, robust to
/// schema evolution across files (a column absent from one file is just skipped).
#[derive(Debug)]
pub struct ScopedPageIndexReaderFactory {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    /// File-column names referenced by the query predicate. Empty means "no
    /// scoping" — `get_metadata` returns footer-only and the opener loads the
    /// page index on demand as usual.
    predicate_column_names: Arc<Vec<String>>,
    /// File-column names this scan PROJECTS (reads). Used to scope the
    /// OffsetIndex to `predicate ∪ projection` instead of all columns. Empty =
    /// fall back to all-column offsets (old behavior).
    projection_column_names: Arc<Vec<String>>,
    /// The physical predicate (if any). Retained in the constructor signature for
    /// parity with the indexed path, but intentionally NOT used for RG scoping
    /// here (see module docs).
    #[allow(dead_code)]
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// File schema (no partition columns), for per-file column resolution.
    file_schema: SchemaRef,
}

impl ScopedPageIndexReaderFactory {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        metadata_cache: Arc<dyn FileMetadataCache>,
        predicate_column_names: Vec<String>,
        projection_column_names: Vec<String>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        file_schema: SchemaRef,
    ) -> Self {
        Self {
            store,
            metadata_cache,
            predicate_column_names: Arc::new(predicate_column_names),
            projection_column_names: Arc::new(projection_column_names),
            predicate,
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
            projection_column_names: Arc::clone(&self.projection_column_names),
            file_schema: Arc::clone(&self.file_schema),
            location: file.object_meta.location.clone(),
            // Carry the listing's ObjectMeta so `get_metadata` resolves the footer
            // from cache without a per-read `head()` syscall (see
            // `load_parquet_metadata_with_meta`).
            object_meta: file.object_meta.clone(),
            metrics: file_metrics,
        }))
    }
}

struct ScopedPageIndexReader {
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<dyn FileMetadataCache>,
    predicate_column_names: Arc<Vec<String>>,
    projection_column_names: Arc<Vec<String>>,
    file_schema: SchemaRef,
    location: object_store::path::Path,
    /// Listing-snapshot metadata (size + last_modified) for `location`. Used to
    /// resolve footer metadata from cache without a `head()` syscall per read.
    object_meta: object_store::ObjectMeta,
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
        // IO-runtime dispatch is handled by the store wrapper around the
        // registered store, so a plain `.await` already runs on the IO runtime.
        async move {
            store
                .get_range(&location, range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
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
                .map_err(|e| ParquetError::External(Box::new(e)))
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
        let projection_names = Arc::clone(&self.projection_column_names);
        let file_schema = Arc::clone(&self.file_schema);
        let location = self.location.clone();
        let object_meta = self.object_meta.clone();
        async move {
            // 1. Footer-only metadata (shared metadata-cache hit if pre-seeded).
            //    Use the listing's ObjectMeta rather than a `head()` — the cache
            //    validity check only needs size + last_modified, both already known.
            let (_schema, _size, footer) = load_parquet_metadata_with_meta(
                Arc::clone(&store),
                &location,
                object_meta,
                Arc::clone(&metadata_cache),
            )
            .await
            .map_err(|e| ParquetError::General(format!("footer metadata {location}: {e}")))?;

            // 2. Resolve predicate + projection names → parquet leaf indices, then
            //    augment with a column-scoped page index. Gated on either being
            //    non-empty: a projection-only query still needs a scoped OffsetIndex.
            if !predicate_names.is_empty() || !projection_names.is_empty() {
                let (parquet_cols, offset_cols) = resolve_predicate_parquet_columns_pair(
                    &file_schema,
                    &footer,
                    &predicate_names,
                    &projection_names,
                );
                if let Some(augmented) = load_scoped_page_index_cols(
                    &store,
                    &location,
                    &footer,
                    &parquet_cols,
                    &offset_cols,
                )
                .await
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
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::page_index::column_index::ColumnIndexMetaData;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjPath;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    // Shared crate-wide guard so all users of the one process-global scoped cache
    // mutually exclude.
    use crate::cache::page_index::SCOPED_CACHE_TEST_GUARD as SCOPED_TEST_GUARD;

    /// Two int columns (`price`, `qty`), one row group, four 8-row data pages.
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

    async fn stage(bytes: Bytes) -> (Arc<dyn ObjectStore>, ObjPath, u64) {
        let size = bytes.len() as u64;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let loc = ObjPath::from("data.parquet");
        store
            .put(&loc, PutPayload::from_bytes(bytes))
            .await
            .unwrap();
        (store, loc, size)
    }

    fn fresh_cache() -> Arc<dyn FileMetadataCache> {
        Arc::new(crate::cache::MutexFileMetadataCache::new(
            datafusion::execution::cache::DefaultFilesMetadataCache::new(64 * 1024 * 1024),
        ))
    }

    fn metrics() -> ExecutionPlanMetricsSet {
        ExecutionPlanMetricsSet::new()
    }

    /// The factory's reader must, on `get_metadata`, return metadata whose page
    /// index is scoped to the predicate column (`price`) — real ColumnIndex for
    /// `price`, NONE placeholder for `qty` — while keeping a REAL OffsetIndex for
    /// BOTH columns. Also fills the shared scoped cache.
    #[tokio::test]
    async fn get_metadata_returns_scoped_page_index() {
        let _g = SCOPED_TEST_GUARD.lock().unwrap();
        crate::cache::page_index::clear_scoped_cache_for_test();

        let (bytes, schema) = two_col_parquet();
        let (store, loc, size) = stage(bytes).await;
        let factory = ScopedPageIndexReaderFactory::new(
            Arc::clone(&store),
            fresh_cache(),
            vec!["price".to_string()],
            // Project both columns so the OffsetIndex is built for both (this test
            // asserts a real OffsetIndex for every column).
            vec!["price".to_string(), "qty".to_string()],
            None,
            schema,
        );
        let pf = PartitionedFile::new(loc.as_ref().to_string(), size);
        let m = metrics();
        let mut reader = factory.create_reader(0, pf, None, &m).unwrap();

        let meta = reader.get_metadata(None).await.unwrap();
        let ci = meta
            .column_index()
            .expect("augmented metadata has column index");
        let oi = meta
            .offset_index()
            .expect("augmented metadata has offset index");
        assert!(
            !matches!(ci[0][0], ColumnIndexMetaData::NONE),
            "predicate col (price) must have a real ColumnIndex"
        );
        assert!(
            matches!(ci[0][1], ColumnIndexMetaData::NONE),
            "non-predicate col (qty) ColumnIndex must be a NONE placeholder"
        );
        assert!(
            !oi[0][0].page_locations().is_empty() && !oi[0][1].page_locations().is_empty(),
            "OffsetIndex must be real for every column"
        );

        let stats = crate::cache::page_index::scoped_cache_stats();
        assert!(stats.entries >= 1 && stats.misses >= 1 && stats.used_bytes > 0);

        crate::cache::page_index::clear_scoped_cache_for_test();
    }

    /// No predicate columns → no scoping happens: `get_metadata` returns the
    /// footer load as-is and the scoped cache is never touched.
    ///
    /// Note: we deliberately do NOT assert the returned metadata has no page
    /// index. Until the base metadata-cache strip lands (Step 1e), the shared
    /// `load_parquet_metadata` still loads the full page index when a metadata
    /// cache is present (DataFusion's `PageIndexPolicy::Optional`). The invariant
    /// this reader guarantees with no predicate is "no scoping", i.e. the scoped
    /// cache stays empty — which holds before and after 1e.
    #[tokio::test]
    async fn get_metadata_no_predicate_does_not_scope() {
        let _g = SCOPED_TEST_GUARD.lock().unwrap();
        crate::cache::page_index::clear_scoped_cache_for_test();

        let (bytes, schema) = two_col_parquet();
        let (store, loc, size) = stage(bytes).await;
        let factory = ScopedPageIndexReaderFactory::new(
            Arc::clone(&store),
            fresh_cache(),
            vec![],
            vec![],
            None,
            schema,
        );
        let pf = PartitionedFile::new(loc.as_ref().to_string(), size);
        let m = metrics();
        let mut reader = factory.create_reader(0, pf, None, &m).unwrap();

        let _meta = reader.get_metadata(None).await.unwrap();
        let stats = crate::cache::page_index::scoped_cache_stats();
        assert_eq!(
            (stats.entries, stats.misses, stats.hits),
            (0, 0, 0),
            "no predicate → scoped cache must be untouched"
        );

        crate::cache::page_index::clear_scoped_cache_for_test();
    }
}
