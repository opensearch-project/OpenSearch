/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::parquet_page_cache::is_scoped_page_index_enabled;
use datafusion::common::TableReference;
use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
use datafusion::execution::cache::cache_manager::CachedFileMetadataEntry;
use datafusion::execution::cache::default_cache::DefaultCache;
use datafusion::execution::cache::{Cache, CacheEntryInfo};
use datafusion::parquet::file::metadata::ParquetMetaData;
use object_store::path::Path;

// Cache type constants
pub const CACHE_TYPE_METADATA: &str = "METADATA";
pub const CACHE_TYPE_STATS: &str = "STATISTICS";
pub const CACHE_TYPE_COLUMN_INDEX: &str = "COLUMN_INDEX";
pub const CACHE_TYPE_OFFSET_INDEX: &str = "OFFSET_INDEX";

/// Return a cache entry whose `ParquetMetaData` carries footer-only metadata (no
/// `ColumnIndex` / `OffsetIndex`). If the entry already lacks a page index — or
/// isn't a `CachedParquetMetaData` at all — it's returned unchanged (no clone, no
/// rebuild).
///
/// This is the single chokepoint that enforces the footer-only invariant: every
/// `put` runs the entry through here before it lands in the shared LRU.
fn strip_page_index(entry: CachedFileMetadataEntry) -> CachedFileMetadataEntry {
    let Some(cached) = entry
        .file_metadata
        .as_any()
        .downcast_ref::<CachedParquetMetaData>()
    else {
        return entry;
    };
    let meta = cached.parquet_metadata();
    if meta.column_index().is_none() && meta.offset_index().is_none() {
        // Already footer-only — keep the existing Arc, avoid a rebuild.
        return entry;
    }
    // Rebuild without the page index. The heavy decoded `ColumnIndex` /
    // `OffsetIndex` are released when the original Arc drops; the footer
    // (row-group + column chunk stats) is preserved.
    let stripped = ParquetMetaData::clone(meta)
        .into_builder()
        .set_column_index(None)
        .set_offset_index(None)
        .build();
    CachedFileMetadataEntry::new(
        entry.meta,
        Arc::new(CachedParquetMetaData::new(Arc::new(stripped))),
    )
}

// Wrapper around DataFusion's `DefaultCache` that adds hit/miss counters and
// enforces the footer-only invariant (`strip_page_index`) on every `put`.
//
// DF55 collapsed the DF54 `CacheAccessor` + `FileMetadataCache` traits into a
// single `Cache<Path, CachedFileMetadataEntry>` trait. `DefaultCache` is already
// internally synchronized (its methods take `&self`), so the previous outer
// `Mutex` wrapper is redundant and has been dropped.
pub struct MutexFileMetadataCache {
    pub inner: DefaultCache<Path, CachedFileMetadataEntry>,
    hit_count: AtomicUsize,
    miss_count: AtomicUsize,
}

impl MutexFileMetadataCache {
    pub fn new(cache: DefaultCache<Path, CachedFileMetadataEntry>) -> Self {
        Self {
            inner: cache,
            hit_count: AtomicUsize::new(0),
            miss_count: AtomicUsize::new(0),
        }
    }

    pub fn hit_count(&self) -> usize {
        self.hit_count.load(Ordering::Relaxed)
    }

    pub fn miss_count(&self) -> usize {
        self.miss_count.load(Ordering::Relaxed)
    }

    pub fn reset_stats(&self) {
        self.hit_count.store(0, Ordering::Relaxed);
        self.miss_count.store(0, Ordering::Relaxed);
    }

    pub fn clear_cache(&self) {
        self.inner.clear();
    }

    pub fn get_cache_limit(&self) -> usize {
        self.inner.cache_limit()
    }
}

impl Cache<Path, CachedFileMetadataEntry> for MutexFileMetadataCache {
    fn get(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        let result = self.inner.get(k);
        if result.is_some() {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.miss_count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn put(&self, k: &Path, v: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        // When scoped page-index is enabled: strip ColumnIndex + OffsetIndex from
        // the entry before caching. The level-1 cache stays footer-only; page-level
        // pruning is handled by the scoped cache (`parquet_page_cache`).
        //
        // When scoped page-index is disabled (fallback mode): retain the full entry
        // including page indexes so DataFusion's default page pruning path continues
        // to function. In this mode `load_parquet_metadata` fetches with
        // `PageIndexPolicy::ReadAll`, so the full index is present to retain.
        let v = if is_scoped_page_index_enabled() {
            strip_page_index(v)
        } else {
            v
        };
        self.inner.put(k, v)
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        self.inner.remove(k)
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.inner.contains_key(k)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn clear(&self) {
        self.inner.clear();
    }

    fn name(&self) -> String {
        self.inner.name()
    }

    fn cache_limit(&self) -> usize {
        self.inner.cache_limit()
    }

    fn update_cache_limit(&self, limit: usize) {
        self.inner.update_cache_limit(limit);
    }

    fn cache_ttl(&self) -> Option<Duration> {
        self.inner.cache_ttl()
    }

    fn update_cache_ttl(&self, ttl: Option<Duration>) {
        self.inner.update_cache_ttl(ttl);
    }

    fn drop_table_entries(&self, table_ref: &TableReference) -> datafusion::common::Result<()> {
        self.inner.drop_table_entries(table_ref)
    }

    fn list_entries(&self) -> datafusion::common::HashMap<Path, CacheEntryInfo<CachedFileMetadataEntry>> {
        self.inner.list_entries()
    }
}

#[cfg(test)]
mod strip_page_index_tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::{EnabledStatistics, WriterProperties};
    use object_store::ObjectMeta;
    use prost::bytes::Bytes;

    fn parquet_with_page_index() -> Bytes {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..4096i64).collect::<Vec<_>>()))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(128)
            .build();
        let mut buf: Vec<u8> = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        Bytes::from(buf)
    }

    fn object_meta(bytes: &Bytes) -> ObjectMeta {
        ObjectMeta {
            location: Path::from("data.parquet"),
            last_modified: chrono::Utc::now(),
            size: bytes.len() as u64,
            e_tag: None,
            version: None,
        }
    }

    fn full_index_entry(bytes: &Bytes) -> CachedFileMetadataEntry {
        let meta = ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .unwrap();
        let pq = meta.metadata().clone();
        assert!(pq.column_index().is_some() && pq.offset_index().is_some());
        CachedFileMetadataEntry::new(object_meta(bytes), Arc::new(CachedParquetMetaData::new(pq)))
    }

    fn page_index_present(entry: &CachedFileMetadataEntry) -> bool {
        let cached = entry
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .unwrap();
        let m = cached.parquet_metadata();
        m.column_index().is_some() || m.offset_index().is_some()
    }

    #[test]
    fn put_strips_page_index_and_get_returns_footer_only() {
        let bytes = parquet_with_page_index();
        let entry = full_index_entry(&bytes);
        assert!(
            page_index_present(&entry),
            "precondition: entry has page index"
        );

        let cache = MutexFileMetadataCache::new(DefaultCache::new(64 * 1024 * 1024));
        let key = Path::from("data.parquet");
        cache.put(&key, entry);

        let got = cache.get(&key).expect("entry must be retrievable");
        assert!(
            !page_index_present(&got),
            "cached entry must be footer-only after put"
        );
        let cached = got
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .unwrap();
        let m = cached.parquet_metadata();
        assert!(m.num_row_groups() > 0);
        assert!(
            m.row_group(0).column(0).statistics().is_some(),
            "footer stats must survive"
        );
    }

    #[test]
    fn strip_is_noop_for_footer_only_entry() {
        let bytes = parquet_with_page_index();
        let meta = ArrowReaderMetadata::load(
            &bytes.clone(),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .unwrap();
        let pq = meta.metadata().clone();
        assert!(pq.column_index().is_none() && pq.offset_index().is_none());
        let entry = CachedFileMetadataEntry::new(
            object_meta(&bytes),
            Arc::new(CachedParquetMetaData::new(Arc::clone(&pq))),
        );
        let stripped = strip_page_index(entry);
        let cached = stripped
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .unwrap();
        assert!(
            Arc::ptr_eq(cached.parquet_metadata(), &pq),
            "footer-only entry must be returned unchanged (same Arc)"
        );
    }
}
