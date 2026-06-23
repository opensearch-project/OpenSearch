/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;

use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
use datafusion::execution::cache::cache_manager::{
    CachedFileMetadataEntry, FileMetadataCache, FileMetadataCacheEntry,
};
use datafusion::execution::cache::CacheAccessor;
use datafusion::parquet::file::metadata::ParquetMetaData;
use object_store::path::Path;
use crate::cache::eviction_policy::CacheEvictionPolicy;
use crate::cache::foyer_backed_cache::FoyerBackedCache;
use crate::parquet_page_cache::is_scoped_page_index_enabled;

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

/// Foyer-backed footer-metadata cache. Replaces the previous
/// `Mutex<DefaultFilesMetadataCache>` (hardcoded LRU) with a byte-bounded, sharded foyer cache
/// so the metadata cache shares the same eviction implementation as the statistics and
/// page-index caches. The name is retained for call-site compatibility.
///
/// Keyed by object-store `Path`; the byte weight of each entry is its
/// `FileMetadata::memory_size()` (the same accounting DataFusion's own metadata cache uses), so
/// the configured `datafusion.metadata.cache.size.limit` keeps its byte semantics. The
/// footer-only invariant is still enforced on every `put` via [`strip_page_index`].
pub struct MutexFileMetadataCache {
    inner: FoyerBackedCache<Path, CachedFileMetadataEntry>,
}

impl MutexFileMetadataCache {
    /// Build with an explicit byte limit and eviction policy.
    pub fn with_policy(size_limit: usize, policy: CacheEvictionPolicy) -> Self {
        Self {
            inner: FoyerBackedCache::new(size_limit, policy, |_k, v: &CachedFileMetadataEntry| {
                v.file_metadata.memory_size()
            }),
        }
    }

    /// Build with an explicit byte limit and the default eviction policy (S3-FIFO).
    pub fn with_limit(size_limit: usize) -> Self {
        Self::with_policy(size_limit, CacheEvictionPolicy::S3Fifo)
    }

    pub fn hit_count(&self) -> usize {
        self.inner.stats().hits as usize
    }

    pub fn miss_count(&self) -> usize {
        self.inner.stats().misses as usize
    }

    pub fn reset_stats(&self) {
        self.inner.reset_stats();
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }

    /// Enable/disable at runtime. Disabling clears the cache to free native heap immediately.
    pub fn set_enabled(&self, enabled: bool) {
        self.inner.set_enabled(enabled);
    }

    pub fn clear_cache(&self) {
        self.inner.clear();
    }

    /// Current byte usage — exposes the foyer-tracked counter (replaces the old
    /// `inner.lock().memory_used()` the cache manager used).
    pub fn memory_used(&self) -> usize {
        self.inner.used_bytes()
    }

    pub fn update_cache_limit(&self, new_limit: usize) {
        self.inner.set_limit(new_limit);
    }

    pub fn get_cache_limit(&self) -> usize {
        self.inner.stats().limit_bytes
    }
}

impl CacheAccessor<Path, CachedFileMetadataEntry> for MutexFileMetadataCache {
    fn get(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        self.inner.get(k)
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
        // DataFusion's CacheAccessor::put returns the previous entry; foyer doesn't surface it,
        // and no caller inspects the return value, so we return None.
        self.inner.insert(k.clone(), v);
        None
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        self.inner.remove(k)
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.inner.contains(k)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn clear(&self) {
        self.inner.clear();
    }

    fn name(&self) -> String {
        "MutexFileMetadataCache(foyer-lru)".to_string()
    }
}

impl FileMetadataCache for MutexFileMetadataCache {
    fn cache_limit(&self) -> usize {
        self.inner.stats().limit_bytes
    }

    fn update_cache_limit(&self, limit: usize) {
        self.inner.set_limit(limit);
    }

    fn list_entries(&self) -> std::collections::HashMap<Path, FileMetadataCacheEntry> {
        // Entry enumeration is not supported by the foyer backend; callers (node-stats) use the
        // hit/miss/byte counters instead. Empty map matches the prior behavior for the
        // table-scope-less metadata cache.
        std::collections::HashMap::new()
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
        assert!(page_index_present(&entry), "precondition: entry has page index");

        let cache = MutexFileMetadataCache::with_limit(64 * 1024 * 1024);
        let key = Path::from("data.parquet");
        cache.put(&key, entry);

        let got = cache.get(&key).expect("entry must be retrievable");
        assert!(!page_index_present(&got), "cached entry must be footer-only after put");
        let cached = got
            .file_metadata
            .as_any()
            .downcast_ref::<CachedParquetMetaData>()
            .unwrap();
        let m = cached.parquet_metadata();
        assert!(m.num_row_groups() > 0);
        assert!(m.row_group(0).column(0).statistics().is_some(), "footer stats must survive");
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
