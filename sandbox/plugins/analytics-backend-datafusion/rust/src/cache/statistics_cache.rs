/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use crate::eviction_policy::{CacheResult, PolicyType};
use crate::cache::foyer_backed_cache::FoyerBackedCache;
use arrow_array::Array;
use datafusion::common::stats::{ColumnStatistics, Precision};
use datafusion::common::ScalarValue;
use datafusion::execution::cache::CacheAccessor;
use datafusion::execution::cache::cache_manager::{CachedFileMetadata, FileStatisticsCache, FileStatisticsCacheEntry};
use datafusion::execution::cache::TableScopedPath;
use datafusion::common::TableReference;
use datafusion::physical_plan::Statistics;
use object_store::{path::Path, ObjectMeta};
use std::sync::Arc;

use std::fs::File;
use arrow_schema::SchemaRef;
use parquet::file::metadata::ParquetMetaData;

/// Trait to calculate heap memory size for statistics objects
trait HeapSize {
    fn heap_size(&self) -> usize;
}

impl HeapSize for Statistics {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.num_rows.heap_size()
            + self.total_byte_size.heap_size()
            + self.column_statistics.heap_size()
    }
}

impl<T: HeapSize + std::fmt::Debug + Clone + PartialEq + Eq + PartialOrd> HeapSize
for Precision<T>
{
    fn heap_size(&self) -> usize {
        match self {
            Precision::Exact(val) => std::mem::size_of::<Self>() + val.heap_size(),
            Precision::Inexact(val) => std::mem::size_of::<Self>() + val.heap_size(),
            Precision::Absent => std::mem::size_of::<Self>(),
        }
    }
}

impl HeapSize for usize {
    fn heap_size(&self) -> usize {
        0 // Primitive types don't have heap allocation
    }
}

impl<T: HeapSize> HeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + (self.capacity() * std::mem::size_of::<T>())
            + self.iter().map(|item| item.heap_size()).sum::<usize>()
    }
}

impl HeapSize for ColumnStatistics {
    fn heap_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.null_count.heap_size()
            + self.max_value.heap_size()
            + self.min_value.heap_size()
            + self.distinct_count.heap_size()
    }
}

impl HeapSize for ScalarValue {
    fn heap_size(&self) -> usize {
        match self {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                std::mem::size_of::<Self>() + s.capacity()
            }
            ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => {
                std::mem::size_of::<Self>() + b.capacity()
            }
            ScalarValue::List(arr) => {
                // Estimate list array memory size
                std::mem::size_of::<Self>() + std::mem::size_of_val(arr.as_ref()) + (arr.len() * 8)
            }
            ScalarValue::Struct(arr) => {
                // Estimate struct array memory size
                std::mem::size_of::<Self>() + std::mem::size_of_val(arr.as_ref()) + (arr.len() * 16)
            }
            _ => std::mem::size_of::<Self>(), // Primitive types and nulls
        }
    }
}

/// Extension trait to add memory_size method to Statistics
trait StatisticsMemorySize {
    fn memory_size(&self) -> usize;
}

impl StatisticsMemorySize for Statistics {
    fn memory_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.num_rows.heap_size()
            + self.total_byte_size.heap_size()
            + self.column_statistics.heap_size()
    }
}

/// Memory-tracking statistics cache, now backed by a byte-bounded foyer cache.
///
/// Previously a hand-rolled `DashMap + Mutex<MemoryState> + Box<dyn CachePolicy>`; now a thin
/// layer over [`FoyerBackedCache`] keyed by `Path`. Byte cost per entry is the cached
/// `Statistics::memory_size()`, so the configured size limit keeps its byte semantics and foyer
/// enforces it at the hard cap (the prior soft 0.8/0.6 threshold knobs are gone — foyer evicts
/// exactly at the limit). Policy is fixed at construction (LRU/LFU); runtime policy swap and
/// manual `evict` were test-only and have been dropped.
pub struct CustomStatisticsCache {
    inner: FoyerBackedCache<Path, CachedFileMetadata>,
}

impl CustomStatisticsCache {
    /// Create a new custom statistics cache. `policy_type` selects LRU or LFU; the
    /// `_eviction_threshold` is accepted for call-site compatibility but unused (foyer evicts at
    /// the hard byte cap).
    pub fn new(policy_type: PolicyType, size_limit: usize, _eviction_threshold: f64) -> Self {
        Self {
            inner: FoyerBackedCache::new(size_limit, policy_type, |_k, v: &CachedFileMetadata| {
                v.statistics.memory_size()
            }),
        }
    }

    /// Create with default configuration
    pub fn with_default_config() -> Self {
        Self::new(PolicyType::Lru, 100 * 1024 * 1024, 0.8) // 100MB default
    }

    /// Get total memory consumed by all cached statistics
    pub fn memory_consumed(&self) -> usize {
        self.inner.used_bytes()
    }

    /// Get cache hit count
    pub fn hit_count(&self) -> usize {
        self.inner.stats().hits as usize
    }

    /// Get cache miss count
    pub fn miss_count(&self) -> usize {
        self.inner.stats().misses as usize
    }

    /// Get cache hit rate (returns value between 0.0 and 1.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hit_count();
        let misses = self.miss_count();
        let total = hits + misses;
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }

    /// Reset hit and miss counters
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

    /// Update the cache size limit (foyer evicts down to the new cap as needed).
    pub fn update_size_limit(&self, new_limit: usize) -> CacheResult<()> {
        self.inner.set_limit(new_limit);
        Ok(())
    }

    /// Get current cache size (actual memory consumption).
    pub fn current_size(&self) -> CacheResult<usize> {
        Ok(self.memory_consumed())
    }

    /// Get current size limit in bytes (configured cap, not utilization)
    pub fn current_size_limit(&self) -> usize {
        self.inner.stats().limit_bytes
    }

    /// Convenience method: put statistics with associated metadata.
    pub fn put_statistics(
        &self,
        k: &Path,
        stats: Arc<Statistics>,
        meta: &ObjectMeta,
    ) -> Option<CachedFileMetadata> {
        let cached = CachedFileMetadata::new(meta.clone(), stats, None);
        self.put(k, cached)
    }

    /// Convenience method: get just the statistics Arc (for callers that don't need full CachedFileMetadata)
    pub fn get_statistics(&self, k: &Path) -> Option<Arc<Statistics>> {
        self.get(k).map(|c| c.statistics)
    }
}

// Path-keyed core operations. DF54 keys the FileStatisticsCache by
// `TableScopedPath`; the convenience methods and tests in this module still use
// bare `Path`, so the storage and bookkeeping stay Path-keyed and the
// CacheAccessor<TableScopedPath> impl below delegates to these via `&key.path`.
// These are inherent methods: for a `&Path` argument they take priority over the
// trait's `&TableScopedPath` methods, so existing callers keep working unchanged.
impl CustomStatisticsCache {
    pub fn get(&self, k: &Path) -> Option<CachedFileMetadata> {
        // hit/miss are tracked inside FoyerBackedCache::get.
        self.inner.get(k)
    }

    pub fn put(&self, k: &Path, v: CachedFileMetadata) -> Option<CachedFileMetadata> {
        // foyer doesn't surface the displaced entry, and no caller inspects it.
        self.inner.insert(k.clone(), v);
        None
    }

    pub fn remove(&self, k: &Path) -> Option<CachedFileMetadata> {
        self.inner.remove(k)
    }

    pub fn contains_key(&self, k: &Path) -> bool {
        self.inner.contains(k)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn clear(&self) {
        self.inner.clear();
        self.reset_stats();
    }

    pub fn name(&self) -> String {
        "CustomStatisticsCache(foyer)".to_string()
    }
}

// DF54 `FileStatisticsCache: CacheAccessor<TableScopedPath, CachedFileMetadata>`.
// Storage stays Path-keyed (see inherent methods above); this delegates via
// `&key.path`. The table scope is not used by this cache.
impl CacheAccessor<TableScopedPath, CachedFileMetadata> for CustomStatisticsCache {
    fn get(&self, k: &TableScopedPath) -> Option<CachedFileMetadata> {
        CustomStatisticsCache::get(self, &k.path)
    }

    fn put(&self, k: &TableScopedPath, v: CachedFileMetadata) -> Option<CachedFileMetadata> {
        CustomStatisticsCache::put(self, &k.path, v)
    }

    fn remove(&self, k: &TableScopedPath) -> Option<CachedFileMetadata> {
        CustomStatisticsCache::remove(self, &k.path)
    }

    fn contains_key(&self, k: &TableScopedPath) -> bool {
        CustomStatisticsCache::contains_key(self, &k.path)
    }

    fn len(&self) -> usize {
        CustomStatisticsCache::len(self)
    }

    fn clear(&self) {
        CustomStatisticsCache::clear(self)
    }

    fn name(&self) -> String {
        CustomStatisticsCache::name(self)
    }
}

impl FileStatisticsCache for CustomStatisticsCache {
    fn cache_limit(&self) -> usize {
        self.current_size_limit()
    }

    fn update_cache_limit(&self, limit: usize) {
        // Best-effort: update_size_limit also triggers eviction; ignore its Result
        // since the trait method is infallible.
        let _ = self.update_size_limit(limit);
    }

    fn list_entries(&self) -> std::collections::HashMap<TableScopedPath, FileStatisticsCacheEntry> {
        std::collections::HashMap::new()
    }

    fn drop_table_entries(
        &self,
        _table_ref: &Option<TableReference>,
    ) -> datafusion::common::Result<()> {
        // This cache does not track table scope, so there are no per-table
        // entries to drop. No-op.
        Ok(())
    }
}

impl Default for CustomStatisticsCache {
    fn default() -> Self {
        Self::with_default_config()
    }
}

/// Compute statistics from an already-loaded `ParquetMetaData` and schema.
///
/// Avoids a second file/IO round-trip when the footer has already been fetched
/// (e.g. by `load_parquet_metadata` during metadata cache warming). The caller
/// is responsible for providing the correct Arrow schema derived from the same
/// metadata.
pub fn compute_parquet_statistics_from_metadata(
    metadata: &ParquetMetaData,
    schema: &SchemaRef,
) -> Result<Statistics, Box<dyn std::error::Error>> {
    use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
    let statistics = DFParquetMetadata::statistics_from_parquet_metadata(metadata, schema)?;
    Ok(statistics)
}

/// Compute statistics from a parquet file using DataFusion's built-in functionality
pub fn compute_parquet_statistics(file_path: &str) -> Result<Statistics, Box<dyn std::error::Error>> {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    let file = File::open(file_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();
    let schema = builder.schema().clone();

    // Create ObjectStore and ObjectMeta for the file
    let _store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
    let path = Path::from(file_path);
    let file_metadata = std::fs::metadata(file_path)?;
    let _object_meta = ObjectMeta {
        location: path,
        last_modified: chrono::DateTime::from(file_metadata.modified()?),
        size: file_metadata.len(),
        e_tag: None,
        version: None,
    };

    // Use DataFusion's method to extract statistics from parquet metadata
    // statistics_from_parquet_metadata is an associated function that takes metadata and schema
    let statistics = DFParquetMetadata::statistics_from_parquet_metadata(metadata, &schema)?;
    Ok(statistics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use datafusion::common::stats::Precision;

    fn create_test_statistics() -> Statistics {
        Statistics {
            num_rows: Precision::Exact(1000),
            total_byte_size: Precision::Exact(50000),
            column_statistics: vec![],
        }
    }

    fn create_test_path(name: &str) -> Path {
        Path::from(format!("/test/{}.parquet", name))
    }

    fn create_test_meta(path: &Path) -> ObjectMeta {
        ObjectMeta {
            location: path.clone(),
            last_modified: Utc::now(),
            size: 1000,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn test_custom_stats_cache_creation() {
        let cache = CustomStatisticsCache::new(PolicyType::Lru, 1024 * 1024, 0.8);
        assert_eq!(cache.name(), "CustomStatisticsCache(foyer)");
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_memory_tracking_with_policy() {
        let cache = CustomStatisticsCache::with_default_config();
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);

        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());
        cache.put_statistics(&path, stats, &meta);

        assert!(cache.memory_consumed() > 0);
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&path).is_some());
    }

    #[test]
    fn test_policy_based_eviction_with_memory() {
        let cache = CustomStatisticsCache::new(PolicyType::Lru, 1000, 0.8);
        for i in 0..10 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_statistics(&path, stats, &meta);
        }
        assert!(cache.memory_consumed() <= 1000);
        assert!(cache.len() > 0);
    }

    #[test]
    fn test_byte_bounded_eviction() {
        // Small cap: inserting many entries must keep memory at/under the limit (foyer evicts
        // at the hard byte cap; the prior manual `evict`/policy-swap knobs are gone).
        let cache = CustomStatisticsCache::new(PolicyType::Lru, 2000, 0.8);
        for i in 0..50 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_statistics(&path, stats, &meta);
        }
        assert!(cache.memory_consumed() <= 2000, "memory={} > cap", cache.memory_consumed());
        assert!(cache.len() > 0);
    }

    #[test]
    fn test_remove_with_memory_tracking() {
        let cache = CustomStatisticsCache::with_default_config();
        let path1 = create_test_path("file1");
        let path2 = create_test_path("file2");
        let meta1 = create_test_meta(&path1);
        let meta2 = create_test_meta(&path2);
        let stats = Arc::new(create_test_statistics());

        cache.put_statistics(&path1, stats.clone(), &meta1);
        cache.put_statistics(&path2, stats, &meta2);
        let memory_with_two = cache.memory_consumed();
        assert_eq!(cache.len(), 2);

        cache.remove(&path1);
        assert_eq!(cache.len(), 1);
        assert!(cache.memory_consumed() < memory_with_two);

        cache.remove(&path2);
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_clear_with_memory_tracking() {
        let cache = CustomStatisticsCache::with_default_config();
        for i in 0..3 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_statistics(&path, stats, &meta);
        }
        assert!(cache.memory_consumed() > 0);
        cache.clear();
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_hit_count_tracking() {
        let cache = CustomStatisticsCache::with_default_config();
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());
        cache.put_statistics(&path, stats, &meta);

        assert!(cache.get(&path).is_some());
        assert_eq!(cache.hit_count(), 1);
        assert!(cache.get(&path).is_some());
        assert_eq!(cache.hit_count(), 2);
        assert_eq!(cache.miss_count(), 0);
    }

    #[test]
    fn test_miss_count_tracking() {
        let cache = CustomStatisticsCache::with_default_config();
        let path = create_test_path("nonexistent");
        assert!(cache.get(&path).is_none());
        assert_eq!(cache.miss_count(), 1);
        assert!(cache.get(&path).is_none());
        assert_eq!(cache.miss_count(), 2);
        assert_eq!(cache.hit_count(), 0);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let cache = CustomStatisticsCache::with_default_config();
        assert_eq!(cache.hit_rate(), 0.0);

        let path1 = create_test_path("file1");
        let meta1 = create_test_meta(&path1);
        let stats = Arc::new(create_test_statistics());
        cache.put_statistics(&path1, stats, &meta1);

        cache.get(&path1); // hit
        cache.get(&path1); // hit
        assert_eq!(cache.hit_rate(), 1.0);

        let path2 = create_test_path("missing");
        cache.get(&path2); // miss
        assert!((cache.hit_rate() - 0.6666666666666666).abs() < 0.0001);
    }

    #[test]
    fn test_reset_stats() {
        let cache = CustomStatisticsCache::with_default_config();
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());
        cache.put_statistics(&path, stats, &meta);

        cache.get(&path);
        let path2 = create_test_path("missing");
        cache.get(&path2);
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 1);

        cache.reset_stats();
        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 0);
        assert_eq!(cache.len(), 1); // entries still exist
    }

    #[test]
    fn test_clear_resets_stats() {
        let cache = CustomStatisticsCache::with_default_config();
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());
        cache.put_statistics(&path, stats, &meta);
        cache.get(&path);

        cache.clear();
        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_concurrent_operations() {
        use std::thread;
        let cache = Arc::new(CustomStatisticsCache::with_default_config());
        let mut handles = vec![];

        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let path = create_test_path(&format!("concurrent{}", i));
                let meta = create_test_meta(&path);
                let stats = Arc::new(create_test_statistics());
                cache_clone.put_statistics(&path, stats, &meta);
                assert!(cache_clone.get(&path).is_some());
            });
            handles.push(handle);
        }

        for handle in handles { handle.join().unwrap(); }
        assert!(cache.len() > 0);
    }
}
