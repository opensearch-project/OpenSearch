/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use crate::eviction_policy::{
    create_policy, CacheError, CachePolicy, CacheResult, PolicyType,
};
use arrow_array::Array;
use datafusion::common::stats::{ColumnStatistics, Precision};
use datafusion::common::ScalarValue;
use dashmap::DashMap;
use datafusion::execution::cache::CacheAccessor;
use datafusion::physical_plan::Statistics;
use object_store::{path::Path, ObjectMeta};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use std::fs::File;

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

/// Combined memory tracking and policy-based eviction cache
///
/// This cache leverages DashMap's built-in concurrency from DefaultFileStatisticsCache
/// and adds memory tracking + policy-based eviction on top.
pub struct CustomStatisticsCache {
    /// The underlying DataFusion statistics cache (DashMap-based, already thread-safe)
    inner_cache: DashMap<Path, (ObjectMeta, Arc<Statistics>)>,
    /// The eviction policy (thread-safe)
    policy: Arc<Mutex<Box<dyn CachePolicy>>>,
    /// Size limit for the cache in bytes
    size_limit: AtomicUsize,
    /// Eviction threshold (0.0 to 1.0)
    eviction_threshold: f64,
    /// Memory usage tracker - maps cache keys to their memory consumption (thread-safe)
    memory_tracker: Arc<Mutex<HashMap<String, usize>>>,
    /// Total memory consumed by all entries (thread-safe)
    total_memory: Arc<Mutex<usize>>,
    /// Cache hit count (thread-safe)
    hit_count: Arc<Mutex<usize>>,
    /// Cache miss count (thread-safe)
    miss_count: Arc<Mutex<usize>>,
}

impl CustomStatisticsCache {
    /// Create a new custom statistics cache
    pub fn new(policy_type: PolicyType, size_limit: usize, eviction_threshold: f64) -> Self {
        Self {
            inner_cache: DashMap::new(),
            policy: Arc::new(Mutex::new(create_policy(policy_type))),
            size_limit: AtomicUsize::new(size_limit),
            eviction_threshold,
            memory_tracker: Arc::new(Mutex::new(HashMap::new())),
            total_memory: Arc::new(Mutex::new(0)),
            hit_count: Arc::new(Mutex::new(0)),
            miss_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Create with default configuration
    pub fn with_default_config() -> Self {
        Self::new(PolicyType::Lru, 100 * 1024 * 1024, 0.8) // 100MB default
    }

    /// Get the underlying cache for compatibility
    pub fn inner(&self) -> &DashMap<Path, (ObjectMeta, Arc<Statistics>)> {
        &self.inner_cache
    }

    /// Get total memory consumed by all cached statistics
    pub fn memory_consumed(&self) -> usize {
        self.total_memory.lock().map(|guard| *guard).unwrap_or(0)
    }

    /// Get cache hit count
    pub fn hit_count(&self) -> usize {
        self.hit_count.lock().map(|guard| *guard).unwrap_or(0)
    }

    /// Get cache miss count
    pub fn miss_count(&self) -> usize {
        self.miss_count.lock().map(|guard| *guard).unwrap_or(0)
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
        if let Ok(mut hits) = self.hit_count.lock() { *hits = 0; }
        if let Ok(mut misses) = self.miss_count.lock() { *misses = 0; }
    }

    /// Update the cache size limit
    pub fn update_size_limit(&self, new_limit: usize) -> CacheResult<()> {
        self.size_limit.store(new_limit, Ordering::Relaxed);
        let current_size = self.current_size()?;
        if current_size > new_limit {
            let target_eviction = current_size - (new_limit as f64 * self.eviction_threshold) as usize;
            let candidates = {
                if let Ok(policy_guard) = self.policy.lock() {
                    policy_guard.select_for_eviction(target_eviction)
                } else { vec![] }
            };
            for candidate_key in candidates {
                if let Ok(path) = self.parse_key_to_path(&candidate_key) {
                    self.remove_internal(&path);
                }
            }
        }
        Ok(())
    }

    /// Switch to a different eviction policy
    pub fn set_policy(&self, policy_type: PolicyType) -> CacheResult<()> {
        let mut policy_guard = self.policy.lock().map_err(|e| CacheError::PolicyLockError {
            reason: format!("Failed to acquire policy lock: {}", e),
        })?;
        let mut new_policy = create_policy(policy_type);
        if let Ok(tracker) = self.memory_tracker.lock() {
            for (key, size) in tracker.iter() {
                new_policy.on_insert(key, *size);
            }
        }
        *policy_guard = new_policy;
        Ok(())
    }

    /// Get current policy name
    pub fn policy_name(&self) -> CacheResult<String> {
        let policy_guard = self.policy.lock().map_err(|e| CacheError::PolicyLockError {
            reason: format!("Failed to acquire policy lock: {}", e),
        })?;
        Ok(policy_guard.policy_name().to_string())
    }

    /// Get current cache size according to policy (uses actual memory consumption)
    pub fn current_size(&self) -> CacheResult<usize> {
        Ok(self.memory_consumed())
    }

    /// Manually trigger eviction (requires &mut self)
    pub fn evict(&mut self, target_size: usize) -> CacheResult<usize> {
        if target_size == 0 { return Ok(0); }

        let candidates = {
            let policy_guard = self.policy.lock().map_err(|e| CacheError::PolicyLockError {
                reason: format!("Failed to acquire policy lock: {}", e),
            })?;
            policy_guard.select_for_eviction(target_size)
        };

        let mut freed_size = 0;
        for key in candidates {
            let entry_size = if let Ok(tracker) = self.memory_tracker.lock() {
                tracker.get(&key).copied().unwrap_or(0)
            } else { 0 };

            if entry_size > 0 {
                if let Ok(path) = self.parse_key_to_path(&key) {
                    if self.inner_cache.remove(&path).is_some() {
                        if let Ok(mut tracker) = self.memory_tracker.lock() {
                            if let Ok(mut total) = self.total_memory.lock() {
                                tracker.remove(&key);
                                *total = total.saturating_sub(entry_size);
                            }
                        }
                        if let Ok(mut policy_guard) = self.policy.lock() {
                            policy_guard.on_remove(&key);
                        }
                        freed_size += entry_size;
                    }
                    if freed_size >= target_size { break; }
                }
            }
        }
        Ok(freed_size)
    }

    /// Parse cache key back to Path
    fn parse_key_to_path(&self, key: &str) -> CacheResult<Path> {
        Ok(Path::from(key))
    }

    /// Remove entry internally (works with &self since inner_cache is thread-safe)
    fn remove_internal(&self, k: &Path) -> Option<Arc<Statistics>> {
        let key = k.to_string();
        let result = self.inner_cache.remove(k);
        if result.is_some() {
            if let Ok(mut tracker) = self.memory_tracker.lock() {
                if let Ok(mut total) = self.total_memory.lock() {
                    if let Some(old_size) = tracker.remove(&key) {
                        *total = total.saturating_sub(old_size);
                    }
                }
            }
            if let Ok(mut policy_guard) = self.policy.lock() {
                policy_guard.on_remove(&key);
            }
        }
        result.map(|x| x.1 .1)
    }
}

// Implement CacheAccessor - DashMap handles concurrency, we just need to handle the &mut self requirement
impl CacheAccessor<Path, Arc<Statistics>> for CustomStatisticsCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &Path) -> Option<Arc<Statistics>> {
        let result = self.inner_cache.get(k);

        if result.is_some() {
            if let Ok(mut hits) = self.hit_count.lock() { *hits += 1; }
            let key = k.to_string();
            let memory_size = if let Ok(tracker) = self.memory_tracker.lock() {
                tracker.get(&key).copied().unwrap_or(0)
            } else { 0 };
            if let Ok(mut policy_guard) = self.policy.lock() {
                policy_guard.on_access(&key, memory_size);
            }
        } else {
            if let Ok(mut misses) = self.miss_count.lock() { *misses += 1; }
        }

        result.map(|s| Some(Arc::clone(&s.value().1))).unwrap_or(None)
    }

    fn get_with_extra(&self, k: &Path, _extra: &Self::Extra) -> Option<Arc<Statistics>> {
        self.get(k)
    }

    fn put(&self, k: &Path, v: Arc<Statistics>) -> Option<Arc<Statistics>> {
        let meta = ObjectMeta {
            location: k.clone(),
            last_modified: chrono::Utc::now(),
            size: 0,
            e_tag: None,
            version: None,
        };
        self.put_with_extra(k, v, &meta)
    }

    fn put_with_extra(
        &self,
        k: &Path,
        v: Arc<Statistics>,
        e: &Self::Extra,
    ) -> Option<Arc<Statistics>> {
        let key = k.to_string();
        let memory_size = v.memory_size();

        let eviction_candidates = if let Ok(tracker) = self.memory_tracker.lock() {
            if let Ok(total) = self.total_memory.lock() {
                let current_size = *total;
                let size_limit = self.size_limit.load(Ordering::Relaxed);
                let threshold = (size_limit as f64 * self.eviction_threshold) as usize;
                if current_size + memory_size > threshold {
                    let target_eviction = (current_size + memory_size) - (size_limit as f64 * 0.6) as usize;
                    if let Ok(policy_guard) = self.policy.lock() {
                        policy_guard.select_for_eviction(target_eviction)
                    } else { vec![] }
                } else { vec![] }
            } else { vec![] }
        } else { vec![] };

        for candidate_key in eviction_candidates {
            if let Ok(path) = self.parse_key_to_path(&candidate_key) {
                self.remove_internal(&path);
            }
        }

        let result = self.inner_cache.insert(k.clone(), (e.clone(), v)).map(|x| x.1);

        if let Ok(mut tracker) = self.memory_tracker.lock() {
            if let Ok(mut total) = self.total_memory.lock() {
                if let Some(old_size) = tracker.get(&key) {
                    *total = total.saturating_sub(*old_size);
                }
                tracker.insert(key.clone(), memory_size);
                *total += memory_size;
            }
        }

        if let Ok(mut policy_guard) = self.policy.lock() {
            policy_guard.on_insert(&key, memory_size);
        }

        result
    }

    fn remove(&self, k: &Path) -> Option<Arc<Statistics>> {
        let key = k.to_string();
        let result = self.inner_cache.remove(k);
        if result.is_some() {
            if let Ok(mut tracker) = self.memory_tracker.lock() {
                if let Ok(mut total) = self.total_memory.lock() {
                    if let Some(old_size) = tracker.remove(&key) {
                        *total = total.saturating_sub(old_size);
                    }
                }
            }
            if let Ok(mut policy_guard) = self.policy.lock() {
                policy_guard.on_remove(&key);
            }
        }
        result.map(|x| x.1 .1)
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.inner_cache.get(k).is_some()
    }

    fn len(&self) -> usize {
        self.memory_tracker.lock().map(|t| t.len()).unwrap_or(0)
    }

    fn clear(&self) {
        self.inner_cache.clear();
        if let Ok(mut tracker) = self.memory_tracker.lock() { tracker.clear(); }
        if let Ok(mut total) = self.total_memory.lock() { *total = 0; }
        if let Ok(mut policy_guard) = self.policy.lock() { policy_guard.clear(); }
        self.reset_stats();
    }

    fn name(&self) -> String {
        format!(
            "CustomStatisticsCache({})",
            self.policy_name().unwrap_or_else(|_| "unknown".to_string())
        )
    }
}

impl datafusion::execution::cache::cache_manager::FileStatisticsCache for CustomStatisticsCache {
    fn list_entries(&self) -> std::collections::HashMap<object_store::path::Path, datafusion::execution::cache::cache_manager::FileStatisticsCacheEntry> {
        std::collections::HashMap::new()
    }
}

impl Default for CustomStatisticsCache {
    fn default() -> Self {
        Self::with_default_config()
    }
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
        assert_eq!(cache.policy_name().unwrap(), "lru");
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
        cache.put_with_extra(&path, stats, &meta);

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
            cache.put_with_extra(&path, stats, &meta);
        }
        assert!(cache.memory_consumed() <= 1000);
        assert!(cache.len() > 0);
    }

    #[test]
    fn test_manual_eviction_with_memory_tracking() {
        let mut cache = CustomStatisticsCache::with_default_config();
        for i in 0..5 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_with_extra(&path, stats, &meta);
        }
        let memory_before = cache.memory_consumed();
        assert!(memory_before > 0);
        let freed = cache.evict(memory_before / 2).unwrap();
        assert!(freed > 0);
        assert!(cache.memory_consumed() < memory_before);
    }

    #[test]
    fn test_policy_switching_with_memory() {
        let cache = CustomStatisticsCache::with_default_config();
        for i in 0..3 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_with_extra(&path, stats, &meta);
        }
        let memory_before = cache.memory_consumed();
        assert_eq!(cache.policy_name().unwrap(), "lru");
        cache.set_policy(PolicyType::Lfu).unwrap();
        assert_eq!(cache.policy_name().unwrap(), "lfu");
        assert_eq!(cache.memory_consumed(), memory_before);
    }

    #[test]
    fn test_remove_with_memory_tracking() {
        let cache = CustomStatisticsCache::with_default_config();
        let path1 = create_test_path("file1");
        let path2 = create_test_path("file2");
        let meta1 = create_test_meta(&path1);
        let meta2 = create_test_meta(&path2);
        let stats = Arc::new(create_test_statistics());

        cache.put_with_extra(&path1, stats.clone(), &meta1);
        cache.put_with_extra(&path2, stats, &meta2);
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
            cache.put_with_extra(&path, stats, &meta);
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
        cache.put_with_extra(&path, stats, &meta);

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
        cache.put_with_extra(&path1, stats, &meta1);

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
        cache.put_with_extra(&path, stats, &meta);

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
        cache.put_with_extra(&path, stats, &meta);
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
                cache_clone.put_with_extra(&path, stats, &meta);
                assert!(cache_clone.get(&path).is_some());
            });
            handles.push(handle);
        }

        for handle in handles { handle.join().unwrap(); }
        assert!(cache.len() > 0);
    }
}
