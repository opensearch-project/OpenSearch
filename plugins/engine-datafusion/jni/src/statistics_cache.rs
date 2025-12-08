
use crate::cache_policy::{
    create_policy, CacheConfig, CacheError, CachePolicy, CacheResult, PolicyType,
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

// JNI imports
use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jlong};
use jni::JNIEnv;
use datafusion::execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use crate::DataFusionRuntime;
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
    /// Cache configuration
    config: CacheConfig,
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
    pub fn new(config: CacheConfig) -> Self {
        let inner_cache = DashMap::new();
        let policy = Arc::new(Mutex::new(create_policy(config.policy_type.clone())));

        Self {
            inner_cache,
            policy,
            config,
            memory_tracker: Arc::new(Mutex::new(HashMap::new())),
            total_memory: Arc::new(Mutex::new(0)),
            hit_count: Arc::new(Mutex::new(0)),
            miss_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Create with default configuration
    pub fn with_default_config() -> Self {
        Self::new(CacheConfig::default())
    }

    /// Get the underlying cache for compatibility
    pub fn inner(&self) -> &DashMap<Path,(ObjectMeta, Arc<Statistics>)> {
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
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Reset hit and miss counters
    pub fn reset_stats(&self) {
        if let Ok(mut hits) = self.hit_count.lock() {
            *hits = 0;
        }
        if let Ok(mut misses) = self.miss_count.lock() {
            *misses = 0;
        }
    }

    /// Update the cache size limit
    pub fn update_size_limit(&mut self, new_limit: usize) -> CacheResult<()> {
        // Update the config size limit
        self.config.size_limit = new_limit;

        let current_size = self.current_size()?;
        if current_size > new_limit {
            let target_eviction = current_size - (new_limit as f64 * 0.8) as usize;
            self.evict(target_eviction)?;
        }
        Ok(())
    }

    /// Switch to a different eviction policy
    pub fn set_policy(&self, policy_type: PolicyType) -> CacheResult<()> {
        let mut policy_guard = self
            .policy
            .lock()
            .map_err(|e| CacheError::PolicyLockError {
                reason: format!("Failed to acquire policy lock: {}", e),
            })?;

        // Create new policy and transfer existing entries
        let mut new_policy = create_policy(policy_type.clone());

        // Get all current entries and notify new policy
        if let Ok(tracker) = self.memory_tracker.lock() {
            for (key, size) in tracker.iter() {
                new_policy.on_insert(key, *size);
            }
        }

        *policy_guard = new_policy;
        // Note: We can't update self.config.policy_type here since we don't have &mut self
        // The policy change is effective immediately through the policy_guard update

        Ok(())
    }

    /// Get current policy name
    pub fn policy_name(&self) -> CacheResult<String> {
        let policy_guard = self
            .policy
            .lock()
            .map_err(|e| CacheError::PolicyLockError {
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
        if target_size == 0 {
            return Ok(0);
        }

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
            } else {
                0
            };

            if entry_size > 0 {
                if let Ok(path) = self.parse_key_to_path(&key) {
                    if self.inner_cache.remove(&path).is_some() {
                        // Update memory tracking
                        if let Ok(mut tracker) = self.memory_tracker.lock() {
                            if let Ok(mut total) = self.total_memory.lock() {
                                tracker.remove(&key);
                                *total = total.saturating_sub(entry_size);
                            }
                        }

                        // Notify policy
                        if let Ok(mut policy_guard) = self.policy.lock() {
                            policy_guard.on_remove(&key);
                        }

                        freed_size += entry_size;
                    }

                    if freed_size >= target_size {
                        break;
                    }
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

        // Actually remove from the underlying cache (DashMap allows this with &self)
        let result = self.inner_cache.remove(k);

        // Only proceed with tracking updates if the entry existed
        if result.is_some() {
            // Update memory tracking
            if let Ok(mut tracker) = self.memory_tracker.lock() {
                if let Ok(mut total) = self.total_memory.lock() {
                    if let Some(old_size) = tracker.remove(&key) {
                        *total = total.saturating_sub(old_size);
                    }
                }
            }

            // Notify policy of removal
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
        println!("stats cache get called");
        let result = self.inner_cache.get(k);

        if result.is_some() {
            // Increment hit count
            if let Ok(mut hits) = self.hit_count.lock() {
                *hits += 1;
            }

            // Notify policy of access
            let key = k.to_string();
            let memory_size = if let Ok(tracker) = self.memory_tracker.lock() {
                tracker.get(&key).copied().unwrap_or(0)
            } else {
                0
            };

            if let Ok(mut policy_guard) = self.policy.lock() {
                policy_guard.on_access(&key, memory_size);
            }
        } else {
            // Increment miss count
            if let Ok(mut misses) = self.miss_count.lock() {
                *misses += 1;
            }
        }

        result.map(|s| Some(Arc::clone(&s.value().1)))
            .unwrap_or(None)
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
        println!("stats cache put called");
        let key = k.to_string();
        let memory_size = v.memory_size();

        // Check if eviction is needed BEFORE inserting
        let current_size = self.memory_consumed();
        if current_size + memory_size > (self.config.size_limit as f64 * 0.8) as usize {
            let target_eviction = (current_size + memory_size) - (self.config.size_limit as f64 * 0.6) as usize;

            // Perform actual eviction using remove_internal
            let candidates = {
                if let Ok(policy_guard) = self.policy.lock() {
                    policy_guard.select_for_eviction(target_eviction)
                } else {
                    vec![]
                }
            };

            for candidate_key in candidates {
                if let Ok(path) = self.parse_key_to_path(&candidate_key) {
                    self.remove_internal(&path);
                }
            }
        }

        // Put in the underlying cache (DashMap handles concurrency)
        let result = self.inner_cache.insert(k.clone(), (e.clone(), v)).map(|x| x.1);

        // Track memory usage
        if let Ok(mut tracker) = self.memory_tracker.lock() {
            if let Ok(mut total) = self.total_memory.lock() {
                // If there was a previous entry, subtract its memory
                if let Some(old_size) = tracker.get(&key) {
                    *total = total.saturating_sub(*old_size);
                }

                // Add new entry memory
                tracker.insert(key.clone(), memory_size);
                *total += memory_size;
            }
        }

        // Notify policy of insertion
        if let Ok(mut policy_guard) = self.policy.lock() {
            policy_guard.on_insert(&key, memory_size);
        }

        result
    }

    fn remove(&mut self, k: &Path) -> Option<Arc<Statistics>> {
        let key = k.to_string();

        // Actually remove from the underlying cache
        let result = self.inner_cache.remove(k);

        // Only proceed with tracking updates if the entry existed
        if result.is_some() {
            // Update memory tracking
            if let Ok(mut tracker) = self.memory_tracker.lock() {
                if let Ok(mut total) = self.total_memory.lock() {
                    if let Some(old_size) = tracker.remove(&key) {
                        *total = total.saturating_sub(old_size);
                    }
                }
            }

            // Notify policy of removal
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
        // Clear the DashMap cache
        self.inner_cache.clear();

        // Clear memory tracking
        if let Ok(mut tracker) = self.memory_tracker.lock() {
            tracker.clear();
        }

        if let Ok(mut total) = self.total_memory.lock() {
            *total = 0;
        }

        // Clear policy
        if let Ok(mut policy_guard) = self.policy.lock() {
            policy_guard.clear();
        }

        // Reset hit/miss counters
        self.reset_stats();
    }

    fn name(&self) -> String {
        format!(
            "CustomStatisticsCache({})",
            self.policy_name().unwrap_or_else(|_| "unknown".to_string())
        )
    }
}

impl Default for CustomStatisticsCache {
    fn default() -> Self {
        Self::with_default_config()
    }
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
        let config = CacheConfig {
            policy_type: PolicyType::Lru,
            size_limit: 1024 * 1024,
            eviction_threshold: 0.8,
        };

        let cache = CustomStatisticsCache::new(config);
        assert_eq!(cache.policy_name().unwrap(), "lru");
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_memory_tracking_with_policy() {
        let cache = CustomStatisticsCache::with_default_config();

        // Initially empty
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);

        // Add an entry
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());

        cache.put_with_extra(&path, stats, &meta);

        // Should have memory consumption and policy tracking
        assert!(cache.memory_consumed() > 0);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_size().unwrap(), cache.memory_consumed());

        // Verify we can retrieve it
        assert!(cache.get(&path).is_some());
    }

    #[test]
    fn test_policy_based_eviction_with_memory() {
        let config = CacheConfig {
            policy_type: PolicyType::Lru,
            size_limit: 1000, // Small limit to trigger eviction
            eviction_threshold: 0.8,
        };
        let cache = CustomStatisticsCache::new(config);

        // Add multiple entries to trigger eviction
        for i in 0..10 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());

            cache.put_with_extra(&path, stats, &meta);
        }

        // Memory should be managed by eviction
        let final_memory = cache.memory_consumed();
        assert!(
            final_memory <= 1000,
            "Memory should be within limit due to eviction"
        );
        assert!(cache.len() > 0, "Should still have some entries");
    }

    #[test]
    fn test_manual_eviction_with_memory_tracking() {
        let mut cache = CustomStatisticsCache::with_default_config();

        // Add entries
        for i in 0..5 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_with_extra(&path, stats, &meta);
        }

        let memory_before = cache.memory_consumed();
        assert!(memory_before > 0);

        // Manually evict some memory
        let freed = cache.evict(memory_before / 2).unwrap();
        let memory_after = cache.memory_consumed();

        assert!(freed > 0, "Should have freed some memory");
        assert!(memory_after < memory_before, "Memory should be reduced");
    }

    #[test]
    fn test_policy_switching_with_memory() {
        let mut cache = CustomStatisticsCache::with_default_config();

        // Add some entries
        for i in 0..3 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_with_extra(&path, stats, &meta);
        }

        let memory_before = cache.memory_consumed();

        // Switch policy
        assert_eq!(cache.policy_name().unwrap(), "lru");
        cache.set_policy(PolicyType::Lfu).unwrap();
        assert_eq!(cache.policy_name().unwrap(), "lfu");

        // Memory tracking should be preserved
        assert_eq!(cache.memory_consumed(), memory_before);
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_remove_with_memory_tracking() {
        let mut cache = CustomStatisticsCache::with_default_config();

        // Add entries
        let path1 = create_test_path("file1");
        let path2 = create_test_path("file2");
        let meta1 = create_test_meta(&path1);
        let meta2 = create_test_meta(&path2);
        let stats = Arc::new(create_test_statistics());

        cache.put_with_extra(&path1, stats.clone(), &meta1);
        cache.put_with_extra(&path2, stats, &meta2);

        let memory_with_two = cache.memory_consumed();
        assert_eq!(cache.len(), 2);

        // Remove one entry
        let removed = cache.remove(&path1);
        assert!(removed.is_some());

        let memory_with_one = cache.memory_consumed();
        assert_eq!(cache.len(), 1);
        assert!(memory_with_one < memory_with_two);

        // Remove second entry
        cache.remove(&path2);
        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_clear_with_memory_tracking() {
        let cache = CustomStatisticsCache::with_default_config();

        // Add multiple entries
        for i in 0..3 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_with_extra(&path, stats, &meta);
        }

        assert!(cache.memory_consumed() > 0);
        assert_eq!(cache.len(), 3);

        // Clear all
        cache.clear();

        assert_eq!(cache.memory_consumed(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_lru_eviction_with_memory() {
        let config = CacheConfig {
            policy_type: PolicyType::Lru,
            size_limit: 2000, // Limit to ~2 entries
            eviction_threshold: 0.8,
        };
        let cache = CustomStatisticsCache::new(config);

        // Add entries
        let mut paths = vec![];
        for i in 0..5 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            paths.push(path.clone());

            cache.put_with_extra(&path, stats, &meta);
        }

        // Access some entries to update LRU order
        cache.get(&paths[2]);
        cache.get(&paths[4]);

        // Memory should be within limits due to LRU eviction
        assert!(cache.memory_consumed() <= 2000);

        // Recently accessed entries should still be available
        assert!(cache.get(&paths[2]).is_some());
        assert!(cache.get(&paths[4]).is_some());
    }

    #[test]
    fn test_lfu_eviction_with_memory() {
        let config = CacheConfig {
            policy_type: PolicyType::Lfu,
            size_limit: 2000, // Limit to ~2 entries
            eviction_threshold: 0.8,
        };
        let cache = CustomStatisticsCache::new(config);

        // Add entries
        let mut paths = vec![];
        for i in 0..5 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            paths.push(path.clone());

            cache.put_with_extra(&path, stats, &meta);
        }

        // Create frequency patterns
        for _ in 0..5 {
            cache.get(&paths[1]);
            cache.get(&paths[3]);
        }

        // Memory should be within limits due to LFU eviction
        assert!(cache.memory_consumed() <= 2000);

        // Frequently accessed entries should still be available
        assert!(cache.get(&paths[1]).is_some());
        assert!(cache.get(&paths[3]).is_some());
    }

    #[test]
    fn test_concurrent_operations() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(CustomStatisticsCache::with_default_config());
        let mut handles = vec![];

        // Spawn multiple threads doing concurrent operations
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let path = create_test_path(&format!("concurrent{}", i));
                let meta = create_test_meta(&path);
                let stats = Arc::new(create_test_statistics());

                // Put operation
                cache_clone.put_with_extra(&path, stats, &meta);

                // Get operation
                let result = cache_clone.get(&path);
                assert!(result.is_some());
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Cache should have entries
        assert!(cache.len() > 0);
    }

    #[test]
    fn test_hit_count_tracking() {
        let cache = CustomStatisticsCache::with_default_config();

        // Initially zero
        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 0);

        // Add an entry
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());
        cache.put_with_extra(&path, stats, &meta);

        // Get the entry - should increment hit count
        assert!(cache.get(&path).is_some());
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 0);

        // Get it again - should increment hit count again
        assert!(cache.get(&path).is_some());
        assert_eq!(cache.hit_count(), 2);
        assert_eq!(cache.miss_count(), 0);
    }

    #[test]
    fn test_miss_count_tracking() {
        let cache = CustomStatisticsCache::with_default_config();

        // Initially zero
        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 0);

        // Try to get non-existent entry - should increment miss count
        let path = create_test_path("nonexistent");
        assert!(cache.get(&path).is_none());
        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 1);

        // Try again - should increment miss count again
        assert!(cache.get(&path).is_none());
        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 2);
    }

    #[test]
    fn test_hit_miss_mixed_operations() {
        let cache = CustomStatisticsCache::with_default_config();

        // Add some entries
        let path1 = create_test_path("file1");
        let path2 = create_test_path("file2");
        let meta1 = create_test_meta(&path1);
        let meta2 = create_test_meta(&path2);
        let stats = Arc::new(create_test_statistics());

        cache.put_with_extra(&path1, stats.clone(), &meta1);
        cache.put_with_extra(&path2, stats, &meta2);

        // Mix of hits and misses
        assert!(cache.get(&path1).is_some()); // hit
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 0);

        let path3 = create_test_path("file3");
        assert!(cache.get(&path3).is_none()); // miss
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 1);

        assert!(cache.get(&path2).is_some()); // hit
        assert_eq!(cache.hit_count(), 2);
        assert_eq!(cache.miss_count(), 1);

        assert!(cache.get(&path3).is_none()); // miss again
        assert_eq!(cache.hit_count(), 2);
        assert_eq!(cache.miss_count(), 2);

        assert!(cache.get(&path1).is_some()); // hit again
        assert_eq!(cache.hit_count(), 3);
        assert_eq!(cache.miss_count(), 2);
    }

    #[test]
    fn test_hit_rate_calculation() {
        let cache = CustomStatisticsCache::with_default_config();

        // Initially 0.0 (no operations)
        assert_eq!(cache.hit_rate(), 0.0);

        // Add entries
        let path1 = create_test_path("file1");
        let path2 = create_test_path("file2");
        let meta1 = create_test_meta(&path1);
        let meta2 = create_test_meta(&path2);
        let stats = Arc::new(create_test_statistics());

        cache.put_with_extra(&path1, stats.clone(), &meta1);
        cache.put_with_extra(&path2, stats, &meta2);

        // 2 hits, 0 misses = 100% hit rate
        cache.get(&path1);
        cache.get(&path2);
        assert_eq!(cache.hit_rate(), 1.0);

        // 2 hits, 1 miss = 66.67% hit rate
        let path3 = create_test_path("file3");
        cache.get(&path3);
        assert!((cache.hit_rate() - 0.6666666666666666).abs() < 0.0001);

        // 3 hits, 1 miss = 75% hit rate
        cache.get(&path1);
        assert_eq!(cache.hit_rate(), 0.75);

        // 3 hits, 2 misses = 60% hit rate
        cache.get(&path3);
        assert_eq!(cache.hit_rate(), 0.6);
    }

    #[test]
    fn test_reset_stats() {
        let cache = CustomStatisticsCache::with_default_config();

        // Add entry and generate some hits/misses
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());
        cache.put_with_extra(&path, stats, &meta);

        cache.get(&path); // hit
        let path2 = create_test_path("file2");
        cache.get(&path2); // miss

        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 1);
        assert_eq!(cache.hit_rate(), 0.5);

        // Reset stats
        cache.reset_stats();

        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 0);
        assert_eq!(cache.hit_rate(), 0.0);

        // Cache entries should still exist
        assert_eq!(cache.len(), 1);
        assert!(cache.get(&path).is_some());

        // After reset, new operations should start counting from zero
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 0);
    }

    #[test]
    fn test_clear_resets_stats() {
        let cache = CustomStatisticsCache::with_default_config();

        // Add entries and generate hits/misses
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());
        cache.put_with_extra(&path, stats, &meta);

        cache.get(&path); // hit
        let path2 = create_test_path("file2");
        cache.get(&path2); // miss

        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 1);

        // Clear should reset stats
        cache.clear();

        assert_eq!(cache.hit_count(), 0);
        assert_eq!(cache.miss_count(), 0);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_hit_miss_with_eviction() {
        let config = CacheConfig {
            policy_type: PolicyType::Lru,
            size_limit: 1500, // Small limit to trigger eviction
            eviction_threshold: 0.8,
        };
        let cache = CustomStatisticsCache::new(config);

        // Add multiple entries to trigger eviction
        let mut paths = vec![];
        for i in 0..5 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            paths.push(path.clone());
            cache.put_with_extra(&path, stats, &meta);
        }

        // Access some entries
        cache.get(&paths[0]); // May or may not be evicted
        cache.get(&paths[4]); // Should still be there

        let hits_before = cache.hit_count();
        let misses_before = cache.miss_count();

        // Try to access potentially evicted entries
        for path in &paths {
            cache.get(path);
        }

        // Should have more hits and/or misses
        assert!(cache.hit_count() >= hits_before);
        assert!(cache.miss_count() >= misses_before);
        assert!(cache.hit_count() + cache.miss_count() > hits_before + misses_before);
    }

    #[test]
    fn test_concurrent_hit_miss_tracking() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(CustomStatisticsCache::with_default_config());

        // Add some entries
        for i in 0..5 {
            let path = create_test_path(&format!("file{}", i));
            let meta = create_test_meta(&path);
            let stats = Arc::new(create_test_statistics());
            cache.put_with_extra(&path, stats, &meta);
        }

        let mut handles = vec![];

        // Spawn threads that will generate hits and misses
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                // Mix of hits and misses
                let existing_path = create_test_path(&format!("file{}", i % 5));
                cache_clone.get(&existing_path); // hit

                let nonexistent_path = create_test_path(&format!("missing{}", i));
                cache_clone.get(&nonexistent_path); // miss
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 10 hits and 10 misses
        assert_eq!(cache.hit_count(), 10);
        assert_eq!(cache.miss_count(), 10);
        assert_eq!(cache.hit_rate(), 0.5);
    }
}

// ============================================================================
// JNI FUNCTIONS
// ============================================================================

/// Compute statistics from a parquet file using DataFusion's built-in functionality
fn compute_parquet_statistics(file_path: &str) -> Result<Statistics, Box<dyn std::error::Error>> {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    
    // Open the parquet file
    let file = File::open(file_path)?;
    
    // Build parquet reader to get metadata
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();
    let schema = builder.schema();
    
    // Get file-level statistics
    let file_metadata = metadata.file_metadata();
    let num_rows = file_metadata.num_rows() as usize;
    
    // Calculate total byte size from row groups
    let mut total_byte_size = 0;
    for row_group in metadata.row_groups() {
        total_byte_size += row_group.total_byte_size() as usize;
    }
    
    // Extract column statistics from parquet metadata
    let mut column_statistics = Vec::new();
    
    // Get the number of columns from schema
    let num_columns = schema.fields().len();
    
    // Initialize column statistics for each field
    for field_idx in 0..num_columns {
        let mut null_count = 0;
        let mut has_statistics = false;
        
        // Aggregate statistics across all row groups for this column
        for row_group in metadata.row_groups() {
            if let Some(col_metadata) = row_group.columns().get(field_idx) {
                if let Some(stats) = col_metadata.statistics() {
                    has_statistics = true;
                    null_count += stats.null_count_opt().unwrap_or(0);
                }
            }
        }
        
        // Create column statistics
        let col_stats = ColumnStatistics {
            null_count: if has_statistics {
                Precision::Exact(null_count as usize)
            } else {
                Precision::Absent
            },
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            distinct_count: Precision::Absent,
            sum_value: Precision::Absent,
        };
        
        column_statistics.push(col_stats);
    }
    
    // Create the Statistics object
    let statistics = Statistics {
        num_rows: Precision::Exact(num_rows),
        total_byte_size: Precision::Exact(total_byte_size),
        column_statistics,
    };
    
    Ok(statistics)
}

/// Create a statistics cache and add it to the CacheManagerConfig
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_createStatisticsCache(
    _env: JNIEnv,
    _class: JClass,
    config_ptr: jlong,
    size_limit: jlong,
) -> jlong {
    // Create memory-aware policy cache with default LRU policy
    let config = CacheConfig {
        policy_type: PolicyType::Lru,
        size_limit: size_limit as usize,
        eviction_threshold: 0.8,
    };
    let memory_aware_cache = CustomStatisticsCache::new(config);

    // Create a new DefaultFileStatisticsCache for the CacheManagerConfig
    let stats_cache = Arc::new(DefaultFileStatisticsCache::default());

    // Update the CacheManagerConfig at the same memory location
    if config_ptr != 0 {
        let cache_manager_config = unsafe { &mut *(config_ptr as *mut CacheManagerConfig) };
        *cache_manager_config = cache_manager_config
            .clone()
            .with_files_statistics_cache(Some(stats_cache));
    }

    // Return the CustomStatisticsCache pointer for JNI operations
    Box::into_raw(Box::new(memory_aware_cache)) as jlong
}

/// Get current memory usage from statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetSize(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> jlong {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.memory_consumed() as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheContainsFile(
    mut env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    file_path: JString
) -> bool {
    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(_) => return false,
    };
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };

    let path_obj = match Path::from_url_path(&file_path) {
        Ok(path) => path,
        Err(_) => {
            println!("Failed to create path object from: {}", file_path);
            return false;
        }
    };
    cache.inner().contains_key(&path_obj)
}

/// Update size limit for statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheUpdateSizeLimit(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
    new_size_limit: jlong,
) -> bool {
    let cache = unsafe { &mut *(cache_ptr as *mut CustomStatisticsCache) };

    match cache.update_size_limit(new_size_limit as usize) {
        Ok(_) => {
            println!("Statistics cache size limit updated to: {}", new_size_limit);
            true
        }
        Err(_) => {
            println!("Failed to update statistics cache size limit to: {}", new_size_limit);
            false
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheClear(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.clear();
    println!("Statistics cache cleared");
}

/// Get hit count from statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetHitCount(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> jlong {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.hit_count() as jlong
}

/// Get miss count from statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetMissCount(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> jlong {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.miss_count() as jlong
}

/// Get hit rate from statistics cache (returns value between 0.0 and 1.0)
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetHitRate(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) -> f64 {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.hit_rate()
}

/// Reset hit and miss counters in statistics cache
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheResetStats(
    _env: JNIEnv,
    _class: JClass,
    cache_ptr: jlong,
) {
    let cache = unsafe { &*(cache_ptr as *const CustomStatisticsCache) };
    cache.reset_stats();
    println!("Statistics cache hit/miss counters reset");
}

/// Automatically compute and cache statistics for a parquet file
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheComputeAndPut(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    file_path: JString,
) -> jboolean {
    if runtime_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Runtime pointer is null");
        return 0;
    }

    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };
    
    // Get the statistics cache
    let stats_cache = match &runtime.statistics_cache {
        Some(cache) => cache,
        None => {
            let _ = env.throw_new("java/lang/IllegalStateException", "Statistics cache not initialized");
            return 0;
        }
    };

    // Parse file path
    let file_path_str: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert file path: {}", e);
            eprintln!("[STATS CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/IllegalArgumentException", &msg);
            return 0;
        }
    };

    // Create Path object for cache key
    let path = Path::from(file_path_str.clone());
    
    // Check if already cached
    if stats_cache.contains_key(&path) {
        eprintln!("[STATS CACHE INFO] Statistics already cached for: {}", file_path_str);
        return 1; // Already cached
    }

    // Read parquet file metadata to compute statistics
    match compute_parquet_statistics(&file_path_str) {
        Ok(stats) => {
            // Create ObjectMeta for the cache entry
            let meta = ObjectMeta {
                location: path.clone(),
                last_modified: chrono::Utc::now(),
                size: std::fs::metadata(&file_path_str)
                    .map(|m| m.len())
                    .unwrap_or(0),
                e_tag: None,
                version: None,
            };

            // Put statistics in cache
            stats_cache.put_with_extra(&path, Arc::new(stats), &meta);
            
            eprintln!("[STATS CACHE INFO] Successfully computed and cached statistics for: {}", file_path_str);
            1 // Success
        }
        Err(e) => {
            let msg = format!("Failed to compute statistics for {}: {}", file_path_str, e);
            eprintln!("[STATS CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/io/IOException", &msg);
            0 // Failure
        }
    }
}

/// Batch compute and cache statistics for multiple parquet files
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheBatchComputeAndPut(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    file_paths: jni::objects::JObjectArray,
) -> jlong {
    use crate::util::parse_string_arr;
    
    if runtime_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Runtime pointer is null");
        return 0;
    }

    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };
    
    // Get the statistics cache
    let stats_cache = match &runtime.statistics_cache {
        Some(cache) => cache,
        None => {
            let _ = env.throw_new("java/lang/IllegalStateException", "Statistics cache not initialized");
            return 0;
        }
    };

    // Parse file paths array
    let file_paths_vec: Vec<String> = match parse_string_arr(&mut env, file_paths) {
        Ok(paths) => paths,
        Err(e) => {
            let msg = format!("Failed to parse file paths array: {}", e);
            eprintln!("[STATS CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/IllegalArgumentException", &msg);
            return 0;
        }
    };

    let mut success_count = 0;
    let mut failed_files = Vec::new();

    for file_path_str in file_paths_vec {
        let path = Path::from(file_path_str.clone());
        
        // Skip if already cached
        if stats_cache.contains_key(&path) {
            success_count += 1;
            continue;
        }

        // Compute and cache statistics
        match compute_parquet_statistics(&file_path_str) {
            Ok(stats) => {
                let meta = ObjectMeta {
                    location: path.clone(),
                    last_modified: chrono::Utc::now(),
                    size: std::fs::metadata(&file_path_str)
                        .map(|m| m.len())
                        .unwrap_or(0),
                    e_tag: None,
                    version: None,
                };

                stats_cache.put_with_extra(&path, Arc::new(stats), &meta);
                success_count += 1;
            }
            Err(e) => {
                eprintln!("[STATS CACHE ERROR] Failed to compute statistics for {}: {}", file_path_str, e);
                failed_files.push(file_path_str);
            }
        }
    }

    if !failed_files.is_empty() {
        eprintln!("[STATS CACHE WARNING] Failed to compute statistics for {} files: {:?}", 
                  failed_files.len(), failed_files);
    }

    eprintln!("[STATS CACHE INFO] Successfully computed and cached statistics for {} files", success_count);
    success_count as jlong
}

/// Get statistics from cache or compute if not present
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_DataFusionQueryJNI_statisticsCacheGetOrCompute(
    mut env: JNIEnv,
    _class: JClass,
    runtime_ptr: jlong,
    file_path: JString,
) -> jboolean {
    if runtime_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Runtime pointer is null");
        return 0;
    }

    let runtime = unsafe { &*(runtime_ptr as *const DataFusionRuntime) };
    
    // Get the statistics cache
    let stats_cache = match &runtime.statistics_cache {
        Some(cache) => cache,
        None => {
            let _ = env.throw_new("java/lang/IllegalStateException", "Statistics cache not initialized");
            return 0;
        }
    };

    // Parse file path
    let file_path_str: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert file path: {}", e);
            eprintln!("[STATS CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/IllegalArgumentException", &msg);
            return 0;
        }
    };

    let path = Path::from(file_path_str.clone());
    
    // Check if already cached
    if stats_cache.get(&path).is_some() {
        eprintln!("[STATS CACHE INFO] Statistics found in cache for: {}", file_path_str);
        return 1; // Found in cache
    }

    // Not in cache, compute and add
    match compute_parquet_statistics(&file_path_str) {
        Ok(stats) => {
            let meta = ObjectMeta {
                location: path.clone(),
                last_modified: chrono::Utc::now(),
                size: std::fs::metadata(&file_path_str)
                    .map(|m| m.len())
                    .unwrap_or(0),
                e_tag: None,
                version: None,
            };

            stats_cache.put_with_extra(&path, Arc::new(stats), &meta);
            
            eprintln!("[STATS CACHE INFO] Computed and cached statistics for: {}", file_path_str);
            1 // Success
        }
        Err(e) => {
            let msg = format!("Failed to compute statistics for {}: {}", file_path_str, e);
            eprintln!("[STATS CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/io/IOException", &msg);
            0 // Failure
        }
    }
}
