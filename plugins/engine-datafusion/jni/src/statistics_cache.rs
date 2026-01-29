
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

use std::fs::File;

/// Trait to calculate heap memory size for statistics objects
/// 
/// IMPORTANT: This trait calculates ONLY the heap-allocated memory, not the stack size.
/// The stack size of the containing struct is accounted for separately in memory_size().
/// 
/// For inline types (primitives, enums stored by value), heap_size() returns 0.
/// For heap-allocated types (String, Vec buffer), heap_size() returns the heap allocation size.
trait HeapSize {
    fn heap_size(&self) -> usize;
}

impl HeapSize for Statistics {
    fn heap_size(&self) -> usize {
        // Only count heap allocations, not the struct's stack size
        self.num_rows.heap_size()
            + self.total_byte_size.heap_size()
            + self.column_statistics.heap_size()
    }
}

impl<T: HeapSize + std::fmt::Debug + Clone + PartialEq + Eq + PartialOrd> HeapSize
for Precision<T>
{
    fn heap_size(&self) -> usize {
        // Precision is an enum stored inline, so no heap allocation for the enum itself.
        // Only count heap allocations from the inner value.
        match self {
            Precision::Exact(val) => val.heap_size(),
            Precision::Inexact(val) => val.heap_size(),
            Precision::Absent => 0,
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
        // Vec allocates its buffer on the heap: capacity * element_size
        // Plus any heap allocations from the elements themselves
        (self.capacity() * std::mem::size_of::<T>())
            + self.iter().map(|item| item.heap_size()).sum::<usize>()
    }
}

impl HeapSize for ColumnStatistics {
    fn heap_size(&self) -> usize {
        // Only count heap allocations from the fields, not the struct's stack size
        self.null_count.heap_size()
            + self.max_value.heap_size()
            + self.min_value.heap_size()
            + self.distinct_count.heap_size()
    }
}

impl HeapSize for ScalarValue {
    fn heap_size(&self) -> usize {
        // Only count actual heap allocations
        match self {
            ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                s.capacity() // String's heap buffer
            }
            ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => {
                b.capacity() // Vec<u8>'s heap buffer
            }
            ScalarValue::List(arr) => {
                // Estimate list array memory size (heap allocation)
                arr.get_array_memory_size()
            }
            ScalarValue::Struct(arr) => {
                // Estimate struct array memory size (heap allocation)
                arr.get_array_memory_size()
            }
            _ => 0, // Primitive types and nulls have no heap allocation
        }
    }
}

/// Extension trait to add memory_size method to Statistics
/// 
/// This returns the total memory footprint of a Statistics object,
/// including both the stack size of the struct and all heap allocations.
trait StatisticsMemorySize {
    fn memory_size(&self) -> usize;
}

impl StatisticsMemorySize for Statistics {
    fn memory_size(&self) -> usize {
        // Total memory = stack size of Statistics struct + all heap allocations
        // 
        // The Statistics struct contains:
        // - num_rows: Precision<usize> (stored inline)
        // - total_byte_size: Precision<usize> (stored inline)
        // - column_statistics: Vec<ColumnStatistics> (Vec header inline, buffer on heap)
        //
        // So we count:
        // 1. size_of::<Statistics>() - the stack size of the struct itself
        // 2. heap_size() - all heap allocations (Vec buffer + any strings/binary in ScalarValues)
        std::mem::size_of::<Self>() + self.heap_size()
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
    size_limit: usize,
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
        let inner_cache = DashMap::new();
        let policy = Arc::new(Mutex::new(create_policy(policy_type)));

        Self {
            inner_cache,
            policy,
            size_limit,
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
        // Update the size limit
        self.size_limit = new_limit;

        let current_size = self.current_size()?;
        if current_size > new_limit {
            let target_eviction = current_size - (new_limit as f64 * self.eviction_threshold) as usize;
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
        // If size_limit is 0, don't cache anything
        if self.size_limit == 0 {
            return None;
        }

        let key = k.to_string();
        let memory_size = v.memory_size();

        // If the entry itself is larger than the size limit, don't cache it
        if memory_size > self.size_limit {
            return None;
        }

        // Check if eviction is needed BEFORE inserting
        let current_size = self.memory_consumed();
        if current_size + memory_size > (self.size_limit as f64 * self.eviction_threshold) as usize {
            let target_eviction = (current_size + memory_size) - (self.size_limit as f64 * 0.6) as usize;

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
        let cache = CustomStatisticsCache::new(PolicyType::Lru, 1024 * 1024, 0.8);
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
        let cache = CustomStatisticsCache::new(PolicyType::Lru, 1000, 0.8);

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
        let cache = CustomStatisticsCache::new(PolicyType::Lru, 2000, 0.8);

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
        let cache = CustomStatisticsCache::new(PolicyType::Lfu, 2000, 0.8);

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
        let cache = CustomStatisticsCache::new(PolicyType::Lru, 1500, 0.8);

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
    fn test_zero_size_limit_prevents_caching() {
        let cache = CustomStatisticsCache::new(PolicyType::Lru, 0, 0.8);

        // Try to add an entry
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());

        // Put should return None and not cache anything
        let result = cache.put_with_extra(&path, stats, &meta);
        assert!(result.is_none());

        // Cache should remain empty
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_consumed(), 0);

        // Get should return None (miss)
        assert!(cache.get(&path).is_none());
        assert_eq!(cache.miss_count(), 1);
        assert_eq!(cache.hit_count(), 0);
    }

    #[test]
    fn test_entry_larger_than_limit_not_cached() {
        // Create a cache with a very small limit
        let cache = CustomStatisticsCache::new(PolicyType::Lru, 10, 0.8);

        // Try to add an entry that's larger than the limit
        let path = create_test_path("file1");
        let meta = create_test_meta(&path);
        let stats = Arc::new(create_test_statistics());

        // The statistics entry is larger than 10 bytes, so it shouldn't be cached
        let result = cache.put_with_extra(&path, stats, &meta);
        assert!(result.is_none());

        // Cache should remain empty
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.memory_consumed(), 0);
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
pub fn compute_parquet_statistics(file_path: &str) -> Result<Statistics, Box<dyn std::error::Error>> {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    // Open the parquet file
    let file = File::open(file_path)?;

    // Build parquet reader to get metadata
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata();
    let schema = builder.schema().clone();

    // Create ObjectStore and ObjectMeta for the file
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
    let path = Path::from(file_path);
    let file_metadata = std::fs::metadata(file_path)?;
    let object_meta = ObjectMeta {
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
