//! # Cache Policy Module
//!
//! Simple pluggable cache eviction policies for statistics cache.

use datafusion::common::instant;
use instant::Instant;
use thiserror::Error;

/// Error types for cache operations
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Policy lock error: {reason}")]
    PolicyLockError { reason: String },
}

/// Result type for cache operations
pub type CacheResult<T> = Result<T, CacheError>;

/// Core trait for cache eviction policies
pub trait CachePolicy: Send + Sync {
    /// Called when a cache entry is accessed
    fn on_access(&mut self, key: &str, size: usize);

    /// Called when a cache entry is inserted
    fn on_insert(&mut self, key: &str, size: usize);

    /// Called when a cache entry is removed
    fn on_remove(&mut self, key: &str);

    /// Select entries for eviction to reach target size
    /// Returns keys to evict, ordered by eviction priority
    fn select_for_eviction(&self, target_size: usize) -> Vec<String>;

    /// Reset policy state
    fn clear(&mut self);

    /// Get the name of this policy
    fn policy_name(&self) -> &'static str;
}

/// Policy types
#[derive(Debug, Clone)]
pub enum PolicyType {
    Lru,
    Lfu,
}

/// Simple cache entry metadata
#[derive(Debug, Clone)]
pub struct CacheEntryMetadata {
    pub size: usize,
    pub last_accessed: Instant,
    pub access_count: usize,
}

impl CacheEntryMetadata {
    pub fn new(_key: String, size: usize) -> Self {
        let now = Instant::now();
        Self {
            size,
            last_accessed: now,
            access_count: 1,
        }
    }

    pub fn on_access(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }
}

/// LRU (Least Recently Used) policy
pub struct LruPolicy {
    entries: dashmap::DashMap<String, CacheEntryMetadata>,
    total_size: std::sync::atomic::AtomicUsize,
}

impl LruPolicy {
    pub fn new() -> Self {
        Self {
            entries: dashmap::DashMap::new(),
            total_size: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for LruPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for LruPolicy {
    fn on_access(&mut self, key: &str, size: usize) {
        match self.entries.get_mut(key) {
            Some(mut entry) => {
                entry.on_access();
            }
            None => {
                let metadata = CacheEntryMetadata::new(key.to_string(), size);
                self.entries.insert(key.to_string(), metadata);
                self.total_size
                    .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    fn on_insert(&mut self, key: &str, size: usize) {
        let metadata = CacheEntryMetadata::new(key.to_string(), size);

        if let Some(old_entry) = self.entries.insert(key.to_string(), metadata) {
            let old_size = old_entry.size;
            self.total_size
                .fetch_sub(old_size, std::sync::atomic::Ordering::Relaxed);
        }

        self.total_size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    fn on_remove(&mut self, key: &str) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size
                .fetch_sub(entry.size, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn select_for_eviction(&self, target_size: usize) -> Vec<String> {
        println!("info seleectpon");
        if target_size == 0 {
            return Vec::new();
        }

        // Collect entries with access times
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let last_accessed = entry.value().last_accessed;
                (key, last_accessed)
            })
            .collect();

        // Sort by access time (oldest first)
        entries.sort_by_key(|(_, last_accessed)| *last_accessed);

        // Select entries for eviction until target size is reached
        let mut candidates = Vec::new();
        let mut freed_size = 0;

        for (key, _) in entries {
            if freed_size >= target_size {
                break;
            }

            if let Some(entry) = self.entries.get(&key) {
                freed_size += entry.size;
                candidates.push(key.clone());
                println!("Selected :{}",key);
            }
        }

        candidates
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.total_size
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    fn policy_name(&self) -> &'static str {
        "lru"
    }
}

/// LFU (Least Frequently Used) policy
pub struct LfuPolicy {
    entries: dashmap::DashMap<String, CacheEntryMetadata>,
    total_size: std::sync::atomic::AtomicUsize,
}

impl LfuPolicy {
    pub fn new() -> Self {
        Self {
            entries: dashmap::DashMap::new(),
            total_size: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for LfuPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl CachePolicy for LfuPolicy {
    fn on_access(&mut self, key: &str, size: usize) {
        match self.entries.get_mut(key) {
            Some(mut entry) => {
                entry.on_access();
            }
            None => {
                let metadata = CacheEntryMetadata::new(key.to_string(), size);
                self.entries.insert(key.to_string(), metadata);
                self.total_size
                    .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    fn on_insert(&mut self, key: &str, size: usize) {
        let metadata = CacheEntryMetadata::new(key.to_string(), size);

        if let Some(old_entry) = self.entries.insert(key.to_string(), metadata) {
            let old_size = old_entry.size;
            self.total_size
                .fetch_sub(old_size, std::sync::atomic::Ordering::Relaxed);
        }

        self.total_size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);
    }

    fn on_remove(&mut self, key: &str) {
        if let Some((_, entry)) = self.entries.remove(key) {
            self.total_size
                .fetch_sub(entry.size, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn select_for_eviction(&self, target_size: usize) -> Vec<String> {
        if target_size == 0 {
            return Vec::new();
        }

        // Collect entries with access counts
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let access_count = entry.value().access_count;
                let last_accessed = entry.value().last_accessed;
                (key, access_count, last_accessed)
            })
            .collect();

        // Sort by access count (least frequent first), then by time for tie-breaking
        entries.sort_by(|(_, count_a, time_a), (_, count_b, time_b)| {
            count_a.cmp(count_b).then(time_a.cmp(time_b))
        });

        // Select entries for eviction until target size is reached
        let mut candidates = Vec::new();
        let mut freed_size = 0;

        for (key, _, _) in entries {
            if freed_size >= target_size {
                break;
            }

            if let Some(entry) = self.entries.get(&key) {
                freed_size += entry.size;
                candidates.push(key);
            }
        }

        candidates
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.total_size
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    fn policy_name(&self) -> &'static str {
        "lfu"
    }
}

/// Create a cache policy instance
pub fn create_policy(policy_type: PolicyType) -> Box<dyn CachePolicy> {
    match policy_type {
        PolicyType::Lru => Box::new(LruPolicy::new()),
        PolicyType::Lfu => Box::new(LfuPolicy::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_cache_config_default() {
        let config = CacheConfig::default();
        assert!(matches!(config.policy_type, PolicyType::Lru));
        assert_eq!(config.size_limit, 100 * 1024 * 1024);
        assert_eq!(config.eviction_threshold, 0.8);
    }

    #[test]
    fn test_cache_entry_metadata() {
        let mut metadata = CacheEntryMetadata::new("test_key".to_string(), 1024);
        assert_eq!(metadata.size, 1024);
        assert_eq!(metadata.access_count, 1);

        let initial_access_time = metadata.last_accessed;
        thread::sleep(Duration::from_millis(1));

        metadata.on_access();
        assert_eq!(metadata.access_count, 2);
        assert!(metadata.last_accessed > initial_access_time);
    }

    #[test]
    fn test_create_policy() {
        let lru_policy = create_policy(PolicyType::Lru);
        assert_eq!(lru_policy.policy_name(), "lru");

        let lfu_policy = create_policy(PolicyType::Lfu);
        assert_eq!(lfu_policy.policy_name(), "lfu");
    }

    #[test]
    fn test_lru_policy_basic_operations() {
        let mut policy = LruPolicy::new();

        assert_eq!(policy.policy_name(), "lru");

        policy.on_insert("key1", 100);
        policy.on_insert("key2", 200);

        policy.on_access("key1", 100);

        policy.on_remove("key1");

        policy.clear();
    }

    #[test]
    fn test_lru_policy_victim_selection() {
        let mut policy = LruPolicy::new();

        policy.on_insert("oldest", 100);
        thread::sleep(Duration::from_millis(1));

        policy.on_insert("middle", 100);
        thread::sleep(Duration::from_millis(1));

        policy.on_insert("newest", 100);
        thread::sleep(Duration::from_millis(1));

        // Access middle entry to make it more recent
        policy.on_access("middle", 100);

        let candidates = policy.select_for_eviction(150);
        assert_eq!(candidates.len(), 2);
        assert!(candidates.contains(&"oldest".to_string()));
        assert!(!candidates.contains(&"middle".to_string()));
    }

    #[test]
    fn test_lfu_policy_victim_selection() {
        let mut policy = LfuPolicy::new();

        policy.on_insert("rarely_used", 100);
        policy.on_insert("sometimes_used", 100);
        policy.on_insert("frequently_used", 100);

        // Create frequency patterns
        policy.on_access("sometimes_used", 100);

        for _ in 0..3 {
            policy.on_access("frequently_used", 100);
        }

        let candidates = policy.select_for_eviction(150);
        assert_eq!(candidates.len(), 2);
        assert!(candidates.contains(&"rarely_used".to_string()));
        assert!(candidates.contains(&"sometimes_used".to_string()));
        assert!(!candidates.contains(&"frequently_used".to_string()));
    }
}
