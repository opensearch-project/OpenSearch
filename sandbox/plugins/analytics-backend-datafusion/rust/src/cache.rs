/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use datafusion::execution::cache::cache_manager::{
    CachedFileMetadataEntry, FileMetadataCache, FileMetadataCacheEntry,
};
use datafusion::execution::cache::CacheAccessor;
use log::error;
use object_store::path::Path;

use crate::eviction_policy::{create_policy, CachePolicy, PolicyType};

// Cache type constants
pub const CACHE_TYPE_METADATA: &str = "METADATA";
pub const CACHE_TYPE_STATS: &str = "STATISTICS";

// Helper function to log cache operations
fn log_cache_error(operation: &str, error: &str) {
    error!("[CACHE ERROR] {} operation failed: {}", operation, error);
}

/// Byte size charged for one metadata entry: the file metadata's own reported size.
fn entry_size(entry: &CachedFileMetadataEntry) -> usize {
    entry.file_metadata.memory_size()
}

/// Inner store for [`MutexFileMetadataCache`]: the keyed entries.
///
/// Held behind an `RwLock` so concurrent reads (`get`/`contains_key`/`len`/`list_entries`) run in
/// parallel; only mutations (`put`/`remove`/`clear`/eviction) take the write lock. The running
/// byte total is tracked separately in the lock-free `memory_used` atomic (see the struct field),
/// so size queries never touch this lock at all.
struct MetaState {
    map: HashMap<Path, CachedFileMetadataEntry>,
}

/// Metadata cache for parquet footers, keyed by file path.
///
/// Historically this wrapped DataFusion's `DefaultFilesMetadataCache`, whose eviction is
/// **hardcoded LRU** — so `datafusion.metadata.cache.eviction.type` had no effect here.
/// It now owns its store and drives the pluggable [`CachePolicy`] (LRU / LFU / S3-FIFO),
/// mirroring `CustomStatisticsCache`, so the configured policy actually governs eviction
/// of metadata entries (and a one-off wide scan no longer pollutes the hot footer set
/// under S3-FIFO). Staleness is still the caller's job via `CachedFileMetadataEntry::is_valid_for`.
pub struct MutexFileMetadataCache {
    state: RwLock<MetaState>,
    /// Pluggable eviction policy (interior-mutable; shared shape with the statistics cache).
    policy: Arc<Mutex<Box<dyn CachePolicy>>>,
    size_limit: AtomicUsize,
    /// Running byte total of resident entries, tracked lock-free alongside the map. Updated on
    /// every mutation while the `state` write lock is held (so it stays consistent with the map),
    /// but read without any lock — size queries (`memory_used`, eviction's budget check) never
    /// contend on `state`. Reads may momentarily trail an in-flight mutation; that's fine, the
    /// total need not be exact (eviction re-checks in a loop).
    memory_used: AtomicUsize,
    hit_count: AtomicUsize,
    miss_count: AtomicUsize,
    /// When false, the cache serves every lookup as a miss and drops all writes.
    /// Toggled at runtime via the datafusion.metadata.cache.enabled setting; disabling
    /// also clears existing entries (see `set_enabled`) so memory is freed.
    enabled: AtomicBool,
}

impl MutexFileMetadataCache {
    /// Create with an explicit eviction policy, byte limit, and initial enabled state.
    pub fn with_enabled(policy_type: PolicyType, size_limit: usize, enabled: bool) -> Self {
        Self {
            state: RwLock::new(MetaState { map: HashMap::new() }),
            policy: Arc::new(Mutex::new(create_policy(policy_type))),
            size_limit: AtomicUsize::new(size_limit),
            memory_used: AtomicUsize::new(0),
            hit_count: AtomicUsize::new(0),
            miss_count: AtomicUsize::new(0),
            enabled: AtomicBool::new(enabled),
        }
    }

    /// Convenience constructor used by tests: LRU policy, given limit, enabled.
    pub fn new(policy_type: PolicyType, size_limit: usize) -> Self {
        Self::with_enabled(policy_type, size_limit, true)
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Enable or disable the cache at runtime. Disabling also clears existing entries
    /// so the memory is released immediately; enabling starts caching again from cold.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        if !enabled {
            self.clear_cache();
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
        if let Ok(mut s) = self.state.write() {
            s.map.clear();
            self.memory_used.store(0, Ordering::Relaxed);
        }
        if let Ok(mut p) = self.policy.lock() {
            p.clear();
        }
        self.reset_stats();
    }

    /// Current bytes held by metadata entries. Lock-free read of the running atomic total — may
    /// momentarily trail an in-flight `put`/`remove`, which is fine (callers tolerate inexactness).
    pub fn memory_used(&self) -> usize {
        self.memory_used.load(Ordering::Relaxed)
    }

    pub fn update_cache_limit(&self, new_limit: usize) {
        self.size_limit.store(new_limit, Ordering::Relaxed);
        self.enforce_limit();
    }

    pub fn get_cache_limit(&self) -> usize {
        self.size_limit.load(Ordering::Relaxed)
    }

    /// Evict via the policy until `memory_used <= size_limit`.
    fn enforce_limit(&self) {
        let limit = self.size_limit.load(Ordering::Relaxed);
        // Evict in rounds: a single select_for_eviction may not free enough (e.g. a
        // policy that promotes rather than evicts on a given victim), so loop until the
        // cache is within budget or no further progress can be made.
        loop {
            let used = self.memory_used();
            if used <= limit {
                return;
            }
            let target = used - limit;
            let victims = {
                let mut p = self.policy.lock().unwrap_or_else(|e| e.into_inner());
                p.select_for_eviction(target)
            };
            if victims.is_empty() {
                return; // policy has nothing more to give; avoid spinning
            }
            let mut freed = 0usize;
            {
                let mut s = self.state.write().unwrap_or_else(|e| e.into_inner());
                for key in &victims {
                    let path = Path::from(key.clone());
                    if let Some(entry) = s.map.remove(&path) {
                        let sz = entry_size(&entry);
                        self.memory_used.fetch_sub(sz, Ordering::Relaxed);
                        freed += sz;
                    }
                }
            }
            // Keep the policy's bookkeeping in sync with the store: the evicted keys are
            // gone, so the policy must forget them (otherwise LRU/LFU keep returning
            // already-removed victims and the cache never converges).
            {
                let mut p = self.policy.lock().unwrap_or_else(|e| e.into_inner());
                for key in &victims {
                    p.on_remove(key);
                }
            }
            if freed == 0 {
                return; // selected victims weren't resident; stop to avoid an infinite loop
            }
        }
    }
}

impl CacheAccessor<Path, CachedFileMetadataEntry> for MutexFileMetadataCache {
    fn get(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        // Disabled cache holds nothing (put drops writes); skip hit/miss accounting since
        // the cache isn't participating.
        if !self.is_enabled() {
            return None;
        }
        let hit = {
            // Read lock: concurrent gets proceed in parallel; only mutations block readers.
            let s = match self.state.read() {
                Ok(s) => s,
                Err(e) => {
                    log_cache_error("get", &e.to_string());
                    return None;
                }
            };
            s.map.get(k).cloned()
        };
        match &hit {
            Some(entry) => {
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut p) = self.policy.lock() {
                    p.on_access(&k.to_string(), entry_size(entry));
                }
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        hit
    }

    fn put(&self, k: &Path, v: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        // Drop writes while disabled so the cache stays empty / frees nothing to track.
        if !self.is_enabled() {
            return None;
        }
        let size = entry_size(&v);
        let key = k.to_string();
        let prev = {
            let mut s = match self.state.write() {
                Ok(s) => s,
                Err(e) => {
                    log_cache_error("put", &e.to_string());
                    return None;
                }
            };
            let prev = s.map.insert(k.clone(), v);
            // Update the byte total while the write lock is held so it stays consistent with the
            // map: net add of `size`, minus the displaced entry's size if we replaced one.
            if let Some(old) = &prev {
                self.memory_used.fetch_sub(entry_size(old), Ordering::Relaxed);
            }
            self.memory_used.fetch_add(size, Ordering::Relaxed);
            prev
        };
        if let Ok(mut p) = self.policy.lock() {
            p.on_insert(&key, size);
        }
        self.enforce_limit();
        prev
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        let removed = {
            let mut s = match self.state.write() {
                Ok(s) => s,
                Err(e) => {
                    log_cache_error("remove", &e.to_string());
                    return None;
                }
            };
            let removed = s.map.remove(k);
            if let Some(entry) = &removed {
                self.memory_used.fetch_sub(entry_size(entry), Ordering::Relaxed);
            }
            removed
        };
        if removed.is_some() {
            if let Ok(mut p) = self.policy.lock() {
                p.on_remove(&k.to_string());
            }
        }
        removed
    }

    fn contains_key(&self, k: &Path) -> bool {
        self.state.read().map(|s| s.map.contains_key(k)).unwrap_or(false)
    }

    fn len(&self) -> usize {
        self.state.read().map(|s| s.map.len()).unwrap_or(0)
    }

    fn clear(&self) {
        self.clear_cache();
    }

    fn name(&self) -> String {
        "MutexFileMetadataCache".to_string()
    }
}

impl FileMetadataCache for MutexFileMetadataCache {
    fn cache_limit(&self) -> usize {
        self.size_limit.load(Ordering::Relaxed)
    }

    fn update_cache_limit(&self, limit: usize) {
        self.size_limit.store(limit, Ordering::Relaxed);
        self.enforce_limit();
    }

    fn list_entries(&self) -> HashMap<Path, FileMetadataCacheEntry> {
        let s = match self.state.read() {
            Ok(s) => s,
            Err(e) => {
                log_cache_error("list_entries", &e.to_string());
                return HashMap::new();
            }
        };
        s.map
            .iter()
            .map(|(path, entry)| {
                (
                    path.clone(),
                    FileMetadataCacheEntry {
                        object_meta: entry.meta.clone(),
                        size_bytes: entry_size(entry),
                        hits: 0,
                        extra: entry.file_metadata.extra_info(),
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eviction_policy::PolicyType;
    use chrono::Utc;
    use datafusion::execution::cache::cache_manager::FileMetadata;
    use object_store::ObjectMeta;
    use std::any::Any;

    /// Minimal `FileMetadata` test double with a fixed byte size.
    #[derive(Debug)]
    struct FakeMetadata {
        bytes: usize,
    }

    impl FileMetadata for FakeMetadata {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn memory_size(&self) -> usize {
            self.bytes
        }
        fn extra_info(&self) -> std::collections::HashMap<String, String> {
            std::collections::HashMap::new()
        }
    }

    const ENTRY_BYTES: usize = 1024;

    fn path(name: &str) -> Path {
        Path::from(format!("/test/{name}.parquet"))
    }

    fn entry(p: &Path) -> CachedFileMetadataEntry {
        let meta = ObjectMeta {
            location: p.clone(),
            last_modified: Utc::now(),
            size: ENTRY_BYTES as u64,
            e_tag: None,
            version: None,
        };
        CachedFileMetadataEntry::new(meta, Arc::new(FakeMetadata { bytes: ENTRY_BYTES }))
    }

    #[test]
    fn test_metadata_cache_basic_put_get_evict() {
        // 5-entry budget, LRU. Insert past the limit → byte cap enforced.
        let cache = MutexFileMetadataCache::new(PolicyType::Lru, ENTRY_BYTES * 5);
        for i in 0..10 {
            let p = path(&format!("f{i}"));
            cache.put(&p, entry(&p));
        }
        assert!(cache.memory_used() <= ENTRY_BYTES * 5, "byte limit must be enforced");
        assert!(cache.len() > 0 && cache.len() <= 5);
    }

    #[test]
    fn test_metadata_cache_disabled_drops_writes() {
        let cache = MutexFileMetadataCache::with_enabled(PolicyType::Lru, ENTRY_BYTES * 5, false);
        let p = path("x");
        cache.put(&p, entry(&p));
        assert!(cache.get(&p).is_none(), "disabled cache serves misses");
        assert_eq!(cache.len(), 0, "disabled cache holds nothing");
    }

    #[test]
    fn test_e2e_metadata_s3fifo_hot_set_survives_scan() {
        // The headline property, end-to-end through the metadata cache's CacheAccessor
        // path with S3-FIFO: a hot footer set survives a wide one-off scan.
        let cache = MutexFileMetadataCache::new(PolicyType::S3Fifo, ENTRY_BYTES * 10);

        let hot: Vec<Path> = (0..3).map(|i| path(&format!("hot{i}"))).collect();
        for p in &hot {
            cache.put(p, entry(p));
        }
        // Query the hot set repeatedly → earns frequency in the policy.
        for _ in 0..5 {
            for p in &hot {
                assert!(cache.get(p).is_some(), "hot footer present during warmup");
            }
        }

        // Wide one-off scan: 100 cold footers, each read once.
        for i in 0..100 {
            let p = path(&format!("scan{i}"));
            cache.put(&p, entry(&p));
        }

        // Majority of the hot footer set must survive (see the statistics-cache e2e
        // test for why "majority" not "all" under the SLRU-like 50% probation default).
        let survivors = hot.iter().filter(|p| cache.get(p).is_some()).count();
        assert!(
            survivors >= 2,
            "S3-FIFO metadata cache must keep most of the {} hot footers after a 100-file scan; survived {}",
            hot.len(), survivors
        );
        assert!(cache.memory_used() <= ENTRY_BYTES * 10, "cache must honor its byte limit");
    }

    #[test]
    fn test_e2e_metadata_update_limit_shrinks() {
        // Lowering the limit at runtime evicts down to the new budget (mirrors the
        // dynamic datafusion.metadata.cache.size.limit path).
        let cache = MutexFileMetadataCache::new(PolicyType::S3Fifo, ENTRY_BYTES * 20);
        for i in 0..15 {
            let p = path(&format!("f{i}"));
            cache.put(&p, entry(&p));
        }
        assert!(cache.memory_used() > ENTRY_BYTES * 5);
        cache.update_cache_limit(ENTRY_BYTES * 5);
        assert!(
            cache.memory_used() <= ENTRY_BYTES * 5,
            "shrinking the limit must evict down to the new budget"
        );
    }
}
