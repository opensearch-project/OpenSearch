use std::sync::{Arc, Mutex};
use jni::JNIEnv;

use datafusion::execution::cache::cache_manager::{FileMetadataCache};
use datafusion::execution::cache::cache_unit::{DefaultFilesMetadataCache};
use datafusion::execution::cache::CacheAccessor;
use object_store::ObjectMeta;
use vectorized_exec_spi::log_error;

pub const ALL_CACHE_TYPES: &[&str] = &[CACHE_TYPE_METADATA, CACHE_TYPE_STATS];

// Cache type constants
pub const CACHE_TYPE_METADATA: &str = "METADATA";
pub const CACHE_TYPE_STATS: &str = "STATISTICS";

// Helper function to handle cache errors
fn handle_cache_error(env: &mut JNIEnv, operation: &str, error: &str) {
    let msg = format!("Cache {} failed: {}", operation, error);
    log_error!("[CACHE ERROR] {}", msg);
    let _ = env.throw_new("java/lang/DataFusionException", &msg);
}

// Helper function to log cache operations
fn log_cache_error(operation: &str, error: &str) {
    log_error!("[CACHE ERROR] {} operation failed: {}", operation, error);
}

// Note: MutexFileMetadataCache wrapper has been removed as DefaultFilesMetadataCache
// is already thread-safe with its own internal Mutex.
// The double-locking was causing race conditions and crashes.

// Note: create_cache function has been removed. Cache creation is now handled through CacheManagerConfig only.
// metadata_cache_put, metadata_cache_remove, and metadata_cache_get functions have been moved to CustomCacheManager as internal methods

// Wrapper to make Mutex<DefaultFilesMetadataCache> implement FileMetadataCache
pub struct MutexFileMetadataCache {
    pub inner: Mutex<DefaultFilesMetadataCache>,
}

impl MutexFileMetadataCache {
    pub fn new(cache: DefaultFilesMetadataCache) -> Self {
        Self {
            inner: Mutex::new(cache),
        }
    }

    pub fn clear(&self) {
        if let Ok(mut cache) = self.inner.lock() {
            cache.clear();
        }
    }

    pub fn update_cache_limit(&self, new_limit: usize) {
        if let Ok(mut cache) = self.inner.lock() {
            cache.update_cache_limit(new_limit);
        }
    }

    pub fn cache_limit(&self) -> usize {
        if let Ok(cache) = self.inner.lock() {
            cache.cache_limit()
        } else {
            0
        }
    }
}

// Implement CacheAccessor which is required by FileMetadataCache
impl CacheAccessor<ObjectMeta, Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> for MutexFileMetadataCache {
    type Extra = ObjectMeta;

    fn get(&self, k: &ObjectMeta) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(cache) => cache.get(k),
            Err(e) => {
                log_cache_error("get", &e.to_string());
                None
            }
        }
    }

    fn get_with_extra(&self, k: &ObjectMeta, extra: &Self::Extra) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(cache) => cache.get_with_extra(k, extra),
            Err(e) => {
                log_cache_error("get_with_extra", &e.to_string());
                None
            }
        }
    }

    fn put(&self, k: &ObjectMeta, v: Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(mut cache) => cache.put(k, v),
            Err(e) => {
                log_cache_error("put", &e.to_string());
                None
            }
        }
    }

    fn put_with_extra(&self, k: &ObjectMeta, v: Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>, e: &Self::Extra) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(mut cache) => cache.put_with_extra(k, v, e),
            Err(err) => {
                log_cache_error("put_with_extra", &err.to_string());
                None
            }
        }
    }

    fn remove(&mut self, k: &ObjectMeta) -> Option<Arc<dyn datafusion::execution::cache::cache_manager::FileMetadata>> {
        match self.inner.lock() {
            Ok(mut cache) => cache.remove(k),
            Err(e) => {
                log_cache_error("remove", &e.to_string());
                None
            }
        }
    }

    fn contains_key(&self, k: &ObjectMeta) -> bool {
        match self.inner.lock() {
            Ok(cache) => cache.contains_key(k),
            Err(e) => {
                log_cache_error("contains_key", &e.to_string());
                false
            }
        }
    }

    fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.len(),
            Err(e) => {
                log_cache_error("len", &e.to_string());
                0
            }
        }
    }

    fn clear(&self) {
        match self.inner.lock() {
            Ok(mut cache) => cache.clear(),
            Err(e) => log_cache_error("clear", &e.to_string()),
        }
    }

    fn name(&self) -> String {
        match self.inner.lock() {
            Ok(cache) => cache.name(),
            Err(e) => {
                log_cache_error("name", &e.to_string());
                "cache_error".to_string()
            }
        }
    }
}

impl FileMetadataCache for MutexFileMetadataCache {
    fn cache_limit(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.cache_limit(),
            Err(e) => {
                log_cache_error("cache_limit", &e.to_string());
                0
            }
        }
    }

    fn update_cache_limit(&self, limit: usize) {
        match self.inner.lock() {
            Ok(mut cache) => cache.update_cache_limit(limit),
            Err(e) => log_cache_error("update_cache_limit", &e.to_string()),
        }
    }

    fn list_entries(&self) -> std::collections::HashMap<object_store::path::Path, datafusion::execution::cache::cache_manager::FileMetadataCacheEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.list_entries(),
            Err(e) => {
                log_cache_error("list_entries", &e.to_string());
                std::collections::HashMap::new()
            }
        }
    }
}
