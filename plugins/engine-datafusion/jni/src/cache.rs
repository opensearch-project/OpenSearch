
use std::sync::{Arc, Mutex};
use std::any::Any;
use jni::JNIEnv;
use jni::sys::jlong;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;

use datafusion::execution::cache::cache_manager::{FileMetadataCache};
use datafusion::execution::cache::cache_unit::{DefaultFilesMetadataCache};
use datafusion::execution::cache::CacheAccessor;
use object_store::ObjectMeta;
use tokio::runtime::Runtime;
use downcast_rs::{Downcast, impl_downcast};


use crate::util::{create_object_meta_from_file,construct_file_metadata};


pub const ALL_CACHE_TYPES: &[&str] = &[CACHE_TYPE_METADATA, CACHE_TYPE_STATS];

// Cache type constants
pub const CACHE_TYPE_METADATA: &str = "METADATA";
pub const CACHE_TYPE_STATS: &str = "STATISTICS";

/// Factory function to create and configure cache based on type
/// This creates the cache in CacheManagerConfig and also stores it in CustomCacheManager
pub fn create_cache(
    cache_manager_config_ptr: jlong,
    cache_type: &str,
    size_limit: usize,
    eviction_type: &str
) -> Result<(), String> {

    match cache_type {
        CACHE_TYPE_METADATA => {
            println!("[CACHE INFO] Creating {} cache with size_limit={}, eviction_type={}",
                     cache_type, size_limit, eviction_type);

            // Create the metadata cache
            let inner_cache = DefaultFilesMetadataCache::new(size_limit);
            let wrapped_cache = Arc::new(MutexFileMetadataCache::new(inner_cache));

            // Update the CacheManagerConfig
            if cache_manager_config_ptr != 0 {
                let config_arc = unsafe { &*(cache_manager_config_ptr as *const Arc<Mutex<CacheManagerConfig>>) };
                if let Ok(mut config) = config_arc.lock() {
                    *config = config.clone().with_file_metadata_cache(Some(wrapped_cache.clone()));
                    println!("[CACHE INFO] Added metadata cache to CacheManagerConfig");
                } else {
                    return Err("Failed to lock CacheManagerConfig".to_string());
                }
            }

            // Also add to global cache map
            let cache_pointer = crate::custom_cache_manager::CachePointer::FileMetadata(wrapped_cache.clone());
            crate::custom_cache_manager::add_cache(cache_type, cache_pointer)?;
            println!("[CACHE INFO] Added metadata cache to global cache map");

            Ok(())
        },
        CACHE_TYPE_STATS => {
            // Future: Create and store stats cache in both managers
            Err("Stats cache not yet implemented".to_string())
        }
        _ => {
            Err(format!("Invalid cache type: {}", cache_type))
        }
    }
}

// Helper function to handle cache errors
fn handle_cache_error(env: &mut JNIEnv, operation: &str, error: &str) {
    let msg = format!("Cache {} failed: {}", operation, error);
    eprintln!("[CACHE ERROR] {}", msg);
    let _ = env.throw_new("java/lang/DataFusionException", &msg);
}

// Helper function to log cache operations
fn log_cache_error(operation: &str, error: &str) {
    eprintln!("[CACHE ERROR] {} operation failed: {}", operation, error);
}

/*
DefaultFilesMetadataCache of datafusion is internally wrapped in a Mutex and requires mut access for operations like remove.
Refer: https://github.com/apache/datafusion/blob/main/datafusion/execution/src/cache/cache_unit.rs#L312-L315
https://github.com/apache/datafusion/blob/main/datafusion/execution/src/cache/cache_unit.rs#L402

Having multiple references to Cache, trying to acquire a mutable reference for methods like remove
would lead to failures.
Hence explicit handling of mutable references is required for which MutexFileMetadataCache is introduced
*/

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



pub fn metadata_cache_put(cache: &Arc<MutexFileMetadataCache>, file_path: &str) -> Result<bool, String> {
    let data_format = if file_path.to_lowercase().ends_with(".parquet") {
        "parquet"
    } else {
        return Ok(false); // Skip unsupported formats
    };


    let object_meta = create_object_meta_from_file(file_path);
    let store = Arc::new(object_store::local::LocalFileSystem::new());


    //TODO: Use TokioRuntimePtr to block on the async operation
    let metadata = Runtime::new()
        .map_err(|e| format!("Failed to create Tokio Runtime: {}", e))?
        .block_on(async {
            construct_file_metadata(store.as_ref(), &object_meta, data_format)
                .await
                .map_err(|e| format!("Failed to construct file metadata: {}", e))
        })?;

    match cache.inner.lock() {
        Ok(mut cache_guard) => {
            let len_before = cache_guard.len();
            let old_metadata = cache_guard.put(&object_meta, metadata);
            let len_after = cache_guard.len();

            if len_after > len_before {
                println!("Successfully cached new metadata for: {} (cache: {} -> {})", file_path, len_before, len_after);
                Ok(true)
            } else if old_metadata.is_some() {
                println!("Successfully updated existing metadata for: {} (cache: {})", file_path, len_after);
                Ok(true)
            } else {
                println!("Failed to cache metadata for: {} (cache: {})", file_path, len_after);
                Ok(false)
            }
        }
        Err(e) => Err(format!("Cache put failed: {}", e))
    }
}



// Regular Rust function for metadata cache remove
pub fn metadata_cache_remove(cache: &Arc<MutexFileMetadataCache>, file_path: &str) -> Result<bool, String> {
    let object_meta = create_object_meta_from_file(file_path);

    match cache.inner.lock() {
        Ok(mut cache_guard) => {
            if cache_guard.remove(&object_meta).is_some() {
                println!("Cache removed for: {}", file_path);
                Ok(true)
            } else {
                println!("Item not found in cache: {}", file_path);
                Ok(false)
            }
        }
        Err(e) => Err(format!("Cache remove failed: {}", e))
    }
}

// Regular Rust function for metadata cache get
pub fn metadata_cache_get(cache: &Arc<MutexFileMetadataCache>, file_path: &str) -> Result<bool, String> {
    let object_meta = create_object_meta_from_file(file_path);

    match cache.inner.lock() {
        Ok(cache_guard) => {
            match cache_guard.get(&object_meta) {
                Some(metadata) => {
                    println!("Retrieved metadata for: {} - size: {:?}", file_path, metadata.memory_size());
                    Ok(true)
                },
                None => {
                    println!("No metadata found for: {}", file_path);
                    Ok(false)
                },
            }
        }
        Err(e) => Err(format!("Cache get failed: {}", e))
    }
}
