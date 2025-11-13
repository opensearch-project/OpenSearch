use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use datafusion::execution::cache::cache_manager::{FileMetadataCache, CacheManagerConfig, CacheManager};
use datafusion::execution::cache::cache_unit::DefaultFilesMetadataCache;
use datafusion::execution::cache::CacheAccessor;
use crate::cache::{ALL_CACHE_TYPES, CACHE_TYPE_METADATA, CACHE_TYPE_STATS, MutexFileMetadataCache, metadata_cache_get, metadata_cache_put, metadata_cache_remove};
use once_cell::sync::Lazy;

/// Enum to hold different cache types
pub enum CachePointer {
    FileMetadata(Arc<MutexFileMetadataCache>),
    // Future: Add more cache types
    // FileStatistics(Arc<dyn FileStatisticsCache>),
    // ListFiles(Arc<dyn ListFilesCache>),
}

/// Global HashMap to store cache pointers by cache type name
pub static GLOBAL_CACHE_MAP: Lazy<Mutex<HashMap<String, CachePointer>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

/// Add a cache to the global map
pub fn add_cache(cache_type: &str, cache: CachePointer) -> Result<(), String> {
    let mut cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    if cache_map.contains_key(cache_type) {
        return Err(format!("Cache type {} already exists", cache_type));
    }
    cache_map.insert(cache_type.to_string(), cache);
    Ok(())
}

/// Get total memory consumed by all caches
pub fn get_total_memory_consumed() -> Result<usize, String> {
    let cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    let mut total = 0;

    for (_, cache_ptr) in cache_map.iter() {
        match cache_ptr {
            CachePointer::FileMetadata(cache) => {
                if let Ok(inner) = cache.inner.lock() {
                    total += inner.memory_used();
                }
            }
            // Future: Add other cache types
        }
    }

    Ok(total)
}

/// Get memory consumed by a specific cache type
pub fn get_memory_consumed_by_type(cache_type: &str) -> Result<usize, String> {
    let cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    match cache_map.get(cache_type) {
        Some(CachePointer::FileMetadata(cache)) => {
            match cache.inner.lock() {
                Ok(inner) => Ok(inner.memory_used()),
                Err(e) => Err(format!("Failed to get metadata cache memory: {}", e))
            }
        }
        None => Err(format!("Cache type {} not found", cache_type)),
        // Future: Add other cache types
    }
}

/// Clear all caches
pub fn clear_all() -> Result<(), String> {
    let cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    for (_, cache_ptr) in cache_map.iter() {
        match cache_ptr {
            CachePointer::FileMetadata(cache) => {
                cache.clear();
            }
            // Future: Add other cache types
        }
    }

    Ok(())
}

/// Clear a specific cache by type
pub fn clear_cache_by_type(cache_type: &str) -> Result<(), String> {
    let cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    match cache_map.get(cache_type) {
        Some(CachePointer::FileMetadata(cache)) => {
            cache.clear();
            Ok(())
        }
        None => Err(format!("Cache type {} not found", cache_type)),
        // Future: Add other cache types
    }
}

/// Update cache size limit by type
pub fn update_cache_limit_by_type(cache_type: &str, new_limit: usize) -> Result<(), String> {
    let cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    match cache_map.get(cache_type) {
        Some(CachePointer::FileMetadata(cache)) => {
            cache.update_cache_limit(new_limit);
            Ok(())
        }
        None => Err(format!("Cache type {} not found", cache_type)),
        // Future: Add other cache types
    }
}

/// Put an item into all enabled caches for the given file path
pub fn add_files_to_cache(file_path: &str) -> Result<Vec<(String, Result<bool, String>)>, String> {
    let cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    let mut results = Vec::new();

    for (cache_type, cache_ptr) in cache_map.iter() {
        let result = match cache_ptr {
            CachePointer::FileMetadata(cache) => {
                metadata_cache_put(cache, file_path)
            }
            // Future: Add other cache types
        };
        results.push((cache_type.clone(), result));
    }

    Ok(results)
}

/// Remove an item from all enabled caches for the given file path
pub fn remove_files_from_cache(file_path: &str) -> Result<Vec<(String, Result<bool, String>)>, String> {
    let cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    let mut results = Vec::new();

    for (cache_type, cache_ptr) in cache_map.iter() {
        let result = match cache_ptr {
            CachePointer::FileMetadata(cache) => {
                metadata_cache_remove(cache, file_path)
            }
            // Future: Add other cache types
        };
        results.push((cache_type.clone(), result));
    }

    Ok(results)
}

/// For Testing Purposes Only
/// Get an item from a specific cache type
pub fn get_item_by_cache_type(cache_type: &str, file_path: &str) -> Result<bool, String> {
    let cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    match cache_map.get(cache_type) {
        Some(CachePointer::FileMetadata(cache)) => {
            let result = metadata_cache_get(cache, file_path);
            println!("Cache Element: {:?}", result);
            if result.is_ok() {
                return result;
            }
            return Err(format!("Cache type {} not found", cache_type));
        }
        None => Err(format!("Cache type {} not found", cache_type)),
        // Future: Add other cache types
    }
}

/// Get cache size limit by type
pub fn get_cache_limit_by_type(cache_type: &str) -> Result<usize, String> {
    let cache_map = GLOBAL_CACHE_MAP.lock()
        .map_err(|e| format!("Failed to lock global cache map: {}", e))?;

    match cache_map.get(cache_type) {
        Some(CachePointer::FileMetadata(cache)) => {
            Ok(cache.cache_limit())
        }
        None => Err(format!("Cache type {} not found", cache_type)),
        // Future: Add other cache types
    }
}
