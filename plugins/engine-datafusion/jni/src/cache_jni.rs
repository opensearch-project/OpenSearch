use jni::objects::{JClass, JObject, JObjectArray, JString};
use jni::sys::{jlong, jstring};
use jni::{JNIEnv};
use crate::custom_cache_manager::CustomCacheManager;
use crate::util::{parse_string_arr};
use crate::cache;
use crate::DataFusionRuntime;
use datafusion::execution::cache::cache_unit::{DefaultFilesMetadataCache, DefaultFileStatisticsCache};
use std::sync::Arc;

/// Create a CustomCacheManager instance
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createCustomCacheManager(
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    let manager = CustomCacheManager::new();
    Box::into_raw(Box::new(manager)) as jlong
}

/// Destroy a CustomCacheManager instance
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_destroyCustomCacheManager(
    mut env: JNIEnv,
    _class: JClass,
    cache_manager_ptr: jlong,
) {
    if cache_manager_ptr != 0 {
        let _ = unsafe { Box::from_raw(cache_manager_ptr as *mut CustomCacheManager) };
        println!("[CACHE INFO] CustomCacheManager destroyed");
    }
}

/// Generic cache creation method that handles all cache types
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createCache(
    mut env: JNIEnv,
    _class: JClass,
    cache_manager_ptr: jlong,
    cache_type: JString,
    size_limit: jlong,
    eviction_type: JString,
) -> jlong {
    if cache_manager_ptr == 0 {
        let _ = env.throw_new("java/lang/DataFusionException", "CustomCacheManager pointer is null");
        return 0;
    }

    let cache_type_str: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache_type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/DataFusionException", &msg);
            return 0;
        }
    };

    let eviction_type_str: String = match env.get_string(&eviction_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert eviction_type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/DataFusionException", &msg);
            return 0;
        }
    };

    println!("[CACHE INFO] Creating cache: type={}, size_limit={}, eviction_type={}",
             cache_type_str, size_limit, eviction_type_str);

    let manager = unsafe { &mut *(cache_manager_ptr as *mut CustomCacheManager) };

    match cache_type_str.as_str() {
        cache::CACHE_TYPE_METADATA => {
            let inner_cache = DefaultFilesMetadataCache::new(size_limit as usize);
            let metadata_cache = Arc::new(cache::MutexFileMetadataCache::new(inner_cache));
            manager.set_file_metadata_cache(metadata_cache);
            println!("[CACHE INFO] Successfully created {} cache in CustomCacheManager", cache_type_str);
        }
        cache::CACHE_TYPE_STATS => {
            // Create statistics cache
            let config = crate::cache_policy::CacheConfig {
                policy_type: crate::cache_policy::PolicyType::Lru,
                size_limit: size_limit as usize,
                eviction_threshold: 0.8,
            };
            let stats_cache = Arc::new(crate::statistics_cache::CustomStatisticsCache::new(config));
            let datafusion_stats_cache = Arc::new(DefaultFileStatisticsCache::default());
            manager.set_statistics_cache(stats_cache, datafusion_stats_cache);
            println!("[CACHE INFO] Successfully created {} cache in CustomCacheManager", cache_type_str);
        }
        _ => {
            let msg = format!("Invalid cache type: {}", cache_type_str);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("java/lang/DataFusionException", &msg);
            return 0;
        }
    }

    0
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerAddFiles(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    files: JObjectArray,
) {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            let file_paths: Vec<String> = match parse_string_arr(&mut env, files) {
                Ok(paths) => paths,
                Err(e) => {
                    let msg = format!("Failed to parse file paths array: {}", e);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                    return;
                }
            };

            match manager.add_files(&file_paths) {
                Ok(results) => {
                    let mut failed_files = Vec::new();
                    for (file_path, success) in results {
                        if !success {
                            failed_files.push(file_path);
                        }
                    }

                    if !failed_files.is_empty() {
                        let msg = format!("Failed to add {} files to cache: {:?}", failed_files.len(), failed_files);
                        eprintln!("[CACHE ERROR] {}", msg);
                    }
                }
                Err(e) => {
                    let msg = format!("Failed to add files to cache: {}", e);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerRemoveFiles(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    files: JObjectArray,
) {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    let file_paths: Vec<String> = match parse_string_arr(&mut env, files) {
        Ok(paths) => paths,
        Err(e) => {
            let msg = format!("Failed to parse file paths array: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return;
        }
    };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            match manager.remove_files(&file_paths) {
                Ok(results) => {
                    let mut failed_files = Vec::new();
                    for (file_path, removed) in results {
                        if !removed {
                            failed_files.push(file_path);
                        }
                    }

                    if !failed_files.is_empty() {
                        let msg = format!("Failed to remove {} files from cache: {:?}", failed_files.len(), failed_files);
                        eprintln!("[CACHE ERROR] {}", msg);
                    }
                }
                Err(e) => {
                    let msg = format!("Failed to remove files from cache: {}", e);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerClear(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
) {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            manager.clear_all();
            println!("[CACHE INFO] Successfully cleared all caches");
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerUpdateSizeLimitForCacheType(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    cache_type: JString,
    new_size_limit: jlong,
) -> bool {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return false;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    let cache_type: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return false;
        }
    };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            match cache_type.as_str() {
                cache::CACHE_TYPE_METADATA => {
                    manager.update_metadata_cache_limit(new_size_limit as usize);
                    true
                }
                _ => {
                    let msg = format!("Unknown cache type: {}", cache_type);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                    false
                }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
            false
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerGetMemoryConsumedForCacheType(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    cache_type: JString,
) -> jlong {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return 0;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    let cache_type: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return 0;
        }
    };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            match manager.get_memory_consumed_by_type(&cache_type) {
                Ok(size) => size as jlong,
                Err(e) => {
                    let msg = format!("Failed to get memory consumed for cache type {}: {}", cache_type, e);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                    0
                }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerGetTotalMemoryConsumed(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
) -> jlong {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return 0;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            manager.get_total_memory_consumed() as jlong
        }
        None => {
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerClearByCacheType(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    cache_type: JString,
) {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    let cache_type: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return;
        }
    };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            match manager.clear_cache_type(&cache_type) {
                Ok(_) => {
                    println!("[CACHE INFO] Cache Type: {} cleared", cache_type);
                }
                Err(e) => {
                    eprintln!("[CACHE ERROR] {}", e);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &e);
                }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_cacheManagerGetItemByCacheType(
    mut env: JNIEnv,
    _class: JClass,
    runtime_env_ptr: jlong,
    cache_type: JString,
    file_path: JString,
) -> bool {
    if runtime_env_ptr == 0 {
        let _ = env.throw_new("java/lang/NullPointerException", "Cache manager pointer is null");
        return false;
    }

    let runtime_env = unsafe { &*(runtime_env_ptr as *const DataFusionRuntime) };

    let cache_type: String = match env.get_string(&cache_type) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert cache type string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return false;
        }
    };

    let file_path: String = match env.get_string(&file_path) {
        Ok(s) => s.into(),
        Err(e) => {
            let msg = format!("Failed to convert file path string: {}", e);
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
            return false;
        }
    };

    match &runtime_env.custom_cache_manager {
        Some(manager) => {
            match cache_type.as_str() {
                cache::CACHE_TYPE_METADATA => {
                    manager.contains_file(&file_path)
                }
                _ => {
                    let msg = format!("Unknown cache type: {}", cache_type);
                    eprintln!("[CACHE ERROR] {}", msg);
                    let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", &msg);
                    false
                }
            }
        }
        None => {
            let msg = "No custom cache manager available";
            eprintln!("[CACHE ERROR] {}", msg);
            let _ = env.throw_new("org/opensearch/datafusion/DataFusionException", msg);
            false
        }
    }
}
