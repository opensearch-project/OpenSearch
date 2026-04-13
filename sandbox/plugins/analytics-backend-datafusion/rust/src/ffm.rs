/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for DataFusion.

use std::slice;
use std::str;
use std::sync::Arc;

use native_bridge_common::ffm_safe;
use parking_lot::RwLock;

use crate::api;
use crate::api::DataFusionRuntime;
use crate::cache;
use crate::custom_cache_manager::CustomCacheManager;
use crate::eviction_policy::PolicyType;
use crate::runtime_manager::RuntimeManager;
use crate::statistics_cache::CustomStatisticsCache;

use datafusion::execution::cache::cache_unit::DefaultFilesMetadataCache;

static TOKIO_RUNTIME_MANAGER: RwLock<Option<Arc<RuntimeManager>>> = RwLock::new(None);

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    if len < 0 {
        return Err(format!("negative string length: {}", len));
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))
}

fn get_rt_manager() -> Result<Arc<RuntimeManager>, String> {
    TOKIO_RUNTIME_MANAGER
        .read()
        .clone()
        .ok_or_else(|| "Runtime manager not initialized".to_string())
}

#[no_mangle]
pub extern "C" fn df_init_runtime_manager(cpu_threads: i32) {
    let mut guard = TOKIO_RUNTIME_MANAGER.write();
    *guard = Some(Arc::new(RuntimeManager::new(cpu_threads as usize)));
}

#[no_mangle]
pub extern "C" fn df_shutdown_runtime_manager() {
    let mgr = TOKIO_RUNTIME_MANAGER.write().take();
    if let Some(mgr) = mgr {
        mgr.shutdown();
    }
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_global_runtime(
    memory_pool_limit: i64,
    cache_manager_ptr: i64,
    spill_dir_ptr: *const u8,
    spill_dir_len: i64,
    spill_limit: i64,
) -> i64 {
    let spill_dir = str_from_raw(spill_dir_ptr, spill_dir_len).map_err(|e| format!("df_create_global_runtime: {}", e))?;
    api::create_global_runtime(memory_pool_limit, cache_manager_ptr, spill_dir, spill_limit)
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_global_runtime(ptr: i64) {
    api::close_global_runtime(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_reader(
    table_path_ptr: *const u8,
    table_path_len: i64,
    files_ptr: *const *const u8,
    files_len_ptr: *const i64,
    files_count: i64,
) -> i64 {
    let table_path = str_from_raw(table_path_ptr, table_path_len).map_err(|e| format!("df_create_reader: {}", e))?;
    let mut filenames = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        filenames.push(str_from_raw(ptr, len).map_err(|e| format!("df_create_reader: {}", e))?.to_string());
    }
    let mgr = get_rt_manager()?;
    api::create_reader(table_path, filenames, &mgr).map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_reader(ptr: i64) {
    api::close_reader(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_query(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    plan_ptr: *const u8,
    plan_len: i64,
    runtime_ptr: i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_execute_query: {}", e))?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);
    mgr.io_runtime
        .block_on(api::execute_query(shard_view_ptr, table_name, plan_bytes, runtime_ptr, &mgr))
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_get_schema(stream_ptr: i64) -> i64 {
    api::stream_get_schema(stream_ptr).map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_next(stream_ptr: i64) -> i64 {
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(api::stream_next(stream_ptr))
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_stream_close(stream_ptr: i64) {
    api::stream_close(stream_ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_sql_to_substrait(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    sql_ptr: *const u8,
    sql_len: i64,
    runtime_ptr: i64,
    out_ptr: *mut u8,
    out_cap: i64,
    out_len: *mut i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_sql_to_substrait: table_name: {}", e))?;
    let sql = str_from_raw(sql_ptr, sql_len).map_err(|e| format!("df_sql_to_substrait: sql: {}", e))?;
    let bytes = api::sql_to_substrait(shard_view_ptr, table_name, sql, runtime_ptr, &mgr)
        .map_err(|e| e.to_string())?;
    if bytes.len() > out_cap as usize {
        return Err(format!(
            "substrait plan size {} exceeds buffer capacity {}",
            bytes.len(),
            out_cap
        ));
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
    if !out_len.is_null() {
        *out_len = bytes.len() as i64;
    }
    Ok(0)
}

#[no_mangle]
pub extern "C" fn df_create_custom_cache_manager() -> i64 {
    let manager = CustomCacheManager::new();
    Box::into_raw(Box::new(manager)) as i64
}

#[no_mangle]
pub unsafe extern "C" fn df_destroy_custom_cache_manager(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut CustomCacheManager);
    }
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_cache(
    cache_manager_ptr: i64,
    cache_type_ptr: *const u8,
    cache_type_len: i64,
    size_limit: i64,
    eviction_type_ptr: *const u8,
    eviction_type_len: i64,
) -> i64 {
    if cache_manager_ptr == 0 {
        return Err("df_create_cache: null cache manager pointer".to_string());
    }
    let cache_type = str_from_raw(cache_type_ptr, cache_type_len)
        .map_err(|e| format!("df_create_cache: cache_type: {}", e))?;
    let _eviction_type = str_from_raw(eviction_type_ptr, eviction_type_len)
        .map_err(|e| format!("df_create_cache: eviction_type: {}", e))?;

    // Safety: cache_manager_ptr must be a valid pointer from df_create_custom_cache_manager
    let manager = &mut *(cache_manager_ptr as *mut CustomCacheManager);

    match cache_type {
        cache::CACHE_TYPE_METADATA => {
            let inner_cache = DefaultFilesMetadataCache::new(size_limit as usize);
            let metadata_cache = Arc::new(cache::MutexFileMetadataCache::new(inner_cache));
            manager.set_file_metadata_cache(metadata_cache);
        }
        cache::CACHE_TYPE_STATS => {
            let stats_cache = Arc::new(CustomStatisticsCache::new(
                PolicyType::Lru,
                size_limit as usize,
                0.8,
            ));
            manager.set_statistics_cache(stats_cache);
        }
        _ => {
            return Err(format!("df_create_cache: invalid cache type: {}", cache_type));
        }
    }
    Ok(0)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_cache_manager_add_files(
    runtime_ptr: i64,
    files_ptr: *const *const u8,
    files_len_ptr: *const i64,
    files_count: i64,
) -> i64 {
    if runtime_ptr == 0 {
        return Err("df_cache_manager_add_files: null runtime pointer".to_string());
    }
    // Safety: runtime_ptr must be a valid pointer from df_create_global_runtime
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime.custom_cache_manager.as_ref()
        .ok_or_else(|| "df_cache_manager_add_files: no cache manager configured".to_string())?;

    let mut file_paths = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        file_paths.push(str_from_raw(ptr, len)
            .map_err(|e| format!("df_cache_manager_add_files: {}", e))?.to_string());
    }

    manager.add_files(&file_paths)
        .map_err(|e| format!("df_cache_manager_add_files: {}", e))?;
    Ok(0)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_cache_manager_remove_files(
    runtime_ptr: i64,
    files_ptr: *const *const u8,
    files_len_ptr: *const i64,
    files_count: i64,
) -> i64 {
    if runtime_ptr == 0 {
        return Err("df_cache_manager_remove_files: null runtime pointer".to_string());
    }
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime.custom_cache_manager.as_ref()
        .ok_or_else(|| "df_cache_manager_remove_files: no cache manager configured".to_string())?;

    let mut file_paths = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        file_paths.push(str_from_raw(ptr, len)
            .map_err(|e| format!("df_cache_manager_remove_files: {}", e))?.to_string());
    }

    manager.remove_files(&file_paths)
        .map_err(|e| format!("df_cache_manager_remove_files: {}", e))?;
    Ok(0)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_cache_manager_clear(runtime_ptr: i64) -> i64 {
    if runtime_ptr == 0 {
        return Err("df_cache_manager_clear: null runtime pointer".to_string());
    }
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime.custom_cache_manager.as_ref()
        .ok_or_else(|| "df_cache_manager_clear: no cache manager configured".to_string())?;
    manager.clear_all();
    Ok(0)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_cache_manager_clear_by_type(
    runtime_ptr: i64,
    cache_type_ptr: *const u8,
    cache_type_len: i64,
) -> i64 {
    if runtime_ptr == 0 {
        return Err("df_cache_manager_clear_by_type: null runtime pointer".to_string());
    }
    let cache_type = str_from_raw(cache_type_ptr, cache_type_len)
        .map_err(|e| format!("df_cache_manager_clear_by_type: {}", e))?;
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime.custom_cache_manager.as_ref()
        .ok_or_else(|| "df_cache_manager_clear_by_type: no cache manager configured".to_string())?;
    manager.clear_cache_type(cache_type)
        .map_err(|e| format!("df_cache_manager_clear_by_type: {}", e))?;
    Ok(0)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_cache_manager_get_memory_by_type(
    runtime_ptr: i64,
    cache_type_ptr: *const u8,
    cache_type_len: i64,
) -> i64 {
    if runtime_ptr == 0 {
        return Err("df_cache_manager_get_memory_by_type: null runtime pointer".to_string());
    }
    let cache_type = str_from_raw(cache_type_ptr, cache_type_len)
        .map_err(|e| format!("df_cache_manager_get_memory_by_type: {}", e))?;
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime.custom_cache_manager.as_ref()
        .ok_or_else(|| "df_cache_manager_get_memory_by_type: no cache manager configured".to_string())?;
    let size = manager.get_memory_consumed_by_type(cache_type)
        .map_err(|e| format!("df_cache_manager_get_memory_by_type: {}", e))?;
    Ok(size as i64)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_cache_manager_get_total_memory(runtime_ptr: i64) -> i64 {
    if runtime_ptr == 0 {
        return Err("df_cache_manager_get_total_memory: null runtime pointer".to_string());
    }
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime.custom_cache_manager.as_ref()
        .ok_or_else(|| "df_cache_manager_get_total_memory: no cache manager configured".to_string())?;
    Ok(manager.get_total_memory_consumed() as i64)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_cache_manager_contains_by_type(
    runtime_ptr: i64,
    cache_type_ptr: *const u8,
    cache_type_len: i64,
    file_path_ptr: *const u8,
    file_path_len: i64,
) -> i64 {
    if runtime_ptr == 0 {
        return Err("df_cache_manager_contains_by_type: null runtime pointer".to_string());
    }
    let cache_type = str_from_raw(cache_type_ptr, cache_type_len)
        .map_err(|e| format!("df_cache_manager_contains_by_type: cache_type: {}", e))?;
    let file_path = str_from_raw(file_path_ptr, file_path_len)
        .map_err(|e| format!("df_cache_manager_contains_by_type: file_path: {}", e))?;
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime.custom_cache_manager.as_ref()
        .ok_or_else(|| "df_cache_manager_contains_by_type: no cache manager configured".to_string())?;
    Ok(if manager.contains_file_by_type(file_path, cache_type) { 1 } else { 0 })
}
