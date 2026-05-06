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
    let spill_dir = str_from_raw(spill_dir_ptr, spill_dir_len)
        .map_err(|e| format!("df_create_global_runtime: {}", e))?;
    api::create_global_runtime(memory_pool_limit, cache_manager_ptr, spill_dir, spill_limit)
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_global_runtime(ptr: i64) {
    api::close_global_runtime(ptr);
}

// ---- Memory pool observability and dynamic limit ----

/// Returns current memory pool usage in bytes.
/// Java: MethodHandle(JAVA_LONG → JAVA_LONG)
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_get_memory_pool_usage(runtime_ptr: i64) -> i64 {
    if runtime_ptr == 0 {
        return Err("null runtime pointer".to_string());
    }
    Ok(api::get_memory_pool_usage(runtime_ptr))
}

/// Returns current memory pool limit in bytes.
/// Java: MethodHandle(JAVA_LONG → JAVA_LONG)
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_get_memory_pool_limit(runtime_ptr: i64) -> i64 {
    if runtime_ptr == 0 {
        return Err("null runtime pointer".to_string());
    }
    Ok(api::get_memory_pool_limit(runtime_ptr))
}

/// Sets the memory pool limit at runtime. Takes effect for new allocations only.
/// Java: MethodHandle(JAVA_LONG, JAVA_LONG → JAVA_LONG)
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_set_memory_pool_limit(runtime_ptr: i64, new_limit: i64) -> i64 {
    if runtime_ptr == 0 {
        return Err("null runtime pointer".to_string());
    }
    api::set_memory_pool_limit(runtime_ptr, new_limit)?;
    Ok(0)
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
    let table_path = str_from_raw(table_path_ptr, table_path_len)
        .map_err(|e| format!("df_create_reader: {}", e))?;
    let mut filenames = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        filenames.push(
            str_from_raw(ptr, len)
                .map_err(|e| format!("df_create_reader: {}", e))?
                .to_string(),
        );
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
    context_id: i64,
    // Pointer to a `WireDatafusionQueryConfig`. `0` = use defaults.
    query_config_ptr: i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_execute_query: {}", e))?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);
    let query_config =
        crate::datafusion_query_config::DatafusionQueryConfig::from_ffm_ptr(query_config_ptr);
    mgr.io_runtime
        .block_on(api::execute_query(
            shard_view_ptr,
            table_name,
            plan_bytes,
            runtime_ptr,
            &mgr,
            context_id,
            query_config,
        ))
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

#[no_mangle]
pub extern "C" fn df_cancel_query(context_id: i64) {
    api::cancel_query(context_id);
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
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_sql_to_substrait: table_name: {}", e))?;
    let sql =
        str_from_raw(sql_ptr, sql_len).map_err(|e| format!("df_sql_to_substrait: sql: {}", e))?;
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

// ---------------------------------------------------------------------------
// Coordinator-reduce local execution exports
//
// Mirror the shard-scan exports above: fallible entry points use `#[ffm_safe]`
// so `Err(String)` returns are converted into a negated heap-allocated error
// string pointer that `NativeCall.invoke` reads and frees on the Java side.
// Close functions are infallible and do not use the macro. The output stream
// returned by `df_execute_local_plan` is the same `QueryStreamHandle` shape
// as `df_execute_query`, so it drains through the existing `df_stream_next` /
// `df_stream_close` paths unchanged.
// ---------------------------------------------------------------------------

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_local_session(runtime_ptr: i64) -> i64 {
    api::create_local_session(runtime_ptr).map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_local_session(ptr: i64) {
    api::close_local_session(ptr);
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
pub unsafe extern "C" fn df_register_partition_stream(
    session_ptr: i64,
    input_id_ptr: *const u8,
    input_id_len: i64,
    schema_ipc_ptr: *const u8,
    schema_ipc_len: i64,
) -> i64 {
    let input_id = str_from_raw(input_id_ptr, input_id_len)
        .map_err(|e| format!("df_register_partition_stream: input_id: {}", e))?;
    let schema_ipc = slice::from_raw_parts(schema_ipc_ptr, schema_ipc_len as usize);
    api::register_partition_stream(session_ptr, input_id, schema_ipc).map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_local_plan(
    session_ptr: i64,
    substrait_ptr: *const u8,
    substrait_len: i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    // Copy substrait bytes into an owned Vec so the spawned future can move them
    // (cpu_executor.spawn requires 'static). Clone the manager Arc twice — once for
    // the inner future to access the runtime env / etc., once for the outer block_on
    // closure to call `cpu_executor().spawn`.
    let bytes_vec = slice::from_raw_parts(substrait_ptr, substrait_len as usize).to_vec();
    let mgr_for_inner = Arc::clone(&mgr);
    let mgr_for_spawn = Arc::clone(&mgr);
    // Wrap plan setup in cpu_executor.spawn so internal DataFusion spawns
    // (RepartitionExec drain, CoalescePartitionsExec, etc.) inherit the CPU executor
    // instead of the IO runtime. Without this, operator hash work runs on IO workers.
    // The IO runtime still drives the outer block_on (bridging the synchronous FFI
    // call to the async spawn handle).
    mgr.io_runtime
        .block_on(async move {
            let inner_fut = async move {
                unsafe { api::execute_local_plan(session_ptr, &bytes_vec, &mgr_for_inner, 0).await }
            };
            match mgr_for_spawn.cpu_executor().spawn(inner_fut).await {
                Ok(inner_result) => inner_result,
                Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                    "execute_local_plan: CPU spawn failed: {e:?}"
                ))),
            }
        })
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_sender_send(sender_ptr: i64, array_ptr: i64, schema_ptr: i64) -> i64 {
    let mgr = get_rt_manager()?;
    api::sender_send(sender_ptr, array_ptr, schema_ptr, mgr.io_runtime.handle())
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_sender_close(sender_ptr: i64) {
    api::sender_close(sender_ptr);
}

/// Memtable variant of `df_register_partition_stream`: instead of returning a
/// sender that streams batches one at a time, the caller hands across `n`
/// already-exported Arrow C Data batches in two parallel pointer arrays and
/// the native side constructs a [`MemTable`] in one shot.
///
/// `array_ptrs` and `schema_ptrs` must each point to an `n`-element array of
/// `i64`s, where each pair `(array_ptrs[i], schema_ptrs[i])` is a populated
/// `FFI_ArrowArray` / `FFI_ArrowSchema` pair owned by the caller. On success
/// Rust takes ownership; on error the structs are dropped on the Rust side.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_register_memtable(
    session_ptr: i64,
    input_id_ptr: *const u8,
    input_id_len: i64,
    schema_ipc_ptr: *const u8,
    schema_ipc_len: i64,
    array_ptrs: *const i64,
    schema_ptrs: *const i64,
    n_batches: i64,
) -> i64 {
    let input_id = str_from_raw(input_id_ptr, input_id_len)
        .map_err(|e| format!("df_register_memtable: input_id: {}", e))?;
    let schema_ipc = slice::from_raw_parts(schema_ipc_ptr, schema_ipc_len as usize);
    let n = n_batches as usize;
    let array_slice: &[i64] = if n == 0 {
        &[]
    } else {
        slice::from_raw_parts(array_ptrs, n)
    };
    let schema_slice: &[i64] = if n == 0 {
        &[]
    } else {
        slice::from_raw_parts(schema_ptrs, n)
    };
    api::register_memtable(session_ptr, input_id, schema_ipc, array_slice, schema_slice)
        .map(|_| 0)
        .map_err(|e| e.to_string())
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
    let eviction_type = str_from_raw(eviction_type_ptr, eviction_type_len)
        .map_err(|e| format!("df_create_cache: eviction_type: {}", e))?;

    let policy_type = match eviction_type.to_uppercase().as_str() {
        "LRU" => PolicyType::Lru,
        "LFU" => PolicyType::Lfu,
        _ => return Err(format!("df_create_cache: unsupported eviction type: {}", eviction_type)),
    };

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
                policy_type,
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

// ---------------------------------------------------------------------------
// SessionContext decomposition — instruction-based execution
// ---------------------------------------------------------------------------

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_session_context(
    shard_view_ptr: i64,
    runtime_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    context_id: i64,
) -> i64 {
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_create_session_context: {}", e))?;
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(crate::session_context::create_session_context(
            runtime_ptr, shard_view_ptr, table_name, context_id,
        ))
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_session_context_indexed(
    shard_view_ptr: i64,
    runtime_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    context_id: i64,
    tree_shape: i32,
    delegated_predicate_count: i32,
) -> i64 {
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_create_session_context_indexed: {}", e))?;
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(crate::session_context::create_session_context_indexed(
            runtime_ptr, shard_view_ptr, table_name, context_id, tree_shape, delegated_predicate_count,
        ))
        .map_err(|e| e.to_string())
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

#[no_mangle]
pub unsafe extern "C" fn df_close_session_context(ptr: i64) {
    crate::session_context::close_session_context(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_with_context(
    session_ctx_ptr: i64,
    plan_ptr: *const u8,
    plan_len: i64,
) -> i64 {
    // Consume the session context handle on entry. Ownership transfers here
    // regardless of whether the remainder of this function succeeds, returns an
    // error via `?`, or panics — RAII (or `catch_unwind` drop-during-unwind)
    // drops `session_handle` and frees the underlying SessionContext resources.
    //
    // This matches the Java-side contract: SessionContextHandle.markConsumed() is
    // invoked in a `finally` after the FFM downcall, so every observable path from
    // Java's perspective ("call.invoke ran") maps to "Rust consumed the handle".
    // If we were to run fallible or panic-prone code (e.g. `get_rt_manager()?`)
    // before Box::from_raw, the handle would leak on those paths.
    let session_handle = *Box::from_raw(session_ctx_ptr as *mut crate::session_context::SessionContextHandle);

    let mgr = get_rt_manager()?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);
    let cpu_executor = mgr.cpu_executor();
    // Route based on whether the session was configured for indexed execution
    let handle_ref = &*(session_ctx_ptr as *const crate::session_context::SessionContextHandle);
    if handle_ref.indexed_config.is_some() {
        mgr.io_runtime
            .block_on(crate::indexed_executor::execute_indexed_with_context(
                session_ctx_ptr,
                plan_bytes.to_vec(),
                cpu_executor,
            ))
            .map_err(|e| e.to_string())
    } else {
        mgr.io_runtime
            .block_on(crate::query_executor::execute_with_context(
                session_ctx_ptr,
                plan_bytes,
                cpu_executor,
            ))
            .map_err(|e| e.to_string())
    }
}
