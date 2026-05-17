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
pub extern "C" fn df_init_runtime_manager(cpu_threads: i32, datanode_multiplier: f64, coordinator_multiplier: f64) {
    let mut guard = TOKIO_RUNTIME_MANAGER.write();
    *guard = Some(Arc::new(RuntimeManager::new(cpu_threads as usize, datanode_multiplier, coordinator_multiplier)));
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
    api::create_reader(table_path, filenames, &mgr, 0).map_err(|e| e.to_string())
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
    // Pointer to a `WireDatafusionQueryConfig`
    query_config_ptr: i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_execute_query: {}", e))?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);
    let query_config =
        crate::datafusion_query_config::DatafusionQueryConfig::from_ffm_ptr(query_config_ptr);

    // Copy the plan bytes so the spawned future can own them (`cpu_executor.spawn`
    // requires `'static`). The `shard_view_ptr`, `runtime_ptr` are raw pointers
    // held live by the caller for the duration of the FFM downcall — safe to
    // capture by value (they are `Copy`).
    let plan_vec = plan_bytes.to_vec();
    let table_name_owned = table_name.to_string();
    let mgr_for_inner = Arc::clone(&mgr);
    let mgr_for_spawn = Arc::clone(&mgr);

    // Wrap plan setup in `cpu_executor.spawn` so DataFusion operators that
    // eagerly spawn in their `execute()` method (RepartitionExec,
    // CoalescePartitionsExec, AggregateExec, ...) inherit the CPU executor
    // instead of the IO runtime. Without this wrap those operator drain tasks
    // land on IO workers at plan-setup time and the IO runtime ends up doing
    // all the work. The IO runtime still drives the outer `block_on`
    // (bridging the synchronous FFM call to the async spawn handle); only
    // the plan construction and stream wrapping hop to CPU.
    mgr.io_runtime
        .block_on(async move {
            let inner_fut = crate::task_monitors::query_execution_monitor().instrument(async move {
                api::execute_query(
                    shard_view_ptr,
                    &table_name_owned,
                    &plan_vec,
                    runtime_ptr,
                    &mgr_for_inner,
                    context_id,
                    query_config,
                )
                .await
            });
            match mgr_for_spawn.cpu_executor().spawn(inner_fut).await {
                Ok(inner) => inner,
                Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                    "df_execute_query: CPU spawn failed: {e:?}"
                ))),
            }
        })
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
        .block_on(crate::task_monitors::stream_next_monitor().instrument(api::stream_next(stream_ptr)))
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
        .block_on(crate::task_monitors::fetch_phase_monitor().instrument(async move {
            let inner_fut = async move {
                unsafe { api::execute_local_plan(session_ptr, &bytes_vec, &mgr_for_inner, 0).await }
            };
            match mgr_for_spawn.cpu_executor().spawn(inner_fut).await {
                Ok(inner_result) => inner_result,
                Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                    "execute_local_plan: CPU spawn failed: {e:?}"
                ))),
            }
        }))
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
        _ => {
            return Err(format!(
                "df_create_cache: unsupported eviction type: {}",
                eviction_type
            ))
        }
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
            return Err(format!(
                "df_create_cache: invalid cache type: {}",
                cache_type
            ));
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
    let manager = runtime
        .custom_cache_manager
        .as_ref()
        .ok_or_else(|| "df_cache_manager_add_files: no cache manager configured".to_string())?;

    let mut file_paths = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        file_paths.push(
            str_from_raw(ptr, len)
                .map_err(|e| format!("df_cache_manager_add_files: {}", e))?
                .to_string(),
        );
    }

    manager
        .add_files(&file_paths)
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
    query_config_ptr: i64,
) -> i64 {
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_create_session_context: {}", e))?;
    let query_config =
        crate::datafusion_query_config::DatafusionQueryConfig::from_ffm_ptr(query_config_ptr);
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(crate::task_monitors::create_context_monitor().instrument(
            crate::session_context::create_session_context(
                runtime_ptr,
                shard_view_ptr,
                table_name,
                context_id,
                query_config,
            )
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
    query_config_ptr: i64,
) -> i64 {
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_create_session_context_indexed: {}", e))?;
    let query_config =
        crate::datafusion_query_config::DatafusionQueryConfig::from_ffm_ptr(query_config_ptr);
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(crate::task_monitors::create_context_monitor().instrument(
            crate::session_context::create_session_context_indexed(
                runtime_ptr, shard_view_ptr, table_name, context_id, tree_shape, delegated_predicate_count, query_config,
            )
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
    let manager = runtime
        .custom_cache_manager
        .as_ref()
        .ok_or_else(|| "df_cache_manager_remove_files: no cache manager configured".to_string())?;

    let mut file_paths = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        file_paths.push(
            str_from_raw(ptr, len)
                .map_err(|e| format!("df_cache_manager_remove_files: {}", e))?
                .to_string(),
        );
    }

    manager
        .remove_files(&file_paths)
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
    let manager = runtime
        .custom_cache_manager
        .as_ref()
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
    let manager = runtime
        .custom_cache_manager
        .as_ref()
        .ok_or_else(|| "df_cache_manager_clear_by_type: no cache manager configured".to_string())?;
    manager
        .clear_cache_type(cache_type)
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
    let manager = runtime.custom_cache_manager.as_ref().ok_or_else(|| {
        "df_cache_manager_get_memory_by_type: no cache manager configured".to_string()
    })?;
    let size = manager
        .get_memory_consumed_by_type(cache_type)
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
    let manager = runtime.custom_cache_manager.as_ref().ok_or_else(|| {
        "df_cache_manager_get_total_memory: no cache manager configured".to_string()
    })?;
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
    let manager = runtime.custom_cache_manager.as_ref().ok_or_else(|| {
        "df_cache_manager_contains_by_type: no cache manager configured".to_string()
    })?;
    Ok(if manager.contains_file_by_type(file_path, cache_type) {
        1
    } else {
        0
    })
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
    let session_handle = *Box::from_raw(session_ctx_ptr as *mut crate::session_context::SessionContextHandle);

    let mgr = get_rt_manager()?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);
    let cpu_executor = mgr.cpu_executor();
    // See `df_execute_query` for the rationale behind wrapping the inner
    // async work in `cpu_executor.spawn`. In short: DataFusion operators
    // (RepartitionExec, CoalescePartitionsExec, AggregateExec) eagerly
    // spawn in `execute()`. Without this wrap those spawns inherit the IO
    // runtime and do all the work there, leaving the CPU runtime idle.
    let plan_vec = plan_bytes.to_vec();
    let cpu_for_cross = cpu_executor.clone();
    let mgr_for_spawn = Arc::clone(&mgr);

    // Route based on whether the session was configured for indexed execution
    if session_handle.indexed_config.is_some() {
        // TODO: refactor execute_indexed_with_context to take SessionContextHandle directly
        let ptr = Box::into_raw(Box::new(session_handle)) as i64;
        mgr.io_runtime
            .block_on(async move {
                let inner_fut = crate::task_monitors::query_execution_monitor().instrument(async move {
                    crate::indexed_executor::execute_indexed_with_context(
                        ptr,
                        plan_vec,
                        cpu_for_cross,
                    ).await
                });
                match mgr_for_spawn.cpu_executor().spawn(inner_fut).await {
                    Ok(inner) => inner,
                    Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                        "df_execute_with_context: CPU spawn failed: {e:?}"
                    ))),
                }
            })
            .map_err(|e| e.to_string())
    } else {
        mgr.io_runtime
            .block_on(async move {
                let inner_fut = crate::task_monitors::query_execution_monitor().instrument(async move {
                    crate::query_executor::execute_with_context(
                        session_handle,
                        &plan_vec,
                        cpu_for_cross,
                    )
                    .await
                });
                eprintln!("[DIAG-GATE] thread={:?} BEFORE cpu_executor.spawn()",
                    std::thread::current().id());
                let result = match mgr_for_spawn.cpu_executor().spawn(inner_fut).await {
                    Ok(inner) => inner,
                    Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                        "df_execute_with_context: CPU spawn failed: {e:?}"
                    ))),
                };
                eprintln!("[DIAG-GATE] thread={:?} AFTER cpu_executor.spawn() completed",
                    std::thread::current().id());
                result
            })
            .map_err(|e| e.to_string())
    }
}


// ---- Stats collection ----

/// Collects all native executor metrics into a caller-provided byte buffer.
///
/// The buffer must have capacity for at least `size_of::<DfStatsBuffer>()` bytes (344).
/// Returns 0 on success.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stats(out_ptr: *mut u8, out_cap: i64) -> i64 {
    use crate::stats::{layout, pack_runtime_metrics, pack_task_monitor, pack_partition_gate, DfStatsBuffer, RuntimeMetricsRepr};
    use crate::task_monitors::{
        query_execution_monitor, stream_next_monitor,
        fetch_phase_monitor, create_context_monitor,
        prepare_partial_plan_monitor, prepare_final_plan_monitor,
        sql_to_substrait_monitor,
    };

    if out_cap < 0 || (out_cap as usize) < layout::BUFFER_BYTE_SIZE {
        return Err(format!(
            "stats buffer too small: need {} but got {}",
            layout::BUFFER_BYTE_SIZE, out_cap
        ));
    }

    let mgr = get_rt_manager()?;

    // IO runtime (always present)
    let io_runtime = pack_runtime_metrics(&mgr.io_monitor, mgr.io_runtime.handle());

    // CPU runtime (optional — zeroed when absent)
    let cpu_runtime = if let Some(ref cpu_mon) = mgr.cpu_monitor {
        if let Some(cpu_handle) = mgr.cpu_executor.handle() {
            pack_runtime_metrics(cpu_mon, &cpu_handle)
        } else {
            RuntimeMetricsRepr::zeroed()
        }
    } else {
        RuntimeMetricsRepr::zeroed()
    };

    let buf = DfStatsBuffer {
        io_runtime,
        cpu_runtime,
        query_execution: pack_task_monitor(query_execution_monitor()),
        stream_next: pack_task_monitor(stream_next_monitor()),
        fetch_phase: pack_task_monitor(fetch_phase_monitor()),
        create_context: pack_task_monitor(create_context_monitor()),
        prepare_partial_plan: pack_task_monitor(prepare_partial_plan_monitor()),
        prepare_final_plan: pack_task_monitor(prepare_final_plan_monitor()),
        sql_to_substrait: pack_task_monitor(sql_to_substrait_monitor()),
        partition_gate: pack_partition_gate(mgr.cpu_executor.concurrency_gate()),
    };

    // Copy struct bytes to caller buffer
    std::ptr::copy_nonoverlapping(
        &buf as *const DfStatsBuffer as *const u8,
        out_ptr,
        std::mem::size_of::<DfStatsBuffer>(),
    );
    Ok(0)
}

// ---------------------------------------------------------------------------
// Distributed aggregate: prepare partial/final plans
// ---------------------------------------------------------------------------

/// Prepares a partial-aggregate physical plan on the session context handle.
///
/// Decodes the Substrait bytes, converts to a physical plan, strips the
/// final-aggregate half, and stores the result on the handle for later
/// execution via `df_execute_with_context`.
///
/// Returns 0 on success; < 0 is a negated error-string pointer.
///
/// # Safety
/// `handle_ptr` must be a valid pointer returned by `df_create_session_context`.
/// `bytes_ptr` must point to `bytes_len` valid bytes of a Substrait plan.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_prepare_partial_plan(
    handle_ptr: i64,
    bytes_ptr: *const u8,
    bytes_len: usize,
) -> i64 {
    let handle = &mut *(handle_ptr as *mut crate::session_context::SessionContextHandle);
    let bytes = slice::from_raw_parts(bytes_ptr, bytes_len);
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(crate::task_monitors::prepare_partial_plan_monitor().instrument(
            crate::session_context::prepare_partial_plan(handle, bytes)
        ))
        .map_err(|e| e.to_string())?;
    Ok(0)
}

/// Prepares a final-aggregate physical plan on a local session.
///
/// Decodes the Substrait bytes, converts to a physical plan, strips the
/// partial-aggregate half, and stores the result on the session for later
/// execution via `df_execute_local_prepared_plan`.
///
/// Returns 0 on success; < 0 is a negated error-string pointer.
///
/// # Safety
/// `session_ptr` must be a valid pointer returned by `df_create_local_session`.
/// `bytes_ptr` must point to `bytes_len` valid bytes of a Substrait plan.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_prepare_final_plan(
    session_ptr: i64,
    bytes_ptr: *const u8,
    bytes_len: usize,
) -> i64 {
    let session = &mut *(session_ptr as *mut crate::local_executor::LocalSession);
    let bytes = slice::from_raw_parts(bytes_ptr, bytes_len);
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(crate::task_monitors::prepare_final_plan_monitor().instrument(
            session.prepare_final_plan(bytes)
        ))
        .map_err(|e| e.to_string())?;
    Ok(0)
}

/// Executes the previously prepared final-aggregate plan on a local session.
///
/// Returns a stream pointer (same shape as `df_execute_local_plan`) that can
/// be drained via `df_stream_next` / `df_stream_close`.
///
/// # Safety
/// `session_ptr` must be a valid pointer returned by `df_create_local_session`
/// with a plan already prepared via `df_prepare_final_plan`.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_local_prepared_plan(session_ptr: i64) -> i64 {
    let session = &*(session_ptr as *const crate::local_executor::LocalSession);
    let mgr = get_rt_manager()?;

    // Acquire coordinator concurrency gate before executing the prepared plan.
    let partition_weight = (num_cpus::get() as u32).max(1);
    let coord_gate = mgr.coordinator_gate().clone();
    let permit = mgr.io_runtime.block_on(
        coord_gate.acquire_many(partition_weight.min(coord_gate.max_permits()))
    );

    // DataFusion's execute_stream is sync, but kicks off RepartitionExec / stream
    // channels that require a Tokio reactor. Enter the IO runtime's context so those
    // operators can register with the reactor.
    let _guard = mgr.io_runtime.enter();
    let df_stream = session.execute_prepared().map_err(|e| e.to_string())?;
    let cross_rt_stream =
        crate::cross_rt_stream::CrossRtStream::new_with_df_error_stream(df_stream, mgr.cpu_executor());
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );
    let query_context = crate::query_tracker::QueryTrackingContext::new(0, session.memory_pool());
    let handle = crate::api::QueryStreamHandle::new(wrapped, query_context, Some(permit));
    Ok(Box::into_raw(Box::new(handle)) as i64)
}
