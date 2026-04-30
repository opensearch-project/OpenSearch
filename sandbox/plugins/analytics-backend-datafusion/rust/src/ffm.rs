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
use crate::runtime_manager::RuntimeManager;

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
    spill_dir_ptr: *const u8,
    spill_dir_len: i64,
    spill_limit: i64,
) -> i64 {
    let spill_dir = str_from_raw(spill_dir_ptr, spill_dir_len).map_err(|e| format!("df_create_global_runtime: {}", e))?;
    api::create_global_runtime(memory_pool_limit, spill_dir, spill_limit)
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
    context_id: i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_execute_query: {}", e))?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);
    mgr.io_runtime
        .block_on(api::execute_query(shard_view_ptr, table_name, plan_bytes, runtime_ptr, &mgr, context_id))
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
