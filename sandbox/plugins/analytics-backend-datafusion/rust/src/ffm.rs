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
use std::time::{Duration, Instant};

use log::warn;
use native_bridge_common::ffm_safe;
use parking_lot::RwLock;

/// Only log block_on durations exceeding this threshold.
const BLOCK_ON_LOG_THRESHOLD: Duration = Duration::from_millis(1);

/// `df_sender_send` return code when the consumer dropped the receiver. Positive so it rides the
/// success half of the FFM contract. MUST match `NativeBridge.SENDER_SEND_RECEIVER_DROPPED`.
const SENDER_SEND_RECEIVER_DROPPED: i64 = 1;

/// Times a block_on call and logs a warning if it exceeds the threshold.
#[inline(always)]
fn timed_block_on<F: std::future::Future>(
    runtime: &tokio::runtime::Runtime,
    op_name: &str,
    future: F,
) -> F::Output {
    let start = Instant::now();
    let result = runtime.block_on(future);
    let elapsed = start.elapsed();
    if elapsed > BLOCK_ON_LOG_THRESHOLD {
        warn!(
            "[blocked-thread] block_on({}) held Java thread for {:?}",
            op_name, elapsed
        );
    }
    result
}

use crate::api;
use crate::api::DataFusionRuntime;
use crate::cache;
use crate::custom_cache_manager::CustomCacheManager;
use crate::datafusion_query_config::InternalSearch;
use crate::eviction_policy::CacheEvictionPolicy;
use crate::runtime_manager::RuntimeManager;
use crate::statistics_cache::CustomStatisticsCache;

use crate::cache::page_index;
use datafusion::execution::cache::DefaultFilesMetadataCache;

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

/// Non-erroring accessor; `None` before init / after shutdown.
pub(crate) fn try_get_rt_manager() -> Option<Arc<RuntimeManager>> {
    TOKIO_RUNTIME_MANAGER.read().clone()
}

#[no_mangle]
pub extern "C" fn df_init_runtime_manager(
    cpu_threads: i32,
    datanode_multiplier: f64,
    coordinator_multiplier: f64,
) {
    let mut guard = TOKIO_RUNTIME_MANAGER.write();
    *guard = Some(Arc::new(RuntimeManager::new(
        cpu_threads as usize,
        datanode_multiplier,
        coordinator_multiplier,
    )));
}

#[no_mangle]
pub extern "C" fn df_shutdown_runtime_manager() {
    let mgr = TOKIO_RUNTIME_MANAGER.write().take();
    if let Some(mgr) = mgr {
        mgr.shutdown();
    }
}

/// Updates the effective permit count of a named concurrency gate.
/// Gate names: "fragment_executor" (targets DedicatedExecutor gate).
///
/// Scale-up is synchronous. Scale-down spawns an async task on the IO
/// runtime to acquire poison permits (may need to wait for in-flight
/// queries to release).
///
/// Java: NativeBridge.updateConcurrencyGate(String, int)
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_update_concurrency_gate(
    gate_name_ptr: *const u8,
    gate_name_len: i64,
    new_max_permits: u32,
) -> i64 {
    let gate_name = str_from_raw(gate_name_ptr, gate_name_len)
        .map_err(|e| format!("df_update_concurrency_gate: {}", e))?;

    let mgr = match get_rt_manager() {
        Ok(m) => m,
        Err(_) => {
            warn!("df_update_concurrency_gate called before runtime init");
            return Ok(0);
        }
    };

    let gate = match gate_name {
        "fragment_executor" => mgr.cpu_executor().concurrency_gate().clone(),
        other => {
            warn!("df_update_concurrency_gate: unknown gate '{}'", other);
            return Ok(0);
        }
    };

    let io_runtime = mgr.io_runtime.clone();
    let gate_name_owned = gate_name.to_string();

    // Spawn the resize on the IO runtime. Scale-up completes immediately;
    // scale-down may need to wait for permits to become available.
    io_runtime.spawn(async move {
        gate.resize(new_max_permits, &gate_name_owned).await;
    });

    Ok(0)
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
    crate::memory_guard::set_pool_limit_for_guard(memory_pool_limit);
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

/// Returns memory pool stats (usage + tripped count) in a single call.
/// Writes [usage_bytes, tripped_count] to the output buffer.
/// Java: MethodHandle(JAVA_LONG, ADDRESS → void)
#[no_mangle]
pub unsafe extern "C" fn df_get_memory_pool_stats(runtime_ptr: i64, out_ptr: *mut i64) {
    if runtime_ptr == 0 || out_ptr.is_null() {
        return;
    }
    api::get_memory_pool_stats(runtime_ptr, out_ptr);
}

/// Sets the memory pool limit at runtime. Takes effect for new allocations only.
/// Java: MethodHandle(JAVA_LONG, JAVA_LONG → JAVA_LONG)
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_set_memory_pool_limit(runtime_ptr: i64, new_limit: i64) -> i64 {
    if runtime_ptr == 0 {
        return Err("null runtime pointer".to_string());
    }
    crate::memory_guard::set_pool_limit_for_guard(new_limit);
    api::set_memory_pool_limit(runtime_ptr, new_limit)?;
    Ok(0)
}

#[no_mangle]
pub extern "C" fn df_set_min_target_partitions(value: i64) {
    api::set_min_target_partitions(value);
}

#[no_mangle]
pub extern "C" fn df_set_reduce_target_partitions(value: i64) {
    api::set_reduce_target_partitions(value);
}

/// Sets the spill-exemption cap in bytes (the total in-flight allocation allowed
/// through the 85% spill gate by spillable consumers so they can finish spilling).
/// Live-tunable; takes effect on the next try_grow. Java: NativeBridge.setSpillExemptCapBytes(long).
#[no_mangle]
pub extern "C" fn df_set_spill_exempt_cap_bytes(value: i64) {
    crate::memory_guard::set_spill_exempt_cap_bytes(value.max(0) as u64);
}

/// Sets memory guard thresholds. Values are thresholds multiplied by 1000
/// (e.g., 700 = 0.70, 850 = 0.85, 950 = 0.95).
#[no_mangle]
pub extern "C" fn df_set_memory_guard_thresholds(
    admission_throttle_x1000: i64,
    admission_reject_x1000: i64,
    execution_spill_x1000: i64,
    execution_critical_x1000: i64,
) {
    crate::memory_guard::set_thresholds(crate::memory_guard::MemoryThresholds {
        admission_throttle: admission_throttle_x1000 as f64 / 1000.0,
        admission_reject: admission_reject_x1000 as f64 / 1000.0,
        execution_spill: execution_spill_x1000 as f64 / 1000.0,
        execution_critical: execution_critical_x1000 as f64 / 1000.0,
    });
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_reader(
    table_path_ptr: *const u8,
    table_path_len: i64,
    files_ptr: *const *const u8,
    files_len_ptr: *const i64,
    writer_generations_ptr: *const i64,
    files_count: i64,
    store_ptr: i64,
    sort_fields_ptr: *const *const u8,
    sort_fields_len_ptr: *const i64,
    sort_orders_ptr: *const *const u8,
    sort_orders_len_ptr: *const i64,
    sort_count: i64,
) -> i64 {
    let table_path = str_from_raw(table_path_ptr, table_path_len)
        .map_err(|e| format!("df_create_reader: {}", e))?;
    let mut filenames = Vec::with_capacity(files_count as usize);
    let mut writer_generations = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        filenames.push(
            str_from_raw(ptr, len)
                .map_err(|e| format!("df_create_reader: {}", e))?
                .to_string(),
        );
        writer_generations.push(*writer_generations_ptr.add(i));
    }
    // Decode parallel sort_fields / sort_orders String arrays. sort_count == 0 means no
    // index sort configured; pass an empty Vec. The Java side guarantees
    // sortFields.size() == sortOrders.size() (IndexSortConfig validates at index creation),
    // so a single sort_count covers both arrays.
    let mut sort_fields = Vec::with_capacity(sort_count as usize);
    let mut sort_orders = Vec::with_capacity(sort_count as usize);
    for i in 0..sort_count as usize {
        let f_ptr = *sort_fields_ptr.add(i);
        let f_len = *sort_fields_len_ptr.add(i);
        sort_fields.push(
            str_from_raw(f_ptr, f_len)
                .map_err(|e| format!("df_create_reader: sort_field[{}]: {}", i, e))?
                .to_string(),
        );
        let o_ptr = *sort_orders_ptr.add(i);
        let o_len = *sort_orders_len_ptr.add(i);
        sort_orders.push(
            str_from_raw(o_ptr, o_len)
                .map_err(|e| format!("df_create_reader: sort_order[{}]: {}", i, e))?
                .to_string(),
        );
    }
    let mgr = get_rt_manager()?;
    api::create_reader(
        table_path,
        filenames,
        writer_generations,
        sort_fields,
        sort_orders,
        &mgr,
        store_ptr,
    )
    .map_err(|e| e.to_string())
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
    // Engine-internal point lookup. 0 = normal query (decode `plan_ptr` as Substrait);
    // 1 = get-by-row-id (`__row_id__ = internal_search_bound`); 2 = seq-no scan
    // (`_seq_no > internal_search_bound`). When non-zero, `plan_ptr`/`plan_len` are
    // ignored and the plan is built natively via the DataFrame API — no Substrait.
    internal_search_mode: i64,
    internal_search_bound: i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_execute_query: {}", e))?;
    let plan_bytes = slice::from_raw_parts(plan_ptr, plan_len as usize);
    let query_config =
        crate::datafusion_query_config::DatafusionQueryConfig::from_ffm_ptr(query_config_ptr);
    let internal_search = InternalSearch::from_wire(internal_search_mode, internal_search_bound);
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
    // all the work. The IO runtime still drives the outer `timed_block_on`
    // (bridging the synchronous FFM call to the async spawn handle); only
    // the plan construction and stream wrapping hop to CPU.
    timed_block_on(&mgr.io_runtime, "execute_query", async move {
        let inner_fut = crate::task_monitors::query_execution_monitor().instrument(async move {
            api::execute_query(
                shard_view_ptr,
                &table_name_owned,
                &plan_vec,
                runtime_ptr,
                &mgr_for_inner,
                context_id,
                query_config,
                internal_search,
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

/// Fetch specific rows by global row ID — QTF fetch phase.
///
/// Row IDs are passed as a direct pointer to i64 values (from BigIntVector's
/// off-heap ArrowBuf). Zero-copy at FFM boundary: Rust reads directly from
/// Java's off-heap buffer without any intermediate allocation.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_fetch_by_row_ids(
    shard_view_ptr: i64,
    row_ids_ptr: i64,
    row_ids_count: i64,
    col_names_ptr: *const *const u8,
    col_names_len_ptr: *const i64,
    col_names_count: i64,
    runtime_ptr: i64,
    context_id: i64,
) -> i64 {
    // Hard FFM-boundary checks (UB risk if violated): pointers must be non-zero before any deref.
    // Always-on `assert!` (not debug_assert!) — these protect against use-after-close from Java.
    assert!(
        shard_view_ptr != 0,
        "df_fetch_by_row_ids: shard_view_ptr is null"
    );
    assert!(runtime_ptr != 0, "df_fetch_by_row_ids: runtime_ptr is null");
    assert!(
        row_ids_count >= 0,
        "df_fetch_by_row_ids: negative row_ids_count {}",
        row_ids_count
    );
    assert!(
        col_names_count >= 0,
        "df_fetch_by_row_ids: negative col_names_count {}",
        col_names_count
    );
    if row_ids_count > 0 {
        assert!(
            row_ids_ptr != 0,
            "df_fetch_by_row_ids: row_ids_ptr is null but count={}",
            row_ids_count
        );
    }
    if col_names_count > 0 {
        assert!(
            !col_names_ptr.is_null(),
            "df_fetch_by_row_ids: col_names_ptr is null but count={}",
            col_names_count
        );
        assert!(
            !col_names_len_ptr.is_null(),
            "df_fetch_by_row_ids: col_names_len_ptr is null but count={}",
            col_names_count
        );
    }

    let mgr = get_rt_manager()?;
    let shard_view = &*(shard_view_ptr as *const crate::api::ShardView);
    let runtime = &*(runtime_ptr as *const crate::api::DataFusionRuntime);

    // Zero-copy read from BigIntVector's direct buffer
    let row_ids: Vec<i64> =
        slice::from_raw_parts(row_ids_ptr as *const i64, row_ids_count as usize).to_vec();

    // Parse column names
    let mut columns: Vec<String> = Vec::with_capacity(col_names_count as usize);
    for i in 0..col_names_count as usize {
        let ptr = *col_names_ptr.add(i);
        let len = *col_names_len_ptr.add(i);
        let name = str_from_raw(ptr, len)
            .map_err(|e| format!("df_fetch_by_row_ids: column name: {}", e))?;
        columns.push(name.to_string());
    }

    mgr.io_runtime
        .block_on(crate::api::fetch_by_row_ids(
            shard_view, runtime, &mgr, row_ids, columns, context_id,
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
    timed_block_on(
        &mgr.io_runtime,
        "stream_next",
        crate::task_monitors::stream_next_monitor().instrument(api::stream_next(stream_ptr)),
    )
    .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_stream_close(stream_ptr: i64) {
    api::stream_close(stream_ptr);
}

/// Returns execution metrics as JSON bytes for the given stream.
/// Writes the pointer to allocated bytes into `out_ptr` and the length into `out_len_ptr`.
/// Returns 0 on success, non-zero if no metrics are available.
/// The caller must free the returned bytes via `df_free_metrics_buf`.
#[no_mangle]
pub unsafe extern "C" fn df_stream_get_metrics(
    stream_ptr: i64,
    out_ptr: *mut *const u8,
    out_len_ptr: *mut i64,
) -> i64 {
    if stream_ptr == 0 {
        return -1;
    }
    let handle = &*(stream_ptr as *const api::QueryStreamHandle);
    match handle.get_metrics_json() {
        Some(bytes) => {
            let len = bytes.len() as i64;
            let boxed = bytes.into_boxed_slice();
            let ptr = Box::into_raw(boxed) as *const u8;
            *out_ptr = ptr;
            *out_len_ptr = len;
            0
        }
        None => -1,
    }
}

/// Frees a metrics buffer previously returned by `df_stream_get_metrics`.
#[no_mangle]
pub unsafe extern "C" fn df_free_metrics_buf(ptr: *mut u8, len: i64) {
    if !ptr.is_null() && len > 0 {
        let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len as usize));
    }
}

#[no_mangle]
pub extern "C" fn df_cancel_query(context_id: i64) {
    api::cancel_query(context_id);
}

/// Sets the cancellation stats threshold in milliseconds.
/// Queries cancelled for less than this duration are not counted in stats.
#[no_mangle]
pub extern "C" fn df_set_cancel_stats_threshold_ms(millis: i64) {
    crate::query_tracker::set_cancel_stats_threshold(millis as u64);
}

// ---------------------------------------------------------------------------
// Per-query registry top-N snapshot
//
// One FFM call: Java allocates a buffer sized for `N` entries, Rust selects
// the heaviest live queries by `current_bytes` (bounded min-heap of size N)
// and writes them back-to-back. See `query_tracker::WireQueryMetric` for the
// wire layout.
// ---------------------------------------------------------------------------

/// Copies up to `cap_entries` of the heaviest live queries (by
/// `current_bytes` desc) as `WireQueryMetric`s into the caller-provided buffer.
/// Returns the number of entries actually written.
///
/// Order of entries within the buffer is unspecified. Completed and zero-byte
/// trackers are filtered out.
///
/// Safety: `out_ptr` must be non-null, 8-byte aligned, and point to storage
/// for at least `cap_entries * size_of::<WireQueryMetric>()` bytes.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_query_registry_top_n_by_current(
    out_ptr: *mut u8,
    cap_entries: i64,
) -> i64 {
    use crate::query_tracker::{snapshot_top_n_by_current, WireQueryMetric};

    if cap_entries < 0 {
        return Err(format!("negative capacity: {cap_entries}"));
    }
    if cap_entries == 0 {
        return Ok(0);
    }
    if out_ptr.is_null() {
        return Err("null snapshot buffer".to_string());
    }
    let out: &mut [WireQueryMetric] =
        slice::from_raw_parts_mut(out_ptr as *mut WireQueryMetric, cap_entries as usize);
    let written = snapshot_top_n_by_current(out);
    Ok(written as i64)
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
    write_out_buffer(&bytes, out_ptr, out_cap, out_len, "substrait plan")?;
    Ok(0)
}

/// Copies `bytes` into a caller-allocated `(out_ptr, out_cap)` buffer and writes
/// the byte count through `out_len` (when non-null). Returns `Err` when the
/// buffer is too small — the caller can re-allocate and retry.
unsafe fn write_out_buffer(
    bytes: &[u8],
    out_ptr: *mut u8,
    out_cap: i64,
    out_len: *mut i64,
    label: &str,
) -> Result<(), String> {
    if bytes.len() > out_cap as usize {
        return Err(format!(
            "{} size {} exceeds buffer capacity {}",
            label,
            bytes.len(),
            out_cap
        ));
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
    if !out_len.is_null() {
        *out_len = bytes.len() as i64;
    }
    Ok(())
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

/// Registers a streaming partition input on the session. Schema is derived by
/// lowering the producer-side substrait `partial_plan_bytes`; the resulting
/// IPC-encoded schema is written into the caller-allocated `out_ptr/out_cap`
/// buffer with the byte count written through `out_len`. Returns the sender
/// pointer (negated error pointer on failure).
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_register_partition_stream(
    session_ptr: i64,
    input_id_ptr: *const u8,
    input_id_len: i64,
    partial_plan_ptr: *const u8,
    partial_plan_len: i64,
    out_ptr: *mut u8,
    out_cap: i64,
    out_len: *mut i64,
) -> i64 {
    let input_id = str_from_raw(input_id_ptr, input_id_len)
        .map_err(|e| format!("df_register_partition_stream: input_id: {}", e))?;
    let partial_plan = slice::from_raw_parts(partial_plan_ptr, partial_plan_len as usize);
    let (sender_ptr, schema_ipc) =
        api::register_partition_stream(session_ptr, input_id, partial_plan)
            .map_err(|e| e.to_string())?;
    write_out_buffer(
        &schema_ipc,
        out_ptr,
        out_cap,
        out_len,
        "register_partition_stream schema IPC",
    )?;
    Ok(sender_ptr)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_local_plan(
    session_ptr: i64,
    substrait_ptr: *const u8,
    substrait_len: i64,
    context_id: i64,
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
    timed_block_on(
        &mgr.io_runtime,
        "execute_local_plan",
        crate::task_monitors::coordinator_reduce_monitor().instrument(async move {
            // No coordinator-gate acquire here. The QTF coordinator-reduce code path runs
            // synchronously inside the SEARCH-thread FFM call (DatafusionReduceSink.<init>);
            // gating it would deadlock when the gate is contended because the SEARCH thread
            // is blocked waiting for permits its own work would release. Keep the gate
            // exclusively on the data-node FFM entry points.
            let inner_fut = async move {
                unsafe {
                    api::execute_local_plan(
                        session_ptr,
                        &bytes_vec,
                        &mgr_for_inner,
                        context_id,
                        None,
                    )
                    .await
                }
            };
            match mgr_for_spawn.cpu_executor().spawn(inner_fut).await {
                Ok(inner_result) => inner_result,
                Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                    "execute_local_plan: CPU spawn failed: {e:?}"
                ))),
            }
        }),
    )
    .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_sender_send(sender_ptr: i64, array_ptr: i64, schema_ptr: i64) -> i64 {
    let mgr = get_rt_manager()?;
    api::sender_send(sender_ptr, array_ptr, schema_ptr, mgr.io_runtime.handle())
        .map(send_outcome_to_code)
        .map_err(|e| e.to_string())
}

/// Maps a send outcome to the `df_sender_send` return code: normal send `0`, dropped receiver
/// [`SENDER_SEND_RECEIVER_DROPPED`] so the Java side can latch early-termination.
fn send_outcome_to_code(outcome: crate::partition_stream::SendOutcome) -> i64 {
    match outcome {
        crate::partition_stream::SendOutcome::Sent => 0,
        crate::partition_stream::SendOutcome::ReceiverDropped => SENDER_SEND_RECEIVER_DROPPED,
    }
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
    partial_plan_ptr: *const u8,
    partial_plan_len: i64,
    array_ptrs: *const i64,
    schema_ptrs: *const i64,
    n_batches: i64,
    out_ptr: *mut u8,
    out_cap: i64,
    out_len: *mut i64,
) -> i64 {
    let input_id = str_from_raw(input_id_ptr, input_id_len)
        .map_err(|e| format!("df_register_memtable: input_id: {}", e))?;
    let partial_plan = slice::from_raw_parts(partial_plan_ptr, partial_plan_len as usize);
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
    let schema_ipc = api::register_memtable(
        session_ptr,
        input_id,
        partial_plan,
        array_slice,
        schema_slice,
    )
    .map_err(|e| e.to_string())?;
    write_out_buffer(
        &schema_ipc,
        out_ptr,
        out_cap,
        out_len,
        "register_memtable schema IPC",
    )?;
    Ok(0)
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

    // Parse the eviction type string into the unified CacheEvictionPolicy enum.
    // All four cache types share one enum — the per-cache match below enforces
    // which policies are valid for each type.
    let policy = match eviction_type.to_uppercase().as_str() {
        "LRU" => CacheEvictionPolicy::Lru,
        "LFU" => CacheEvictionPolicy::Lfu,
        "FIFO" => CacheEvictionPolicy::Fifo,
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
            // METADATA uses DefaultFilesMetadataCache (has its own LRU); eviction
            // type is accepted but not forwarded.
            let inner_cache = DefaultFilesMetadataCache::new(size_limit as usize);
            let metadata_cache = Arc::new(cache::MutexFileMetadataCache::new(inner_cache));
            manager.set_file_metadata_cache(metadata_cache);
        }
        cache::CACHE_TYPE_STATS => {
            if policy == CacheEvictionPolicy::Fifo {
                return Err(
                    "df_create_cache: STATISTICS cache does not support FIFO eviction".to_string(),
                );
            }
            let stats_cache =
                Arc::new(CustomStatisticsCache::new(policy, size_limit as usize, 0.8));
            manager.set_statistics_cache(stats_cache);
        }
        cache::CACHE_TYPE_COLUMN_INDEX => {
            // CI/OI use BoundedCache<FIFO>; eviction type must be FIFO.
            if policy != CacheEvictionPolicy::Fifo {
                return Err(format!("df_create_cache: COLUMN_INDEX cache only supports FIFO eviction, got {eviction_type}"));
            }
            manager.set_column_index_cache(size_limit as usize);
        }
        cache::CACHE_TYPE_OFFSET_INDEX => {
            if policy != CacheEvictionPolicy::Fifo {
                return Err(format!("df_create_cache: OFFSET_INDEX cache only supports FIFO eviction, got {eviction_type}"));
            }
            manager.set_offset_index_cache(size_limit as usize);
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

    let rt_manager = get_rt_manager().map_err(|e| format!("df_cache_manager_add_files: {}", e))?;
    let rt_handle = rt_manager.io_runtime.handle();

    manager
        .add_files(&file_paths, rt_handle)
        .map_err(|e| format!("df_cache_manager_add_files: {}", e))?;
    Ok(0)
}

/// Warmup: load footer (lightweight) into heap and promote footer + page/offset
/// index bytes to the metadata Foyer tier (never-evict) through the store.
///
/// After this call:
/// - file_metadata_cache (heap): lightweight ParquetMetaData (footer only, no page indexes)
/// - data Foyer: raw footer + page index bytes (via get_ranges populate)
/// - metadata Foyer: the same ranges promoted via `MetadataCachingStore::put_metadata`
///
/// Promotion happens entirely in Rust inside `CustomCacheManager::add_files_with_store`;
/// the Java caller only supplies the file paths.
///
/// # Safety
/// - `runtime_ptr` must be a valid pointer from `df_create_global_runtime`.
/// - `store_ptr` must be a valid `Box<Arc<dyn MetadataCachingStore>>` pointer (produced by
///   `ts_get_object_store_box_ptr`).
/// - `files_ptr[i]` must point to `files_len_ptr[i]` valid UTF-8 bytes.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_cache_manager_add_files_with_store(
    runtime_ptr: i64,
    store_ptr: i64,
    files_ptr: *const *const u8,
    files_len_ptr: *const i64,
    files_count: i64,
) -> i64 {
    if runtime_ptr == 0 {
        return Err("df_cache_manager_add_files_with_store: null runtime pointer".to_string());
    }
    if store_ptr == 0 {
        return Err("df_cache_manager_add_files_with_store: null store pointer".to_string());
    }

    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime
        .custom_cache_manager
        .as_ref()
        .ok_or_else(|| "df_cache_manager_add_files_with_store: no cache manager".to_string())?;

    // Pointer type is `Arc<dyn MetadataCachingStore>`; the manager calls `put_metadata`
    // directly via the trait, no downcast needed.
    let store_box = &*(store_ptr
        as *const std::sync::Arc<
            dyn opensearch_tiered_storage::tiered_object_store::MetadataCachingStore,
        >);
    let store = std::sync::Arc::clone(store_box);

    let mut file_paths = Vec::with_capacity(files_count as usize);
    for i in 0..files_count as usize {
        let ptr = *files_ptr.add(i);
        let len = *files_len_ptr.add(i);
        file_paths.push(
            str_from_raw(ptr, len)
                .map_err(|e| format!("df_cache_manager_add_files_with_store: {}", e))?
                .to_string(),
        );
    }

    let rt_manager =
        get_rt_manager().map_err(|e| format!("df_cache_manager_add_files_with_store: {}", e))?;
    let rt_handle = rt_manager.io_runtime.handle();

    let results = manager
        .add_files_with_store(&file_paths, store, rt_handle)
        .map_err(|e| format!("df_cache_manager_add_files_with_store: {}", e))?;

    // Log summary
    let success_count = results.iter().filter(|(_, ok)| *ok).count();
    native_bridge_common::log_info!(
        "df_cache_manager_add_files_with_store: {} files, {} warmed (page-index promoted to metadata Foyer)",
        files_count, success_count
    );

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
    has_partial_aggregate: u8,
    plan_ptr: *const u8,
    plan_len: i64,
) -> i64 {
    crate::search_stats::inc_listing_table_scan();
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_create_session_context: {}", e))?;
    let query_config =
        crate::datafusion_query_config::DatafusionQueryConfig::from_ffm_ptr(query_config_ptr);
    let plan_bytes: &[u8] = if plan_len > 0 {
        slice::from_raw_parts(plan_ptr, plan_len as usize)
    } else {
        &[]
    };
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(crate::task_monitors::plan_setup_monitor().instrument(
            crate::session_context::create_session_context(
                runtime_ptr,
                shard_view_ptr,
                table_name,
                context_id,
                has_partial_aggregate != 0,
                query_config,
                plan_bytes,
            ),
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
    requests_row_ids: u8,
    has_partial_aggregate: u8,
    query_config_ptr: i64,
    plan_ptr: *const u8,
    plan_len: i64,
) -> i64 {
    match tree_shape {
        1 => crate::search_stats::inc_single_collector_scan(),
        2 => crate::search_stats::inc_bitmap_tree_scan(),
        _ => {}
    }
    let table_name = str_from_raw(table_name_ptr, table_name_len)
        .map_err(|e| format!("df_create_session_context_indexed: {}", e))?;
    let query_config =
        crate::datafusion_query_config::DatafusionQueryConfig::from_ffm_ptr(query_config_ptr);
    let plan_bytes: &[u8] = if plan_len > 0 {
        slice::from_raw_parts(plan_ptr, plan_len as usize)
    } else {
        &[]
    };
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(crate::task_monitors::plan_setup_monitor().instrument(
            crate::session_context::create_session_context_indexed(
                runtime_ptr,
                shard_view_ptr,
                table_name,
                context_id,
                tree_shape,
                delegated_predicate_count,
                requests_row_ids != 0,
                has_partial_aggregate != 0,
                query_config,
                plan_bytes,
            ),
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

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_cache_manager_update_size_limit(
    runtime_ptr: i64,
    cache_type_ptr: *const u8,
    cache_type_len: i64,
    new_limit: i64,
) -> i64 {
    if runtime_ptr == 0 {
        return Err("df_cache_manager_update_size_limit: null runtime pointer".to_string());
    }
    if new_limit < 0 {
        return Err(format!(
            "df_cache_manager_update_size_limit: negative limit {}",
            new_limit
        ));
    }
    let cache_type = str_from_raw(cache_type_ptr, cache_type_len)
        .map_err(|e| format!("df_cache_manager_update_size_limit: {}", e))?;
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let manager = runtime.custom_cache_manager.as_ref().ok_or_else(|| {
        "df_cache_manager_update_size_limit: no cache manager configured".to_string()
    })?;
    match cache_type {
        cache::CACHE_TYPE_METADATA => {
            manager.update_metadata_cache_limit(new_limit as usize);
            Ok(0)
        }
        cache::CACHE_TYPE_STATS => {
            manager
                .update_statistics_cache_limit(new_limit as usize)
                .map_err(|e| format!("df_cache_manager_update_size_limit: {}", e))?;
            Ok(0)
        }
        _ => Err(format!(
            "df_cache_manager_update_size_limit: unsupported cache type: {}",
            cache_type
        )),
    }
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
    let session_handle =
        *Box::from_raw(session_ctx_ptr as *mut crate::session_context::SessionContextHandle);

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

    // Route based on whether the session was configured for indexed execution,
    // or if the plan projects __row_id__ (QTF query phase). QTF row-id computation
    // always runs in the indexed executor.
    let has_row_id = plan_bytes
        .windows(crate::ROW_ID_COLUMN_NAME.len())
        .any(|w| w == crate::ROW_ID_COLUMN_NAME.as_bytes());
    let use_indexed = session_handle.indexed_config.is_some() || has_row_id;
    if use_indexed {
        // Extract target_partitions BEFORE boxing into raw pointer (session_handle is consumed).
        let partition_weight = session_handle.query_config.target_partitions.max(1) as u32;
        // TODO: refactor execute_indexed_with_context to take SessionContextHandle directly
        let ptr = Box::into_raw(Box::new(session_handle)) as i64;
        mgr.io_runtime
            .block_on(async move {
                // Acquire datanode gate on IO runtime BEFORE spawning on CPU.
                // This blocks the IO thread (and thus the Java search thread),
                // creating backpressure at the Java threadpool level when the gate is full.
                let gate = mgr_for_spawn.cpu_executor().concurrency_gate().clone();
                let max_p = gate.max_permits();
                let permit = gate.acquire_many(partition_weight.min(max_p)).await;

                let inner_fut =
                    crate::task_monitors::query_execution_monitor().instrument(async move {
                        crate::indexed_executor::execute_indexed_with_context(
                            ptr,
                            plan_vec,
                            cpu_for_cross,
                            permit,
                        )
                        .await
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
        // Extract target_partitions before moving session_handle into the closure.
        let partition_weight = session_handle.query_config.target_partitions.max(1) as u32;
        mgr.io_runtime
            .block_on(async move {
                // Acquire datanode gate on IO runtime BEFORE spawning on CPU.
                // This blocks the IO thread (and thus the Java search thread),
                // creating backpressure at the Java threadpool level when the gate is full.
                let gate = mgr_for_spawn.cpu_executor().concurrency_gate().clone();
                let max_p = gate.max_permits();
                let permit = gate.acquire_many(partition_weight.min(max_p)).await;

                let inner_fut =
                    crate::task_monitors::query_execution_monitor().instrument(async move {
                        crate::query_executor::execute_with_context(
                            session_handle,
                            &plan_vec,
                            cpu_for_cross,
                            permit,
                        )
                        .await
                    });
                match mgr_for_spawn.cpu_executor().spawn(inner_fut).await {
                    Ok(inner) => inner,
                    Err(e) => Err(datafusion::error::DataFusionError::Execution(format!(
                        "df_execute_with_context: CPU spawn failed: {e:?}"
                    ))),
                }
            })
            .map_err(|e| e.to_string())
    }
}

// ---- Stats collection ----

/// Collects all native executor metrics into a caller-provided byte buffer.
///
/// `runtime_ptr` may be `0` to skip cache-stats collection. When non-zero it
/// must be a valid pointer returned by [`df_create_global_runtime`].
///
/// The buffer must have capacity for at least `size_of::<DfStatsBuffer>()` bytes (600).
/// Returns 0 on success.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stats(runtime_ptr: i64, out_ptr: *mut u8, out_cap: i64) -> i64 {
    use crate::stats::{
        layout, pack_adaptive_budget, pack_cache_stats, pack_partition_gate, pack_runtime_metrics,
        pack_task_monitor, CacheStatsRepr, DfStatsBuffer, RuntimeMetricsRepr,
    };
    use crate::task_monitors::{
        coordinator_reduce_monitor, plan_setup_monitor, query_execution_monitor,
        stream_next_monitor,
    };

    if out_cap < 0 || (out_cap as usize) < layout::BUFFER_BYTE_SIZE {
        return Err(format!(
            "stats buffer too small: need {} but got {}",
            layout::BUFFER_BYTE_SIZE,
            out_cap
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

    // Cache stats (zeroed when no runtime pointer or no cache manager)
    let cache_stats = if runtime_ptr != 0 {
        let runtime = &*(runtime_ptr as *const DataFusionRuntime);
        runtime
            .custom_cache_manager
            .as_ref()
            .map(pack_cache_stats)
            .unwrap_or_else(CacheStatsRepr::default)
    } else {
        CacheStatsRepr::default()
    };

    let buf = DfStatsBuffer {
        io_runtime,
        cpu_runtime,
        coordinator_reduce: pack_task_monitor(coordinator_reduce_monitor()),
        query_execution: pack_task_monitor(query_execution_monitor()),
        stream_next: pack_task_monitor(stream_next_monitor()),
        plan_setup: pack_task_monitor(plan_setup_monitor()),
        fragment_executor_gate: pack_partition_gate(mgr.cpu_executor.concurrency_gate()),
        adaptive_budget: pack_adaptive_budget(),
        cache_stats,
        search_stats: crate::search_stats::snapshot(),
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
        .block_on(
            crate::task_monitors::plan_setup_monitor()
                .instrument(crate::session_context::prepare_partial_plan(handle, bytes)),
        )
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
        .block_on(
            crate::task_monitors::plan_setup_monitor()
                .instrument(session.prepare_final_plan(bytes)),
        )
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
pub unsafe extern "C" fn df_execute_local_prepared_plan(session_ptr: i64, context_id: i64) -> i64 {
    let mgr = get_rt_manager()?;
    // No coordinator-gate acquire here — see df_execute_local_plan for the rationale
    // (the QTF coordinator-reduce path runs synchronously inside the SEARCH-thread FFM
    // call and gating it can deadlock).
    api::execute_local_prepared_plan(session_ptr, &mgr, context_id, None).map_err(|e| e.to_string())
}

// ── Scoped page-index cache limit setters ────────────────────────────────────
//
// These are wired from Java at startup (CacheUtils.createCacheConfig) and on
// dynamic setting changes (DataFusionPlugin settings consumers). They forward
// to the process-global caches in crate::cache::page_index.
//
// NOTE: On main these are stubs that do nothing — the cache module from PR 1
// is not yet present. Once PR 1 merges the bodies replace these no-ops.

/// Set the byte budget of the process-global scoped ColumnIndex cache.
/// Zero is ignored; negative returns an error.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn df_set_column_index_cache_limit(size_limit: i64) -> i64 {
    if size_limit < 0 {
        return Err(format!(
            "df_set_column_index_cache_limit: negative limit {}",
            size_limit
        ));
    }
    page_index::set_column_index_cache_limit(size_limit as usize);
    Ok(0)
}

/// Set the byte budget of the process-global scoped OffsetIndex cache.
/// Zero is ignored; negative returns an error.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn df_set_offset_index_cache_limit(size_limit: i64) -> i64 {
    if size_limit < 0 {
        return Err(format!(
            "df_set_offset_index_cache_limit: negative limit {}",
            size_limit
        ));
    }
    page_index::set_offset_index_cache_limit(size_limit as usize);
    Ok(0)
}

/// Clear the process-global scoped page-index caches (drop entries + reset counters).
#[ffm_safe]
#[no_mangle]
pub extern "C" fn df_clear_scoped_page_index_cache() -> i64 {
    page_index::clear_scoped_cache();
    Ok(0)
}

/// Enable or disable the scoped page-index feature.
/// When disabled: metadata cache retains full page index (fallback mode).
/// When enabled (default): metadata cache strips page index; scoped caches handle it.
#[ffm_safe]
#[no_mangle]
pub extern "C" fn df_set_scoped_page_index_enabled(enabled: i64) -> i64 {
    page_index::set_scoped_page_index_enabled(enabled != 0);
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_stream::SendOutcome;

    #[test]
    fn send_outcome_maps_sent_to_zero() {
        assert_eq!(send_outcome_to_code(SendOutcome::Sent), 0);
    }

    #[test]
    fn send_outcome_maps_receiver_dropped_to_sentinel() {
        // Must surface the sentinel, not collapse to 0 like a normal send, or Java never latches
        // isConsumerDone().
        assert_eq!(
            send_outcome_to_code(SendOutcome::ReceiverDropped),
            SENDER_SEND_RECEIVER_DROPPED
        );
        assert_eq!(SENDER_SEND_RECEIVER_DROPPED, 1);
    }

    /// Initialize the global runtime manager for tests.
    /// Uses 2 CPU threads and 1.5 multiplier (default) for both gates.
    fn init_test_runtime() {
        df_init_runtime_manager(2, 1.5, 1.5);
    }

    /// Shutdown and clear the global runtime manager after tests.
    /// Must be called from a blocking context (not inside an async runtime).
    fn shutdown_test_runtime() {
        df_shutdown_runtime_manager();
    }

    /// Helper: call df_update_concurrency_gate with a Rust string.
    /// Returns the i64 result (0 = success for the outer call).
    unsafe fn call_update_gate(gate_name: &str, new_max: u32) -> i64 {
        df_update_concurrency_gate(gate_name.as_ptr(), gate_name.len() as i64, new_max)
    }

    /// Validates: Requirements 2.2, 2.4, 2.6
    ///
    /// Combined test for FFI gate routing to avoid global state conflicts
    /// between parallel test threads. Tests are run sequentially within this
    /// function since they all share the TOKIO_RUNTIME_MANAGER global.
    ///
    /// Covers:
    /// - "fragment_executor" routes to the DedicatedExecutor's gate (Req 2.2)
    /// - Unknown gate name logs warning and returns success (Req 2.4)
    /// - Calling update before runtime init returns success (Req 2.6)
    #[test]
    fn test_ffi_gate_routing() {
        // ── Test 1: update before runtime init returns success (Req 2.6) ──
        shutdown_test_runtime(); // ensure clean state
        let result = unsafe { call_update_gate("fragment_executor", 10) };
        assert_eq!(
            result, 0,
            "FFI call should return success even before runtime init"
        );

        // ── Initialize runtime for remaining tests ──
        init_test_runtime();
        let mgr = get_rt_manager().expect("runtime should be initialized");

        // ── Test 2: "fragment_executor" routes to CPU executor gate (Req 2.2) ──
        {
            let gate = mgr.cpu_executor().concurrency_gate().clone();
            let initial_max = gate.max_permits();
            let new_max = initial_max + 4;

            let result = unsafe { call_update_gate("fragment_executor", new_max) };
            assert_eq!(
                result, 0,
                "FFI call should return success for 'fragment_executor'"
            );

            // The resize is spawned on the IO runtime asynchronously.
            // Wait briefly for it to complete.
            std::thread::sleep(std::time::Duration::from_millis(200));

            assert_eq!(
                gate.max_permits(),
                new_max,
                "fragment_executor gate max_permits should be updated to {}",
                new_max
            );
        }

        // ── Test 3: unknown gate name returns success without modifying gates (Req 2.4) ──
        {
            let fragment_executor_gate = mgr.cpu_executor().concurrency_gate().clone();

            let fragment_executor_max_before = fragment_executor_gate.max_permits();

            let result = unsafe { call_update_gate("unknown_gate", 99) };
            assert_eq!(
                result, 0,
                "FFI call should return success even for unknown gate"
            );

            // Wait briefly to ensure no async resize was spawned
            std::thread::sleep(std::time::Duration::from_millis(100));

            // Gate should not have been modified
            assert_eq!(
                fragment_executor_gate.max_permits(),
                fragment_executor_max_before,
                "fragment_executor gate should not be modified for unknown gate name"
            );
        }

        // ── Cleanup ──
        shutdown_test_runtime();
    }
}
