/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Bridge-agnostic API layer.
//!
//! All functions in this module use plain Rust types — no JNI, no FFI-specific
//! types. Both the current JNI bridge (`lib.rs`) and a future FFM/C bridge can
//! call these functions directly.
//!
//! # Pointer contract
//!
//! Functions that accept `i64` pointer arguments require non-zero, valid pointers
//! to the corresponding Rust type. The caller (bridge layer) is responsible for
//! null-checking before calling. Functions that return `i64` return heap-allocated
//! pointers via `Box::into_raw`; the caller owns the pointer and must call the
//! corresponding close function exactly once.
//!
//! # Thread safety
//!
//! - `init_runtime_manager` and `shutdown_runtime_manager` must be called from a
//!   single thread (node startup/shutdown).
//! - `create_global_runtime` / `close_global_runtime` are not thread-safe for the
//!   same pointer.
//! - `execute_query`: async. Safe to call concurrently with different shard/runtime pointers.
//!   The bridge layer wraps with `block_on` or `spawn`.
//! - `stream_next`: async. The bridge layer wraps with `block_on` or `spawn`.
//! - `stream_get_schema`, `stream_close` must NOT be called
//!   concurrently on the same stream pointer.
//!
//! # FFM bridge example
//!
//! When migrating from JNI to JDK FFM (Foreign Function & Memory API), create an
//! `ffi_bridge.rs` that exports `extern "C"` functions calling this API. The JNI
//! bridge (`lib.rs`) and FFM bridge are interchangeable — only the type conversion
//! layer differs.
//!
//! ```rust,ignore
//! // ffi_bridge.rs — extern "C" bridge for JDK FFM (replaces lib.rs JNI bridge)
//! //
//! // Java side uses java.lang.foreign.Linker to call these functions directly.
//! // Strings are passed as (pointer, length) pairs. Byte arrays likewise.
//! // No JNIEnv, no JString, no GlobalRef — pure C ABI.
//!
//! use crate::api;
//! use crate::runtime_manager::RuntimeManager;
//! use std::sync::{Arc, OnceLock};
//!
//! static RUNTIME_MANAGER: OnceLock<Arc<RuntimeManager>> = OnceLock::new();
//!
//! /// Initialize the Tokio runtime manager.
//! /// Java: MethodHandle = linker.downcallHandle(lib.find("df_init"), FunctionDescriptor.ofVoid(JAVA_INT));
//! #[no_mangle]
//! pub extern "C" fn df_init(cpu_threads: i32) {
//!     RUNTIME_MANAGER.get_or_init(|| Arc::new(RuntimeManager::new(cpu_threads as usize)));
//! }
//!
//! /// Create a global DataFusion runtime. Returns pointer as i64, or 0 on error.
//! /// Java: MethodHandle = linker.downcallHandle(lib.find("df_create_runtime"),
//! ///     FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));
//! #[no_mangle]
//! pub extern "C" fn df_create_runtime(
//!     memory_limit: i64,
//!     spill_dir_ptr: *const u8,
//!     spill_dir_len: i64,
//!     spill_limit: i64,
//! ) -> i64 {
//!     let spill_dir = unsafe {
//!         std::str::from_utf8_unchecked(
//!             std::slice::from_raw_parts(spill_dir_ptr, spill_dir_len as usize)
//!         )
//!     };
//!     api::create_global_runtime(memory_limit, spill_dir, spill_limit).unwrap_or(0)
//! }
//!
//! /// Execute a query. Returns stream pointer as i64, or 0 on error.
//! /// Error message written to (err_buf_ptr, err_buf_len), actual length returned via err_len_out.
//! /// Java: MethodHandle = linker.downcallHandle(lib.find("df_execute_query"),
//! ///     FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG));
//! #[no_mangle]
//! pub extern "C" fn df_execute_query(
//!     shard_view_ptr: i64,
//!     table_name_ptr: *const u8,
//!     table_name_len: i64,
//!     plan_ptr: *const u8,
//!     plan_len: i64,
//!     runtime_ptr: i64,
//! ) -> i64 {
//!     let manager = RUNTIME_MANAGER.get().expect("not initialized");
//!     let table_name = unsafe {
//!         std::str::from_utf8_unchecked(
//!             std::slice::from_raw_parts(table_name_ptr, table_name_len as usize)
//!         )
//!     };
//!     let plan_bytes = unsafe {
//!         std::slice::from_raw_parts(plan_ptr, plan_len as usize)
//!     };
//!     manager.io_runtime.block_on(unsafe {
//!         api::execute_query(shard_view_ptr, table_name, plan_bytes, runtime_ptr, manager)
//!     }).unwrap_or(0)
//! }
//!
//! /// Get next batch. Returns FFI_ArrowArray pointer, 0 for end-of-stream, -1 on error.
//! #[no_mangle]
//! pub extern "C" fn df_stream_next(stream_ptr: i64) -> i64 {
//!     let manager = RUNTIME_MANAGER.get().expect("not initialized");
//!     manager.io_runtime.block_on(unsafe { api::stream_next(stream_ptr) }).unwrap_or(-1)
//! }
//!
//! /// Close a stream. Safe with 0.
//! #[no_mangle]
//! pub extern "C" fn df_stream_close(stream_ptr: i64) {
//!     unsafe { api::stream_close(stream_ptr) };
//! }
//!
//! // Java side (JDK 22+):
//! //
//! //   try (Arena arena = Arena.ofConfined()) {
//! //       SymbolLookup lib = SymbolLookup.libraryLookup("libopensearch_datafusion.so", arena);
//! //       Linker linker = Linker.nativeLinker();
//! //
//! //       var init = linker.downcallHandle(
//! //           lib.find("df_init").get(),
//! //           FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT)
//! //       );
//! //       init.invoke(Runtime.getRuntime().availableProcessors());
//! //
//! //       var createRuntime = linker.downcallHandle(
//! //           lib.find("df_create_runtime").get(),
//! //           FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG)
//! //       );
//! //       MemorySegment spillDir = arena.allocateFrom("/tmp/spill");
//! //       long runtimePtr = (long) createRuntime.invoke(512_000_000L, spillDir, spillDir.byteSize(), 256_000_000L);
//! //
//! //       var executeQuery = linker.downcallHandle(
//! //           lib.find("df_execute_query").get(),
//! //           FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, ADDRESS, JAVA_LONG, ADDRESS, JAVA_LONG, JAVA_LONG)
//! //       );
//! //       MemorySegment tableName = arena.allocateFrom("my_table");
//! //       MemorySegment plan = arena.allocateFrom(MemoryLayout.sequenceLayout(planBytes.length, JAVA_BYTE), planBytes);
//! //       long streamPtr = (long) executeQuery.invoke(shardViewPtr, tableName, tableName.byteSize(), plan, plan.byteSize(), runtimePtr);
//! //   }
//! ```

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionConfig;
use futures::TryStreamExt;

use crate::cross_rt_stream::CrossRtStream;
use crate::runtime_manager::RuntimeManager;
use crate::util::create_object_metas;

/// Opaque runtime handle returned to the caller.
/// Contains the DataFusion RuntimeEnv and a pre-built SessionState template.
pub struct DataFusionRuntime {
    pub runtime_env: datafusion::execution::runtime_env::RuntimeEnv,
    /// Pre-built session state with all default features registered.
    /// Cloned per query to avoid re-registering functions and optimizer rules.
    pub session_state_template: SessionState,
}

/// Opaque shard view handle returned to the caller.
pub(crate) struct ShardView {
    pub table_path: ListingTableUrl,
    pub object_metas: Arc<Vec<object_store::ObjectMeta>>,
}

// ---------------------------------------------------------------------------
// Runtime management
// ---------------------------------------------------------------------------

/// Creates a DataFusion global runtime with the given resource limits.
///
/// Returns a heap-allocated pointer (as i64) to `DataFusionRuntime`.
/// Caller must call `close_global_runtime` exactly once to free it.
pub fn create_global_runtime(
    memory_pool_limit: i64,
    spill_dir: &str,
    spill_limit: i64,
) -> Result<i64, DataFusionError> {
    let disk_manager = DiskManagerBuilder::default()
        .with_max_temp_directory_size(spill_limit as u64)
        .with_mode(DiskManagerMode::Directories(vec![PathBuf::from(spill_dir)]));

    let memory_pool = Arc::new(TrackConsumersPool::new(
        GreedyMemoryPool::new(memory_pool_limit as usize),
        NonZeroUsize::new(5).unwrap(),
    ));

    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .with_disk_manager_builder(disk_manager)
        .build()?;

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = false;
    config.options_mut().execution.target_partitions = 4;
    config.options_mut().execution.batch_size = 8192;

    let session_state_template = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env.clone()))
        .with_default_features()
        .build();

    let runtime = DataFusionRuntime { runtime_env, session_state_template };
    Ok(Box::into_raw(Box::new(runtime)) as i64)
}

/// Closes a DataFusion global runtime. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_global_runtime`.
pub unsafe fn close_global_runtime(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut DataFusionRuntime);
    }
}


// ---------------------------------------------------------------------------
// Reader management
// ---------------------------------------------------------------------------

/// Creates a native reader (ShardView) for the given path and files.
///
/// Returns a heap-allocated pointer (as i64) to `ShardView`.
/// Caller must call `close_reader` exactly once to free it.
pub fn create_reader(
    table_path: &str,
    mut filenames: Vec<String>,
    manager: &RuntimeManager,
) -> Result<i64, DataFusionError> {
    filenames.sort();

    let table_url = ListingTableUrl::parse(table_path)
        .map_err(|e| DataFusionError::Execution(format!("Invalid table path: {}", e)))?;

    // TODO: use global runtime's object store instead of building a throwaway RuntimeEnv
    let default_rt = RuntimeEnvBuilder::new().build()?;
    let store = default_rt.object_store(&table_url)?;

    let object_metas = manager.io_runtime.block_on(
        create_object_metas(store.as_ref(), table_path, filenames),
    )?;

    let shard_view = ShardView {
        table_path: table_url,
        object_metas: Arc::new(object_metas),
    };
    Ok(Box::into_raw(Box::new(shard_view)) as i64)
}

/// Closes a native reader. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_reader`.
pub unsafe fn close_reader(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut ShardView);
    }
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

/// Executes a query. Returns a heap-allocated pointer (as i64) to the result stream.
/// Caller must call `stream_close` exactly once to free it.
///
/// This is an async function — the bridge layer decides how to run it
/// (`block_on` for synchronous JNI, `spawn` for async delivery).
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
pub async unsafe fn execute_query(
    shard_view_ptr: i64,
    table_name: &str,
    plan_bytes: &[u8],
    runtime_ptr: i64,
    manager: &RuntimeManager,
) -> Result<i64, DataFusionError> {
    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);

    let table_path = shard_view.table_path.clone();
    let object_metas = shard_view.object_metas.clone();
    let cpu_executor = manager.cpu_executor();

    let result = crate::query_executor::execute_query(
        table_path,
        object_metas,
        table_name.to_string(),
        plan_bytes.to_vec(),
        runtime,
        cpu_executor,
    )
    .await?;

    Ok(result)
}

// ---------------------------------------------------------------------------
// Stream operations
// ---------------------------------------------------------------------------

/// Returns the Arrow schema for the given stream as a heap-allocated FFI_ArrowSchema pointer.
///
/// # Safety
/// `stream_ptr` must be a valid, non-zero pointer to a RecordBatchStreamAdapter.
pub unsafe fn stream_get_schema(stream_ptr: i64) -> Result<i64, DataFusionError> {
    let stream = &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>);
    let schema = stream.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())
        .map_err(|e| DataFusionError::Execution(format!("Schema conversion failed: {}", e)))?;
    Ok(Box::into_raw(Box::new(ffi_schema)) as i64)
}

/// Loads the next record batch from the stream.
///
/// Returns a heap-allocated FFI_ArrowArray pointer (as i64), or 0 if end-of-stream.
///
/// This is an async function — the bridge layer decides how to run it.
///
/// # Safety
/// `stream_ptr` must be a valid, non-zero pointer. Must not be called concurrently
/// on the same stream.
pub async unsafe fn stream_next(
    stream_ptr: i64,
) -> Result<i64, DataFusionError> {
    let stream = &mut *(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>);

    let result = stream.try_next().await?;

    match result {
        Some(batch) => {
            let struct_array: StructArray = batch.into();
            let array_data = struct_array.into_data();
            let ffi_array = FFI_ArrowArray::new(&array_data);
            Ok(Box::into_raw(Box::new(ffi_array)) as i64)
        }
        None => Ok(0),
    }
}

/// Closes a result stream. Safe to call with 0 (no-op).
///
/// # Safety
/// `stream_ptr` must be 0 or a valid pointer returned by `execute_query`.
pub unsafe fn stream_close(stream_ptr: i64) {
    if stream_ptr != 0 {
        let _ = Box::from_raw(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>);
    }
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Converts SQL to Substrait plan bytes (test only).
///
/// # Safety
/// `shard_view_ptr` and `runtime_ptr` must be valid, non-zero pointers.
pub unsafe fn sql_to_substrait(
    shard_view_ptr: i64,
    table_name: &str,
    sql: &str,
    runtime_ptr: i64,
    manager: &RuntimeManager,
) -> Result<Vec<u8>, DataFusionError> {
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
    use datafusion::execution::cache::cache_manager::CacheManagerConfig;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let table_path = shard_view.table_path.clone();
    let object_metas = shard_view.object_metas.clone();
    let table_name = table_name.to_string();

    manager.io_runtime.block_on(async {
        let list_file_cache = Arc::new(DefaultListFilesCache::default());
        list_file_cache.put(
            &datafusion::execution::cache::TableScopedPath {
                table: None,
                path: table_path.prefix().clone(),
            },
            object_metas,
        );
        let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
            .with_cache_manager(
                CacheManagerConfig::default()
                    .with_list_files_cache(Some(list_file_cache))
                    .with_file_metadata_cache(Some(
                        runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                    ))
                    .with_files_statistics_cache(
                        runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                    ),
            )
            .build()?;

        let state = runtime.session_state_template.clone()
            .with_runtime_env(Arc::from(runtime_env));
        let ctx = datafusion::prelude::SessionContext::new_with_state(state);

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let schema = listing_options.infer_schema(&ctx.state(), &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);
        ctx.register_table(&table_name, Arc::new(ListingTable::try_new(config)?))?;

        let plan = ctx.sql(sql).await?.logical_plan().clone();
        let substrait = to_substrait_plan(&plan, &ctx.state())?;
        let mut buf = Vec::new();
        substrait.encode(&mut buf)
            .map_err(|e| DataFusionError::Execution(format!("Substrait encode failed: {}", e)))?;
        Ok(buf)
    })
}
