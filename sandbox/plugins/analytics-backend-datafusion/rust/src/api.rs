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

use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::ipc::reader::StreamReader;
use arrow_array::{Array, StructArray};
use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::RecordBatch;
use arrow_schema::ffi::FFI_ArrowSchema;
use datafusion::common::DataFusionError;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{GreedyMemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::execution::RecordBatchStream;
use datafusion::prelude::SessionConfig;
use futures::TryStreamExt;

use crate::cross_rt_stream::CrossRtStream;
use crate::local_executor::LocalSession;
use crate::partition_stream::PartitionStreamSender;
use crate::query_memory_pool_tracker::QueryTrackingContext;
use crate::runtime_manager::RuntimeManager;

/// Bundles a stream with its query tracking context so that dropping the
/// handle automatically marks the query completed in the registry.
pub struct QueryStreamHandle {
    stream: RecordBatchStreamAdapter<CrossRtStream>,
    /// Held for its `Drop` impl — marks the query completed when the
    /// stream is closed.
    _query_tracking_context: QueryTrackingContext,
}

impl QueryStreamHandle {
    pub fn new(stream: RecordBatchStreamAdapter<CrossRtStream>, query_context: QueryTrackingContext) -> Self {
        Self { stream, _query_tracking_context: query_context }
    }
}

/// Build ObjectMeta for each file using the given object store.
pub async fn create_object_metas(
    store: &dyn object_store::ObjectStore,
    base_path: &str,
    filenames: Vec<String>,
) -> Result<Vec<object_store::ObjectMeta>, DataFusionError> {
    let mut metas = Vec::with_capacity(filenames.len());
    for filename in filenames {
        let full_path = if filename.starts_with('/') || filename.contains(base_path) {
            filename
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filename)
        };
        let path = object_store::path::Path::from(full_path.as_str());
        let meta = store.head(&path).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get object meta for {}: {}", full_path, e))
        })?;
        metas.push(meta);
    }
    Ok(metas)
}

/// Opaque runtime handle returned to the caller.
/// Contains the DataFusion RuntimeEnv (memory pool, disk spill, cache).
pub struct DataFusionRuntime {
    pub runtime_env: datafusion::execution::runtime_env::RuntimeEnv,
}

/// Opaque shard view handle returned to the caller.
pub(crate) struct ShardView {
    pub table_path: ListingTableUrl,
    pub object_metas: Arc<Vec<object_store::ObjectMeta>>,
}

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

    let runtime = DataFusionRuntime { runtime_env };
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

/// Creates a native reader (ShardView) for the given path and files.
///
/// Returns a heap-allocated pointer (as i64) to `ShardView`.
/// Caller must call `close_reader` exactly once to free it.
pub fn create_reader(
    table_path: &str,
    mut filenames: Vec<String>,
    tokio_rt_manager: &RuntimeManager,
) -> Result<i64, DataFusionError> {
    filenames.sort();

    let table_url = ListingTableUrl::parse(table_path)
        .map_err(|e| DataFusionError::Execution(format!("Invalid table path: {}", e)))?;

    // TODO: use global runtime's object store instead of building a throwaway RuntimeEnv
    let default_rt = RuntimeEnvBuilder::new().build()?;
    let store = default_rt.object_store(&table_url)?;

    let object_metas = tokio_rt_manager.io_runtime.block_on(
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
    context_id: i64,
) -> Result<i64, DataFusionError> {
    let shard_view = &*(shard_view_ptr as *const ShardView);
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);

    let table_path = shard_view.table_path.clone();
    let object_metas = shard_view.object_metas.clone();
    let cpu_executor = manager.cpu_executor();

    // Create per-query context — auto-registers in the global registry
    let global_pool = runtime.runtime_env.memory_pool.clone();
    let query_context = QueryTrackingContext::new(context_id, global_pool);
    let query_memory_pool = query_context.memory_pool()
        .map(|p| p as Arc<dyn datafusion::execution::memory_pool::MemoryPool>);

    let stream_ptr = crate::query_executor::execute_query(
        table_path,
        object_metas,
        table_name.to_string(),
        plan_bytes.to_vec(),
        runtime,
        cpu_executor,
        query_memory_pool,
    )
    .await?;

    // Reconstruct the stream from the raw pointer returned by query_executor
    let stream = *Box::from_raw(stream_ptr as *mut RecordBatchStreamAdapter<CrossRtStream>);
    let handle = QueryStreamHandle::new(stream, query_context);
    Ok(Box::into_raw(Box::new(handle)) as i64)
}

/// Returns the Arrow schema for the given stream as a heap-allocated FFI_ArrowSchema pointer.
///
/// # Safety
/// `stream_ptr` must be a valid, non-zero pointer to a QueryStreamHandle.
pub unsafe fn stream_get_schema(stream_ptr: i64) -> Result<i64, DataFusionError> {
    let handle = &mut *(stream_ptr as *mut QueryStreamHandle);
    let schema = handle.stream.schema();
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
    let handle = &mut *(stream_ptr as *mut QueryStreamHandle);

    let result = handle.stream.try_next().await?;

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
        // Dropping the handle drops both the stream and the query context.
        // The context's Drop impl marks the query completed in the registry.
        let _ = Box::from_raw(stream_ptr as *mut QueryStreamHandle);
    }
}

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

        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(Arc::from(runtime_env))
            .with_default_features()
            .build();
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

// ---------------------------------------------------------------------------
// Coordinator-reduce local execution API
//
// Mirrors the shard-scan path: a `LocalSession` pointer is created once per
// reduce stage, streaming inputs are registered under synthetic names, a
// Substrait plan is executed against those inputs, and the output stream is
// drained via the existing `stream_next` / `stream_close` exports (because
// `execute_local_plan` hands back a `QueryStreamHandle` of the same shape
// `execute_query` returns).
// ---------------------------------------------------------------------------

/// Creates a `LocalSession` bound to the given runtime's [`RuntimeEnv`]
/// (memory pool, disk manager, and caches are shared).
///
/// Returns a heap-allocated pointer (as i64) to `LocalSession`. Caller must
/// call `close_local_session` exactly once to free it.
///
/// # Safety
/// `runtime_ptr` must be a valid, non-zero pointer returned by
/// `create_global_runtime`.
pub unsafe fn create_local_session(runtime_ptr: i64) -> Result<i64, DataFusionError> {
    let runtime = &*(runtime_ptr as *const DataFusionRuntime);
    let session = LocalSession::new(&runtime.runtime_env);
    Ok(Box::into_raw(Box::new(session)) as i64)
}

/// Closes a `LocalSession`. Safe to call with 0 (no-op).
///
/// # Safety
/// `ptr` must be 0 or a valid pointer returned by `create_local_session`.
pub unsafe fn close_local_session(ptr: i64) {
    if ptr != 0 {
        let _ = Box::from_raw(ptr as *mut LocalSession);
    }
}

/// Registers a streaming input on the session under `input_id`, using the
/// Arrow schema decoded from the IPC stream bytes.
///
/// The IPC bytes are expected to be a single schema message produced by
/// Arrow's streaming IPC writer (e.g. Java's `MessageSerializer.serializeMetadata`
/// or an `ArrowStreamWriter` flush of just the schema). Only the schema is
/// read — any payload in the buffer is ignored.
///
/// Returns a heap-allocated pointer (as i64) to a [`PartitionStreamSender`].
/// Caller must call `sender_close` exactly once to free it (closing the
/// sender signals EOF to the receiver side, so the native execute driver
/// naturally completes).
///
/// # Safety
/// `session_ptr` must be a valid, non-zero pointer returned by
/// `create_local_session`.
pub unsafe fn register_partition_stream(
    session_ptr: i64,
    input_id: &str,
    schema_ipc: &[u8],
) -> Result<i64, DataFusionError> {
    let session = &mut *(session_ptr as *mut LocalSession);
    let mut cursor = Cursor::new(schema_ipc);
    let reader = StreamReader::try_new(&mut cursor, None).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to decode Arrow IPC schema for '{}': {}",
            input_id, e
        ))
    })?;
    let schema = reader.schema();
    let sender = session.register_partition(input_id, schema)?;
    Ok(Box::into_raw(Box::new(sender)) as i64)
}

/// Executes a Substrait plan against a `LocalSession` and returns a
/// `QueryStreamHandle` pointer whose output can be drained via the existing
/// `stream_next` / `stream_close` exports.
///
/// The returned stream wraps the DataFusion output in the same
/// `CrossRtStream` + `RecordBatchStreamAdapter` shape as `execute_query`,
/// so the session produces batches on the CPU executor while `stream_next`
/// consumes them on the I/O runtime.
///
/// This is an async function — the bridge layer decides how to run it
/// (`block_on` for synchronous FFM entry, `spawn` for async delivery).
///
/// # Safety
/// `session_ptr` must be a valid, non-zero pointer returned by
/// `create_local_session`.
pub async unsafe fn execute_local_plan(
    session_ptr: i64,
    substrait_bytes: &[u8],
    manager: &RuntimeManager,
    context_id: i64,
) -> Result<i64, DataFusionError> {
    let session = &*(session_ptr as *const LocalSession);

    // Per-query memory tracking — wraps the session's global pool. A
    // `context_id` of 0 disables tracking (pool is not consulted).
    let query_context = QueryTrackingContext::new(context_id, session.memory_pool());

    let df_stream = session.execute_substrait(substrait_bytes).await?;

    // Wrap the output in the same CrossRtStream + RecordBatchStreamAdapter
    // shape as `execute_query`, so existing `stream_next` / `stream_close`
    // drain this handle unchanged.
    let cross_rt_stream =
        CrossRtStream::new_with_df_error_stream(df_stream, manager.cpu_executor());
    let wrapped = RecordBatchStreamAdapter::new(cross_rt_stream.schema(), cross_rt_stream);

    let handle = QueryStreamHandle::new(wrapped, query_context);
    Ok(Box::into_raw(Box::new(handle)) as i64)
}

/// Imports an Arrow C Data batch and pushes it through the partition
/// stream's mpsc. The Rust side takes ownership of the
/// `FFI_ArrowArray` / `FFI_ArrowSchema` structs on success — the Java side
/// must not release them after a successful send. On error ownership is
/// released back to Rust's drop impls (the imported structs go out of scope
/// without being forgotten).
///
/// The `io_handle` is the Tokio handle used to drive the blocking send;
/// typically the `io_runtime` handle from the global `RuntimeManager`.
///
/// # Safety
/// - `sender_ptr` must be a valid, non-zero pointer returned by
///   `register_partition_stream`.
/// - `array_ptr` must point to a populated `FFI_ArrowArray` struct owned by
///   the caller; ownership transfers to Rust on success.
/// - `schema_ptr` must point to a populated `FFI_ArrowSchema` struct owned
///   by the caller; ownership transfers to Rust on success.
pub unsafe fn sender_send(
    sender_ptr: i64,
    array_ptr: i64,
    schema_ptr: i64,
    io_handle: &tokio::runtime::Handle,
) -> Result<(), DataFusionError> {
    let sender = &*(sender_ptr as *const PartitionStreamSender);

    // Take ownership of the Java-allocated FFI structs. `from_raw` reads
    // the struct contents into Rust-owned values; the original memory is
    // now Rust's responsibility to drop.
    let ffi_array = FFI_ArrowArray::from_raw(array_ptr as *mut FFI_ArrowArray);
    let ffi_schema = FFI_ArrowSchema::from_raw(schema_ptr as *mut FFI_ArrowSchema);

    // `from_ffi` takes the array by value (consumes it) and the schema by
    // reference (it is still dropped when `ffi_schema` goes out of scope).
    let array_data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).map_err(|e| {
        DataFusionError::Execution(format!("Failed to import Arrow C Data array: {}", e))
    })?;

    let struct_array = StructArray::from(array_data);
    let batch = RecordBatch::from(struct_array);

    sender.send_blocking(Ok(batch), io_handle)
}

/// Closes a partition stream sender. Dropping the sender closes the mpsc,
/// which the receiver side (DataFusion's streaming table) interprets as
/// end-of-input.
///
/// Safe to call with 0 (no-op).
///
/// # Safety
/// `sender_ptr` must be 0 or a valid pointer returned by
/// `register_partition_stream`.
pub unsafe fn sender_close(sender_ptr: i64) {
    if sender_ptr != 0 {
        let _ = Box::from_raw(sender_ptr as *mut PartitionStreamSender);
    }
}

/// Imports a batch of Arrow C Data structures into a [`Vec<RecordBatch>`] and
/// registers them as an in-memory table on the given session under `input_id`.
///
/// The Java side has accumulated all shard responses, exported each
/// `VectorSchemaRoot` to a paired `FFI_ArrowArray` / `FFI_ArrowSchema`, and
/// passed the raw pointers as two parallel slices. Rust takes ownership of
/// the FFI structs on success.
///
/// On error ownership is released back to Rust's drop impls (the imported
/// structs go out of scope without being forgotten).
///
/// # Safety
/// - `session_ptr` must be a valid, non-zero pointer returned by
///   `create_local_session`.
/// - `array_ptrs` and `schema_ptrs` must point to populated FFI structs owned
///   by the caller; ownership transfers to Rust on success.
pub unsafe fn register_memtable(
    session_ptr: i64,
    input_id: &str,
    schema_ipc: &[u8],
    array_ptrs: &[i64],
    schema_ptrs: &[i64],
) -> Result<(), DataFusionError> {
    if array_ptrs.len() != schema_ptrs.len() {
        return Err(DataFusionError::Execution(format!(
            "register_memtable: array_ptrs.len()={} != schema_ptrs.len()={}",
            array_ptrs.len(),
            schema_ptrs.len()
        )));
    }
    let session = &mut *(session_ptr as *mut LocalSession);

    let mut cursor = Cursor::new(schema_ipc);
    let reader = StreamReader::try_new(&mut cursor, None).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to decode Arrow IPC schema for '{}': {}",
            input_id, e
        ))
    })?;
    let table_schema = reader.schema();

    // The IPC schema is what the substrait plan was compiled against — same as the streaming
    // sink registers. The exported VSRs may arrive with batch-level schemas that differ in
    // nullability/metadata/field-naming details; the streaming sink tolerates this because
    // DataFusion's streaming source addresses columns by index. `MemTable::try_new` instead
    // checks each batch's schema against the table schema. To stay compatible with both
    // shapes, rebuild each imported batch with `table_schema` — the column data is reused
    // verbatim, but the schema header is the planner's.
    let mut batches = Vec::with_capacity(array_ptrs.len());
    for (&array_ptr, &schema_ptr) in array_ptrs.iter().zip(schema_ptrs.iter()) {
        let ffi_array = FFI_ArrowArray::from_raw(array_ptr as *mut FFI_ArrowArray);
        let ffi_schema = FFI_ArrowSchema::from_raw(schema_ptr as *mut FFI_ArrowSchema);
        let array_data = arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).map_err(|e| {
            DataFusionError::Execution(format!("Failed to import Arrow C Data array: {}", e))
        })?;
        let struct_array = StructArray::from(array_data);
        let raw = RecordBatch::from(struct_array);
        let aligned = RecordBatch::try_new(Arc::clone(&table_schema), raw.columns().to_vec()).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to align imported batch to registered schema for '{}': {}",
                input_id, e
            ))
        })?;
        batches.push(aligned);
    }

    session.register_memtable(input_id, table_schema, batches)
}
