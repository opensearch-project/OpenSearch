/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Integration tests for the coordinator-reduce FFM exports.
//!
//! These tests drive the `df_*` C ABI entry points directly (same symbols
//! the Java side calls through `NativeBridge`) and validate:
//!
//! - `df_create_local_session` / `df_close_local_session` lifecycle
//! - `df_register_partition_stream` exposes the input as a DataFusion table
//! - `df_sender_send` → execute → `df_stream_next` drains a `SUM` aggregate
//! - Error path: `df_sender_send` on a sender whose receiver is gone returns
//!   a negative rc and the heap-allocated error string decodes cleanly
//! - `df_close_local_session` drops registered senders (receiver side of the
//!   mpsc closes), so a subsequent `df_sender_send` fails
//!
//! The runtime manager (`df_init_runtime_manager`) is a process-global
//! singleton, so we initialize it exactly once across all tests via a
//! `OnceLock` guard.

use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use arrow::ipc::writer::StreamWriter;
use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, Int64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;
use tempfile::TempDir;

use opensearch_datafusion::ffm::{
    df_close_global_runtime, df_close_local_session, df_create_global_runtime,
    df_create_local_session, df_execute_local_plan, df_init_runtime_manager,
    df_register_partition_stream, df_sender_close, df_sender_send, df_stream_close,
    df_stream_next,
};

// ---------------------------------------------------------------------------
// One-time setup
// ---------------------------------------------------------------------------

static RT_INIT: OnceLock<()> = OnceLock::new();

fn ensure_runtime_manager() {
    RT_INIT.get_or_init(|| {
        df_init_runtime_manager(1);
    });
}

/// Holds a DataFusionRuntime pointer and its backing temp dir so the spill
/// directory is removed when the test finishes.
struct RuntimeGuard {
    ptr: i64,
    _spill_dir: TempDir,
}

impl RuntimeGuard {
    fn new() -> Self {
        ensure_runtime_manager();
        let spill_dir = TempDir::new().expect("tempdir");
        let spill_path = spill_dir
            .path()
            .to_str()
            .expect("utf-8 spill path")
            .to_string();
        let spill_bytes = spill_path.as_bytes();
        // 128 MiB pool, 64 MiB spill cap — arbitrary; these tests push a
        // handful of small batches.
        let rc = unsafe {
            df_create_global_runtime(
                128 * 1024 * 1024,
                spill_bytes.as_ptr(),
                spill_bytes.len() as i64,
                64 * 1024 * 1024,
            )
        };
        assert!(rc > 0, "df_create_global_runtime returned {}", rc);
        Self {
            ptr: rc,
            _spill_dir: spill_dir,
        }
    }
}

impl Drop for RuntimeGuard {
    fn drop(&mut self) {
        unsafe { df_close_global_runtime(self.ptr) };
    }
}

// ---------------------------------------------------------------------------
// Helpers: IPC schema bytes, Arrow C Data export, error decoding
// ---------------------------------------------------------------------------

fn i64_schema(column: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(column, DataType::Int64, false)]))
}

fn i64_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .expect("batch builds")
}

/// Serialize a `Schema` to Arrow IPC stream bytes — the shape
/// `df_register_partition_stream` expects.
fn schema_to_ipc_bytes(schema: &Schema) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut buf, schema).expect("stream writer builds");
        writer.finish().expect("stream writer finishes");
    }
    buf
}

/// Export a `RecordBatch` as (FFI_ArrowArray*, FFI_ArrowSchema*) and transfer
/// ownership to the caller — mirrors what `DatafusionReduceSink.feed` will do
/// on the Java side.
///
/// Returns `(array_ptr, schema_ptr)` as `i64` addresses. On successful
/// `df_sender_send` the Rust side consumes both pointers via `from_raw`.
fn export_batch_ptrs(batch: RecordBatch) -> (i64, i64) {
    let schema = batch.schema();
    let ffi_schema =
        FFI_ArrowSchema::try_from(schema.as_ref()).expect("schema export");

    let struct_array: StructArray = batch.into();
    let array_data = struct_array.into_data();
    let ffi_array = FFI_ArrowArray::new(&array_data);

    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (array_ptr, schema_ptr)
}

/// Decode (and free) the heap-allocated error string referenced by a
/// negative FFM return code. Convention is defined in
/// `sandbox/libs/dataformat-native/rust/common/src/error.rs`: the error
/// pointer is the positive value of the negated return code, and the
/// string is a `CString` that must be freed via `CString::from_raw`.
fn decode_error(rc: i64) -> String {
    assert!(rc < 0, "expected negative rc, got {}", rc);
    let ptr = (-rc) as *mut c_char;
    // SAFETY: the FFM layer produces this string via `CString::into_raw`;
    // taking ownership back via `from_raw` both reads and frees it.
    let cstring = unsafe { CString::from_raw(ptr) };
    cstring.to_string_lossy().into_owned()
}

/// Build a Substrait plan for `SELECT SUM(x) AS total FROM "input-0"` using a
/// throwaway session that only knows the schema — the plan is portable onto
/// any session where `"input-0"` has the same schema.
async fn build_sum_substrait(schema: SchemaRef) -> Vec<u8> {
    let ctx = SessionContext::new();
    let empty = MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("mem table");
    ctx.register_table("input-0", Arc::new(empty))
        .expect("register input-0");
    let plan = ctx
        .sql("SELECT SUM(x) AS total FROM \"input-0\"")
        .await
        .expect("sql parses")
        .logical_plan()
        .clone();
    let substrait = to_substrait_plan(&plan, &ctx.state()).expect("to_substrait");
    let mut buf = Vec::new();
    substrait.encode(&mut buf).expect("encode");
    buf
}

fn register_input(session_ptr: i64, input_id: &str, schema: &Schema) -> i64 {
    let ipc = schema_to_ipc_bytes(schema);
    let id_bytes = input_id.as_bytes();
    let rc = unsafe {
        df_register_partition_stream(
            session_ptr,
            id_bytes.as_ptr(),
            id_bytes.len() as i64,
            ipc.as_ptr(),
            ipc.len() as i64,
        )
    };
    assert!(rc > 0, "df_register_partition_stream rc={}", rc);
    rc
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_create_session_returns_nonzero_ptr() {
    let runtime = RuntimeGuard::new();
    let session_ptr = unsafe { df_create_local_session(runtime.ptr) };
    assert!(
        session_ptr > 0,
        "df_create_local_session returned {}",
        session_ptr
    );
    unsafe { df_close_local_session(session_ptr) };
}

#[test]
fn test_register_partition_exposes_streaming_table() {
    let runtime = RuntimeGuard::new();
    let session_ptr = unsafe { df_create_local_session(runtime.ptr) };
    assert!(session_ptr > 0);

    let schema_a = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
    let schema_b = Schema::new(vec![Field::new("y", DataType::Int64, true)]);

    let sender_a = register_input(session_ptr, "input-0", &schema_a);
    let sender_b = register_input(session_ptr, "input-1", &schema_b);
    assert_ne!(sender_a, sender_b);

    // Drop the senders first so the mpsc receivers in the registered
    // StreamingTables see EOF when the session is later dropped.
    unsafe { df_sender_close(sender_a) };
    unsafe { df_sender_close(sender_b) };
    unsafe { df_close_local_session(session_ptr) };
}

#[test]
fn test_execute_sum_substrait() {
    let runtime = RuntimeGuard::new();
    let session_ptr = unsafe { df_create_local_session(runtime.ptr) };
    assert!(session_ptr > 0);

    let schema = i64_schema("x");
    let sender_ptr = register_input(session_ptr, "input-0", schema.as_ref());

    // Build the Substrait plan off-thread — we need it before calling
    // `df_execute_local_plan` which runs `block_on` on the caller.
    let substrait_bytes = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio rt");
        rt.block_on(build_sum_substrait(Arc::clone(&schema)))
    };

    // Producer must start BEFORE `df_execute_local_plan`: the execute path
    // blocks on `block_on(execute_substrait)` which may poll the streaming
    // input during logical → physical planning. The channel capacity (4) is
    // wide enough to buffer all three batches, so the producer finishes
    // without waiting on a consumer.
    let producer_schema = Arc::clone(&schema);
    let producer = thread::spawn(move || {
        for chunk in [
            vec![1i64, 2, 3],
            vec![4i64, 5, 6],
            vec![7i64, 8, 9],
        ] {
            let batch = i64_batch(&producer_schema, &chunk);
            let (arr_ptr, sch_ptr) = export_batch_ptrs(batch);
            let rc = unsafe { df_sender_send(sender_ptr, arr_ptr, sch_ptr) };
            assert_eq!(rc, 0, "df_sender_send rc={}", rc);
        }
        // EOF — releases the sender, which closes the mpsc.
        unsafe { df_sender_close(sender_ptr) };
    });

    // Give the producer a moment to start pushing so the first batches are
    // in the channel before the physical plan pulls from it. Not required
    // for correctness (mpsc is bounded and the producer drives its own
    // block_on), but reduces interleaving surprises.
    thread::sleep(Duration::from_millis(10));

    let stream_ptr = unsafe {
        df_execute_local_plan(
            session_ptr,
            substrait_bytes.as_ptr(),
            substrait_bytes.len() as i64,
        )
    };
    assert!(stream_ptr > 0, "df_execute_local_plan rc={}", stream_ptr);

    // Drain the output stream. Each `df_stream_next` returns either a
    // pointer to a heap-allocated `FFI_ArrowArray` (caller owns it and
    // must drop it) or 0 for EOS.
    let mut total: i64 = 0;
    loop {
        let rc = unsafe { df_stream_next(stream_ptr) };
        assert!(rc >= 0, "df_stream_next rc={}", rc);
        if rc == 0 {
            break;
        }
        // SAFETY: `rc` is a `Box::into_raw(Box::new(FFI_ArrowArray))` ptr.
        let ffi_array = unsafe { Box::from_raw(rc as *mut FFI_ArrowArray) };
        let result_schema = Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Int64,
            true,
        )]));
        let ffi_schema = FFI_ArrowSchema::try_from(result_schema.as_ref())
            .expect("result schema export");
        let array_data = unsafe {
            arrow_array::ffi::from_ffi(*ffi_array, &ffi_schema).expect("import")
        };
        let struct_array = StructArray::from(array_data);
        let batch = RecordBatch::from(struct_array);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 column");
        for i in 0..col.len() {
            if !col.is_null(i) {
                total += col.value(i);
            }
        }
    }
    unsafe { df_stream_close(stream_ptr) };
    unsafe { df_close_local_session(session_ptr) };
    producer.join().expect("producer thread");

    assert_eq!(total, 45, "SUM(1..=9) should be 45, got {}", total);
}

#[test]
fn test_sender_send_error_path() {
    let runtime = RuntimeGuard::new();
    let session_ptr = unsafe { df_create_local_session(runtime.ptr) };
    assert!(session_ptr > 0);

    let schema = i64_schema("x");
    let sender_ptr = register_input(session_ptr, "input-0", schema.as_ref());

    // Drop the session: its registered StreamingTable drops the
    // SingleReceiverPartition, which drops the mpsc receiver. The sender's
    // channel is now closed.
    unsafe { df_close_local_session(session_ptr) };

    // Attempting to send now fails — `send_blocking` reports "receiver
    // dropped before send".
    let batch = i64_batch(&schema, &[1, 2, 3]);
    let (arr_ptr, sch_ptr) = export_batch_ptrs(batch);
    let rc = unsafe { df_sender_send(sender_ptr, arr_ptr, sch_ptr) };
    assert!(rc < 0, "expected error, got rc={}", rc);

    let msg = decode_error(rc);
    assert!(
        msg.contains("receiver dropped") || msg.contains("receiver"),
        "unexpected error message: {}",
        msg
    );

    unsafe { df_sender_close(sender_ptr) };
}

#[test]
fn test_close_session_drops_registered_senders() {
    let runtime = RuntimeGuard::new();
    let session_ptr = unsafe { df_create_local_session(runtime.ptr) };
    assert!(session_ptr > 0);

    let schema = i64_schema("x");
    let sender_ptr = register_input(session_ptr, "input-0", schema.as_ref());

    // Sanity check: before session close, sending succeeds (the channel has
    // capacity 4 so one send does not block).
    let batch_ok = i64_batch(&schema, &[10]);
    let (a0, s0) = export_batch_ptrs(batch_ok);
    let rc_ok = unsafe { df_sender_send(sender_ptr, a0, s0) };
    assert_eq!(rc_ok, 0, "pre-close send should succeed, rc={}", rc_ok);

    // Close the session. The surviving sender's mpsc is now orphaned.
    unsafe { df_close_local_session(session_ptr) };

    // Subsequent `df_sender_send` on the still-live sender pointer fails.
    let batch_fail = i64_batch(&schema, &[20]);
    let (a1, s1) = export_batch_ptrs(batch_fail);
    let rc_err = unsafe { df_sender_send(sender_ptr, a1, s1) };
    assert!(
        rc_err < 0,
        "post-close send should fail, got rc={}",
        rc_err
    );
    let msg = decode_error(rc_err);
    assert!(
        msg.contains("receiver"),
        "expected receiver-dropped error, got: {}",
        msg
    );

    unsafe { df_sender_close(sender_ptr) };
}
