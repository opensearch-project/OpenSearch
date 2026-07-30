/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Regression test for `compact_string_view_columns` (api.rs line 459).
//!
//! Exercises `df_stream_next` end-to-end with a sliced StringView batch.
//! When compaction is active, the output batch's backing buffers should be
//! right-sized (~3KB for 100 strings) rather than carrying the full 10K-element
//! buffer (~300KB).

use std::sync::{Arc, OnceLock};
use std::thread;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, StringViewArray, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;
use tempfile::TempDir;

use opensearch_datafusion::ffm::{
    df_close_global_runtime, df_close_local_session, df_create_global_runtime,
    df_create_local_session, df_execute_local_plan, df_init_runtime_manager,
    df_register_partition_stream, df_sender_close, df_sender_send, df_stream_close, df_stream_next,
};

// ---------------------------------------------------------------------------
// One-time setup (same OnceLock pattern as local_exec_test.rs)
// ---------------------------------------------------------------------------

static RT_INIT: OnceLock<()> = OnceLock::new();

fn ensure_runtime_manager() {
    RT_INIT.get_or_init(|| {
        df_init_runtime_manager(1, 1.5, 1.5);
    });
}

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
        let rc = unsafe {
            df_create_global_runtime(
                128 * 1024 * 1024,
                0,
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
// Helpers
// ---------------------------------------------------------------------------

fn utf8view_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Utf8View,
        false,
    )]))
}

/// Build a substrait plan for `SELECT * FROM "input-0"` with the given schema.
async fn build_passthrough_substrait(schema: SchemaRef) -> Vec<u8> {
    let ctx = SessionContext::new();
    let empty = MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("mem table");
    ctx.register_table("input-0", Arc::new(empty))
        .expect("register input-0");
    let plan = ctx
        .sql("SELECT * FROM \"input-0\"")
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
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio rt");
    let plan_bytes = rt.block_on(build_passthrough_substrait(Arc::new(schema.clone())));
    let id_bytes = input_id.as_bytes();
    let mut out_buf = vec![0u8; 64 * 1024];
    let mut out_len: i64 = 0;
    let rc = unsafe {
        df_register_partition_stream(
            session_ptr,
            id_bytes.as_ptr(),
            id_bytes.len() as i64,
            plan_bytes.as_ptr(),
            plan_bytes.len() as i64,
            out_buf.as_mut_ptr(),
            out_buf.len() as i64,
            &mut out_len as *mut i64,
        )
    };
    assert!(rc > 0, "df_register_partition_stream rc={}", rc);
    rc
}

fn export_batch_ptrs(batch: RecordBatch) -> (i64, i64) {
    let schema = batch.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).expect("schema export");

    let struct_array: StructArray = batch.into();
    let array_data = struct_array.into_data();
    let ffi_array = FFI_ArrowArray::new(&array_data);

    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (array_ptr, schema_ptr)
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[test]
fn test_stringview_gc_on_sliced_batch() {
    let runtime = RuntimeGuard::new();
    let session_ptr = unsafe { df_create_local_session(runtime.ptr) };
    assert!(session_ptr > 0);

    let schema = utf8view_schema();
    let sender_ptr = register_input(session_ptr, "input-0", schema.as_ref());

    // Build a substrait plan for passthrough SELECT
    let substrait_bytes = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio rt");
        rt.block_on(build_passthrough_substrait(Arc::clone(&schema)))
    };

    // Create a 10K-element StringViewArray, then slice to 100 rows.
    // Each string is ~30 bytes so the full array backing is ~300KB.
    // The sliced batch only references 100 strings (~3KB of actual data).
    let producer = thread::spawn(move || {
        let full_strings: Vec<String> = (0..10_000)
            .map(|i| format!("string-value-padding-{:010}", i))
            .collect();
        let full_array = StringViewArray::from_iter_values(full_strings.iter().map(|s| s.as_str()));

        // Verify the full array has substantial backing buffers
        let full_buffer_size: usize = full_array.data_buffers().iter().map(|b| b.len()).sum();
        assert!(
            full_buffer_size > 200_000,
            "full array buffer size should be >200KB, got {}",
            full_buffer_size
        );

        // Slice to rows [100..200] — only 100 elements
        let sliced = full_array.slice(100, 100);
        let sliced_view = sliced
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("still a StringViewArray");

        // The sliced array still references the full backing buffers
        let sliced_buffer_size: usize = sliced_view.data_buffers().iter().map(|b| b.len()).sum();
        assert!(
            sliced_buffer_size > 200_000,
            "sliced array should still carry full buffers before GC, got {}",
            sliced_buffer_size
        );

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "s",
                DataType::Utf8View,
                false,
            )])),
            vec![Arc::new(sliced)],
        )
        .expect("batch from sliced array");

        let (arr_ptr, sch_ptr) = export_batch_ptrs(batch);
        let rc = unsafe { df_sender_send(sender_ptr, arr_ptr, sch_ptr) };
        assert_eq!(rc, 0, "df_sender_send rc={}", rc);

        unsafe { df_sender_close(sender_ptr) };
    });

    let stream_ptr = unsafe {
        df_execute_local_plan(
            session_ptr,
            substrait_bytes.as_ptr(),
            substrait_bytes.len() as i64,
            0, // context_id
        )
    };
    assert!(stream_ptr > 0, "df_execute_local_plan rc={}", stream_ptr);

    // Drain the output and check buffer sizes
    let mut output_batches: Vec<RecordBatch> = Vec::new();
    loop {
        let rc = unsafe { df_stream_next(stream_ptr) };
        assert!(rc >= 0, "df_stream_next rc={}", rc);
        if rc == 0 {
            break;
        }
        let ffi_array = unsafe { Box::from_raw(rc as *mut FFI_ArrowArray) };
        let result_schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let ffi_schema =
            FFI_ArrowSchema::try_from(result_schema.as_ref()).expect("result schema export");
        let array_data =
            unsafe { arrow_array::ffi::from_ffi(*ffi_array, &ffi_schema).expect("import") };
        let struct_array = StructArray::from(array_data);
        let batch = RecordBatch::from(struct_array);
        output_batches.push(batch);
    }

    unsafe { df_stream_close(stream_ptr) };
    unsafe { df_close_local_session(session_ptr) };
    producer.join().expect("producer thread");

    // Validate: we got exactly 100 rows and the buffers are compact
    let total_rows: usize = output_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 100, "expected 100 rows, got {}", total_rows);

    let mut total_buffer_bytes: usize = 0;
    for batch in &output_batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("Utf8View column");
        let buf_size: usize = col.data_buffers().iter().map(|b| b.len()).sum();
        total_buffer_bytes += buf_size;
    }

    // 100 strings of ~30 bytes each = ~3KB of actual data.
    // With compaction (line 459 active): buffers should be <= 10KB (generous margin).
    // Without compaction: buffers would be >200KB (full 10K-element backing).
    assert!(
        total_buffer_bytes < 10_000,
        "StringView buffers should be compacted to ~3KB, but got {} bytes. \
         This indicates compact_string_view_columns is not working.",
        total_buffer_bytes
    );
}
