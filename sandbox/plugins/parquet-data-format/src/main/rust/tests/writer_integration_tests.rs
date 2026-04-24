/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use opensearch_parquet_format::object_store_handle;
use opensearch_parquet_format::test_utils::*;
use opensearch_parquet_format::writer;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

#[test]
fn test_complete_writer_lifecycle() {
    let (_temp_dir, store_handle) = create_temp_store();
    let writer_handle = create_writer_on_store(store_handle, "complete_workflow.parquet");

    for _ in 0..3 {
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_handle(writer_handle);
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }

    let finalize = writer::finalize_writer(writer_handle).unwrap();
    assert_eq!(finalize.num_rows, 9); // 3 batches × 3 rows
    assert_ne!(finalize.crc32, 0);

    // Verify file on disk
    let file_path = _temp_dir.path().join("complete_workflow.parquet");
    assert!(file_path.exists());
    assert!(file_path.metadata().unwrap().len() > 0);

    // Verify readable
    let read_metadata = writer::get_file_metadata(file_path.to_string_lossy().to_string()).unwrap();
    assert_eq!(read_metadata.num_rows(), finalize.num_rows);

    unsafe {
        object_store_handle::drop_store(store_handle);
    }
}

#[test]
fn test_concurrent_writers() {
    let (_temp_dir, store_handle) = create_temp_store();
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..5 {
        let success = Arc::clone(&success_count);
        // Each thread needs its own store handle clone
        let thread_store = unsafe { object_store_handle::arc_clone_from_raw(store_handle) };
        let thread_store_handle = object_store_handle::arc_into_raw(thread_store);

        let handle = thread::spawn(move || {
            let path = format!("concurrent_{}.parquet", i);
            let (_schema, schema_ptr) = create_test_ffi_schema();
            let writer_handle = writer::create_writer(thread_store_handle, &path, schema_ptr)
                .expect("create_writer failed");

            let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
            writer::write_data(writer_handle, array_ptr, data_schema_ptr)
                .expect("write_data failed");
            cleanup_ffi_data(array_ptr, data_schema_ptr);

            let result = writer::finalize_writer(writer_handle).expect("finalize failed");
            assert_eq!(result.num_rows, 3);
            success.fetch_add(1, Ordering::SeqCst);

            unsafe {
                object_store_handle::drop_store(thread_store_handle);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(success_count.load(Ordering::SeqCst), 5);
    unsafe {
        object_store_handle::drop_store(store_handle);
    }
}
