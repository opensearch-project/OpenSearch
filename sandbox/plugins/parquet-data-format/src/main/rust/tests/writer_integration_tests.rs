/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use opensearch_parquet_format::test_utils::*;
use opensearch_parquet_format::writer::NativeParquetWriter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

#[test]
fn test_complete_writer_lifecycle() {
    let (_temp_dir, filename) = get_temp_file_path("complete_workflow.parquet");
    let file_path = std::path::Path::new(&filename);
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

    for _i in 0..3 {
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }

    let metadata = close_writer_and_get_metadata(&filename, schema_ptr);
    assert_eq!(metadata.metadata.file_metadata().num_rows(), 9); // 3 batches × 3 rows
    assert!(metadata.metadata.file_metadata().version() > 0);

    assert!(NativeParquetWriter::sync_to_disk(filename.clone()).is_ok());
    assert!(file_path.exists());
    assert!(file_path.metadata().unwrap().len() > 0);

    let read_metadata = NativeParquetWriter::get_file_metadata(filename.clone()).unwrap();
    assert_eq!(read_metadata.num_rows(), metadata.metadata.file_metadata().num_rows());
    assert_eq!(read_metadata.version(), metadata.metadata.file_metadata().version());
}

#[test]
fn test_concurrent_writer_creation() {
    let temp_dir = tempdir().unwrap();
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..10 {
        let temp_dir_path = temp_dir.path().to_path_buf();
        let success_count = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            let file_path = temp_dir_path.join(format!("concurrent_{}.parquet", i));
            let filename = file_path.to_string_lossy().to_string();
            let (_schema, schema_ptr) = create_test_ffi_schema();
            if NativeParquetWriter::create_writer(filename.clone(), "test-index".to_string(), schema_ptr, vec![], vec![], vec![]).is_ok() {
                success_count.fetch_add(1, Ordering::SeqCst);
                let _ = NativeParquetWriter::finalize_writer(filename);
            }
            cleanup_ffi_schema(schema_ptr);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(success_count.load(Ordering::SeqCst), 10);
}

#[test]
fn test_concurrent_close_operations_same_file() {
    let (_temp_dir, filename) = get_temp_file_path("close_race.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..3 {
        let filename = filename.clone();
        let success_count = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            if NativeParquetWriter::finalize_writer(filename).is_ok() {
                success_count.fetch_add(1, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(success_count.load(Ordering::SeqCst), 1);
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_concurrent_writes_same_file() {
    let (_temp_dir, filename) = get_temp_file_path("concurrent_write_ffi.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..5 {
        let filename = filename.clone();
        let success_count = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
            if NativeParquetWriter::write_data(filename, array_ptr, data_schema_ptr).is_ok() {
                success_count.fetch_add(1, Ordering::SeqCst);
            }
            cleanup_ffi_data(array_ptr, data_schema_ptr);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(success_count.load(Ordering::SeqCst), 5);
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_concurrent_writes_different_files() {
    let temp_dir = tempdir().unwrap();
    let file_count = 8;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    let mut filenames = vec![];
    let mut schema_ptrs = vec![];

    for i in 0..file_count {
        let file_path = temp_dir.path().join(format!("concurrent_write_{}.parquet", i));
        let filename = file_path.to_string_lossy().to_string();
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
        filenames.push(filename);
        schema_ptrs.push(schema_ptr);
    }

    for i in 0..file_count {
        let filename = filenames[i].clone();
        let success_count = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            for _ in 0..2 {
                let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
                if NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr).is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                cleanup_ffi_data(array_ptr, data_schema_ptr);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(success_count.load(Ordering::SeqCst), file_count * 2);
    for (i, filename) in filenames.iter().enumerate() {
        close_writer_and_cleanup_schema(filename, schema_ptrs[i]);
    }
}

#[test]
fn test_concurrent_complete_writer_lifecycle() {
    let temp_dir = tempdir().unwrap();
    let thread_count = 6;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..thread_count {
        let temp_dir_path = temp_dir.path().to_path_buf();
        let success_count = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            let file_path = temp_dir_path.join(format!("lifecycle_{}.parquet", i));
            let filename = file_path.to_string_lossy().to_string();
            let (_schema, schema_ptr) = create_test_ffi_schema();

            if NativeParquetWriter::create_writer(filename.clone(), "test-index".to_string(), schema_ptr, vec![], vec![], vec![]).is_ok() {
                let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
                let write_ok = NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr).is_ok();
                cleanup_ffi_data(array_ptr, data_schema_ptr);

                if write_ok {
                    if let Ok(Some(metadata)) = NativeParquetWriter::finalize_writer(filename.clone()) {
                        if metadata.metadata.file_metadata().num_rows() == 3
                            && NativeParquetWriter::sync_to_disk(filename.clone()).is_ok()
                            && file_path.exists()
                        {
                            success_count.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
            cleanup_ffi_schema(schema_ptr);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(success_count.load(Ordering::SeqCst), thread_count);
}
