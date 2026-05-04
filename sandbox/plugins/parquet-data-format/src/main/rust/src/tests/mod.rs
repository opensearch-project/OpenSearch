/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

use crate::test_utils::*;
use crate::writer::NativeParquetWriter;

use std::fs::File;
use std::io::Read;

#[test]
fn test_create_writer_success() {
    let (_temp_dir, filename) = get_temp_file_path("test.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    assert!(NativeParquetWriter::has_writer(&filename));
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_create_writer_invalid_path() {
    let invalid_path = "/invalid/path/that/does/not/exist/test.parquet";
    let (_schema, schema_ptr) = create_test_ffi_schema();
    let result = NativeParquetWriter::create_writer(invalid_path.to_string(), "test-index".to_string(), schema_ptr, vec![], vec![], vec![]);
    assert!(result.is_err());
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_create_writer_invalid_schema_pointer() {
    let (_temp_dir, filename) = get_temp_file_path("invalid_schema.parquet");
    let result = NativeParquetWriter::create_writer(filename, "test-index".to_string(), 0, vec![], vec![], vec![]);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid schema address"));
}

#[test]
fn test_create_writer_multiple_times_same_file() {
    let (_temp_dir, filename) = get_temp_file_path("duplicate.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let (_, schema_ptr2) = create_test_ffi_schema();
    let result2 = NativeParquetWriter::create_writer(filename.clone(), "test-index".to_string(), schema_ptr2, vec![], vec![], vec![]);
    assert!(result2.is_err());
    assert!(result2.unwrap_err().to_string().contains("Writer already exists"));
    cleanup_ffi_schema(schema_ptr2);
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_write_data_success() {
    let (_temp_dir, filename) = get_temp_file_path("write_success.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
    let result = NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr);
    assert!(result.is_ok());
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_write_data_no_writer() {
    let (array_ptr, schema_ptr) = create_test_ffi_data().unwrap();
    let result = NativeParquetWriter::write_data("nonexistent.parquet".to_string(), array_ptr, schema_ptr);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Writer not found"));
    cleanup_ffi_data(array_ptr, schema_ptr);
}

#[test]
fn test_write_data_multiple_batches() {
    let (_temp_dir, filename) = get_temp_file_path("multi_write_ffi.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    for _ in 0..3 {
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_write_data_invalid_pointers() {
    let (_temp_dir, filename) = get_temp_file_path("invalid_ffi.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let result = NativeParquetWriter::write_data(filename.clone(), 0, 0);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid FFI addresses"));
    let result = NativeParquetWriter::write_data(filename.clone(), 0, schema_ptr);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid FFI addresses"));
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_write_data_incompatible_schema() {
    let (_temp_dir, filename) = get_temp_file_path("write_mismatch.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let (array_ptr, data_schema_ptr) = create_mismatched_ffi_data().unwrap();
    let result = NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr);
    assert!(result.is_err());
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_finalize_writer_success() {
    let (_temp_dir, filename) = get_temp_file_path("test_close.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    let result = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(result.is_ok());
    assert!(Path::new(&filename).exists());
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_finalize_writer_with_data_returns_correct_metadata() {
    let (_temp_dir, filename) = get_temp_file_path("close_with_data.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    for _ in 0..2 {
        let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
        NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr).unwrap();
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }
    let result = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(result.is_ok());
    let metadata = result.unwrap().unwrap();
    assert_eq!(metadata.metadata.file_metadata().num_rows(), 6);
    assert!(metadata.metadata.file_metadata().version() > 0);
    assert_ne!(metadata.crc32, 0, "CRC32 should be non-zero for a file with data");
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_close_nonexistent_writer() {
    let result = NativeParquetWriter::finalize_writer("nonexistent.parquet".to_string());
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Writer not found"));
}

#[test]
fn test_close_multiple_times_same_file() {
    let (_temp_dir, filename) = get_temp_file_path("test.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    let result1 = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(result1.is_ok());
    let result2 = NativeParquetWriter::finalize_writer(filename);
    assert!(result2.is_err());
    assert!(result2.unwrap_err().to_string().contains("Writer not found"));
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_sync_to_disk_success() {
    let (_temp_dir, filename) = get_temp_file_path("test_flush.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    let _ = NativeParquetWriter::finalize_writer(filename.clone());
    let result = NativeParquetWriter::sync_to_disk(filename);
    assert!(result.is_ok());
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_flush_nonexistent_file() {
    let result = NativeParquetWriter::sync_to_disk("nonexistent.parquet".to_string());
    assert!(result.is_err());
}

#[test]
fn test_complete_writer_lifecycle() {
    let (_temp_dir, filename) = get_temp_file_path("complete_workflow.parquet");
    let file_path = Path::new(&filename);
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

    for _ in 0..3 {
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }

    let close_result = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(close_result.is_ok());
    assert!(close_result.unwrap().is_some());

    assert!(NativeParquetWriter::sync_to_disk(filename.clone()).is_ok());
    assert!(file_path.exists());
    assert!(file_path.metadata().unwrap().len() > 0);

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_sorted_writer_ascending() {
    let (_temp_dir, filename) = get_temp_file_path("sorted_asc.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    let (ap1, sp1) = create_test_ffi_data_with_ids(
        vec![30, 10, 50], vec![Some("C"), Some("A"), Some("E")]
    ).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
    cleanup_ffi_data(ap1, sp1);

    let (ap2, sp2) = create_test_ffi_data_with_ids(
        vec![20, 40], vec![Some("B"), Some("D")]
    ).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
    cleanup_ffi_data(ap2, sp2);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![10, 20, 30, 40, 50], "Data should be sorted ascending by id");

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_sorted_writer_descending() {
    let (_temp_dir, filename) = get_temp_file_path("sorted_desc.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", true);

    let (ap1, sp1) = create_test_ffi_data_with_ids(
        vec![30, 10, 50], vec![Some("C"), Some("A"), Some("E")]
    ).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
    cleanup_ffi_data(ap1, sp1);

    let (ap2, sp2) = create_test_ffi_data_with_ids(
        vec![20, 40], vec![Some("B"), Some("D")]
    ).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
    cleanup_ffi_data(ap2, sp2);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![50, 40, 30, 20, 10], "Data should be sorted descending by id");

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_unsorted_writer_preserves_insertion_order() {
    let (_temp_dir, filename) = get_temp_file_path("unsorted.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

    let (ap1, sp1) = create_test_ffi_data_with_ids(
        vec![30, 10, 50], vec![Some("C"), Some("A"), Some("E")]
    ).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
    cleanup_ffi_data(ap1, sp1);

    let (ap2, sp2) = create_test_ffi_data_with_ids(
        vec![20, 40], vec![Some("B"), Some("D")]
    ).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
    cleanup_ffi_data(ap2, sp2);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![30, 10, 50, 20, 40], "Data should preserve insertion order");

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_get_filtered_writer_memory_usage_with_writers() {
    let (_temp_dir, filename1) = get_temp_file_path("test1.parquet");
    let (_temp_dir2, filename2) = get_temp_file_path("test2.parquet");
    let prefix = _temp_dir.path().to_string_lossy().to_string();
    let (_schema1, schema_ptr1) = create_writer_and_assert_success(&filename1);
    let (_schema2, schema_ptr2) = create_writer_and_assert_success(&filename2);
    let result = NativeParquetWriter::get_filtered_writer_memory_usage(prefix);
    assert!(result.is_ok());
    assert!(result.unwrap() >= 0);
    close_writer_and_cleanup_schema(&filename1, schema_ptr1);
    close_writer_and_cleanup_schema(&filename2, schema_ptr2);
}

// CRC32 tests

fn compute_file_crc32(path: &str) -> u32 {
    let mut file = File::open(path).unwrap();
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).unwrap();
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }
    hasher.finalize()
}

#[test]
fn test_crc32_matches_reread_with_data() {
    let (_temp_dir, filename) = get_temp_file_path("crc32_with_data.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

    for _ in 0..3 {
        let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
        NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr).unwrap();
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }

    let result = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(result.is_ok());
    let finalize_result = result.unwrap().unwrap();
    let streaming_crc32 = finalize_result.crc32;

    assert_eq!(finalize_result.metadata.file_metadata().num_rows(), 9);

    let reread_crc32 = compute_file_crc32(&filename);
    assert_eq!(streaming_crc32, reread_crc32);
    assert_ne!(streaming_crc32, 0);

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_crc32_differs_for_different_content() {
    let (_temp_dir1, filename1) = get_temp_file_path("crc32_diff_a.parquet");
    let (_schema1, schema_ptr1) = create_writer_and_assert_success(&filename1);
    let result1 = NativeParquetWriter::finalize_writer(filename1.clone());
    let crc32_empty = result1.unwrap().unwrap().crc32;
    cleanup_ffi_schema(schema_ptr1);

    let (_temp_dir2, filename2) = get_temp_file_path("crc32_diff_b.parquet");
    let (_schema2, schema_ptr2) = create_writer_and_assert_success(&filename2);
    let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
    NativeParquetWriter::write_data(filename2.clone(), array_ptr, data_schema_ptr).unwrap();
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    let result2 = NativeParquetWriter::finalize_writer(filename2.clone());
    let crc32_with_data = result2.unwrap().unwrap().crc32;
    cleanup_ffi_schema(schema_ptr2);

    assert_ne!(crc32_empty, crc32_with_data);
}

// Concurrency tests

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
                let (ap, sp) = create_test_ffi_data().unwrap();
                let _ = NativeParquetWriter::write_data(filename.clone(), ap, sp);
                cleanup_ffi_data(ap, sp);
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

    let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
    cleanup_ffi_data(array_ptr, data_schema_ptr);

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
