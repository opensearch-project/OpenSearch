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
use crate::writer::SETTINGS_STORE;
use crate::native_settings::NativeSettings;

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
    let result = NativeParquetWriter::create_writer(invalid_path.to_string(), "test-index".to_string(), schema_ptr, vec![], vec![], vec![], 0);
    assert!(result.is_err());
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_create_writer_invalid_schema_pointer() {
    let (_temp_dir, filename) = get_temp_file_path("invalid_schema.parquet");
    let result = NativeParquetWriter::create_writer(filename, "test-index".to_string(), 0, vec![], vec![], vec![], 0);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid schema address"));
}

#[test]
fn test_create_writer_multiple_times_same_file() {
    let (_temp_dir, filename) = get_temp_file_path("duplicate.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let (_, schema_ptr2) = create_test_ffi_schema();
    let result2 = NativeParquetWriter::create_writer(filename.clone(), "test-index".to_string(), schema_ptr2, vec![], vec![], vec![], 0);
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

// ===== Arrow IPC staging path tests =====

#[test]
fn test_ipc_staging_sorted_writer_creates_and_cleans_up_staging_file() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_cleanup.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    // With eager sort-and-write, no staging file exists until a chunk is flushed.
    // The writer accumulates in memory. Verify the writer is open.
    assert!(NativeParquetWriter::has_writer(&filename), "Writer should be open");

    let (ap, sp) = create_test_ffi_data_with_ids(vec![30, 10, 20], vec![Some("C"), Some("A"), Some("B")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // The final Parquet file should exist
    assert!(Path::new(&filename).exists(), "Final Parquet file should exist");

    // Verify data is sorted
    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![10, 20, 30]);

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_ipc_staging_has_writer_returns_true() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_has_writer.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    assert!(NativeParquetWriter::has_writer(&filename), "has_writer should return true for IPC writer");

    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_ipc_staging_duplicate_writer_rejected() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_dup.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    let (_, schema_ptr2) = create_test_ffi_schema();
    let result = NativeParquetWriter::create_writer(
        filename.clone(), "test-index".to_string(), schema_ptr2,
        vec!["id".to_string()], vec![false], vec![false], 0
    );
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Writer already exists"));

    cleanup_ffi_schema(schema_ptr2);
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_ipc_staging_empty_data_produces_valid_parquet() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_empty.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    // Finalize without writing any data
    let result = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(result.is_ok());
    assert!(Path::new(&filename).exists(), "Empty Parquet file should be created");

    let metadata = result.unwrap().unwrap();
    assert_eq!(metadata.metadata.file_metadata().num_rows(), 0);

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_ipc_staging_multi_batch_sort() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_multi_batch.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    // Write multiple batches with interleaved values
    let (ap1, sp1) = create_test_ffi_data_with_ids(vec![50, 10], vec![Some("E"), Some("A")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
    cleanup_ffi_data(ap1, sp1);

    let (ap2, sp2) = create_test_ffi_data_with_ids(vec![30, 20], vec![Some("C"), Some("B")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
    cleanup_ffi_data(ap2, sp2);

    let (ap3, sp3) = create_test_ffi_data_with_ids(vec![40, 60], vec![Some("D"), Some("F")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap3, sp3).unwrap();
    cleanup_ffi_data(ap3, sp3);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![10, 20, 30, 40, 50, 60], "Multiple IPC batches should be sorted correctly");

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_ipc_staging_descending_sort() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_desc.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", true);

    let (ap, sp) = create_test_ffi_data_with_ids(vec![10, 30, 20], vec![Some("A"), Some("C"), Some("B")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![30, 20, 10], "IPC path should support descending sort");

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_ipc_and_parquet_writers_coexist() {
    let (_temp_dir1, sorted_file) = get_temp_file_path("ipc_sorted.parquet");
    let (_temp_dir2, unsorted_file) = get_temp_file_path("parquet_unsorted.parquet");

    // Create one IPC writer (sorted) and one Parquet writer (unsorted)
    let (_schema1, sp1) = create_sorted_writer_and_assert_success(&sorted_file, "id", false);
    let (_schema2, sp2) = create_writer_and_assert_success(&unsorted_file);

    // Write to both
    let (ap1, dp1) = create_test_ffi_data_with_ids(vec![30, 10, 20], vec![Some("C"), Some("A"), Some("B")]).unwrap();
    NativeParquetWriter::write_data(sorted_file.clone(), ap1, dp1).unwrap();
    cleanup_ffi_data(ap1, dp1);

    let (ap2, dp2) = create_test_ffi_data_with_ids(vec![30, 10, 20], vec![Some("C"), Some("A"), Some("B")]).unwrap();
    NativeParquetWriter::write_data(unsorted_file.clone(), ap2, dp2).unwrap();
    cleanup_ffi_data(ap2, dp2);

    // Finalize both
    NativeParquetWriter::finalize_writer(sorted_file.clone()).unwrap();
    NativeParquetWriter::finalize_writer(unsorted_file.clone()).unwrap();

    // Sorted file should be sorted
    let sorted_ids = read_parquet_file_sorted_ids(&sorted_file);
    assert_eq!(sorted_ids, vec![10, 20, 30]);

    // Unsorted file should preserve insertion order
    let unsorted_ids = read_parquet_file_sorted_ids(&unsorted_file);
    assert_eq!(unsorted_ids, vec![30, 10, 20]);

    cleanup_ffi_schema(sp1);
    cleanup_ffi_schema(sp2);
}

#[test]
fn test_ipc_staging_concurrent_sorted_writers() {
    let temp_dir = tempdir().unwrap();
    let thread_count = 6;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..thread_count {
        let temp_dir_path = temp_dir.path().to_path_buf();
        let success_count = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            let file_path = temp_dir_path.join(format!("ipc_concurrent_{}.parquet", i));
            let filename = file_path.to_string_lossy().to_string();
            let (_schema, schema_ptr) = create_test_ffi_schema();

            if NativeParquetWriter::create_writer(
                filename.clone(), "test-index".to_string(), schema_ptr,
                vec!["id".to_string()], vec![false], vec![false], 0
            ).is_ok() {
                let (ap, sp) = create_test_ffi_data_with_ids(
                    vec![30, 10, 20], vec![Some("C"), Some("A"), Some("B")]
                ).unwrap();
                let write_ok = NativeParquetWriter::write_data(filename.clone(), ap, sp).is_ok();
                cleanup_ffi_data(ap, sp);

                if write_ok {
                    if let Ok(Some(metadata)) = NativeParquetWriter::finalize_writer(filename.clone()) {
                        if metadata.metadata.file_metadata().num_rows() == 3 {
                            let ids = read_parquet_file_sorted_ids(&filename);
                            if ids == vec![10, 20, 30] {
                                success_count.fetch_add(1, Ordering::SeqCst);
                            }
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

#[test]
fn test_ipc_staging_complete_lifecycle_with_sync() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_lifecycle.parquet");
    let file_path = Path::new(&filename);
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    for batch_ids in [vec![50, 30], vec![10, 40], vec![20, 60]] {
        let names: Vec<Option<&str>> = batch_ids.iter().map(|_| Some("x")).collect();
        let (ap, sp) = create_test_ffi_data_with_ids(batch_ids, names).unwrap();
        NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
        cleanup_ffi_data(ap, sp);
    }

    let result = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(result.is_ok());
    let metadata = result.unwrap().unwrap();
    assert_eq!(metadata.metadata.file_metadata().num_rows(), 6);

    assert!(NativeParquetWriter::sync_to_disk(filename.clone()).is_ok());
    assert!(file_path.exists());
    assert!(file_path.metadata().unwrap().len() > 0);

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![10, 20, 30, 40, 50, 60]);

    let read_metadata = NativeParquetWriter::get_file_metadata(filename.clone()).unwrap();
    assert_eq!(read_metadata.num_rows(), 6);

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

            if NativeParquetWriter::create_writer(filename.clone(), "test-index".to_string(), schema_ptr, vec![], vec![], vec![], 0).is_ok() {
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

#[test]
fn test_bloom_filter_false_propagates_through_settings_store() {
    let index_name = "test-bloom-false-index-unique1";
    let settings = NativeSettings {
        index_name: Some(index_name.to_string()),
        bloom_filter_enabled: Some(false),
        ..Default::default()
    };
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let stored = SETTINGS_STORE.get(index_name).unwrap();
    assert_eq!(stored.bloom_filter_enabled, Some(false));
    assert_eq!(stored.get_bloom_filter_enabled(), false);
    drop(stored);

    let (_temp_dir, filename) = get_temp_file_path("bloom_test.parquet");
    let (_schema, schema_ptr) = create_test_ffi_schema();
    let result = NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr, vec![], vec![], vec![], 0
    );
    assert!(result.is_ok());

    let stored_after = SETTINGS_STORE.get(index_name).unwrap();
    assert_eq!(
        stored_after.bloom_filter_enabled, Some(false),
        "bloom_filter_enabled should remain false after create_writer, but got: {:?}",
        stored_after.bloom_filter_enabled
    );
    assert_eq!(stored_after.get_bloom_filter_enabled(), false);
    drop(stored_after);

    close_writer_and_cleanup_schema(&filename, schema_ptr);
    SETTINGS_STORE.remove(index_name);
}

#[test]
fn test_bloom_filter_default_when_no_settings() {
    let index_name = "test-bloom-default-index-unique2";

    let (_temp_dir, filename) = get_temp_file_path("bloom_default.parquet");
    let (_schema, schema_ptr) = create_test_ffi_schema();
    let result = NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr, vec![], vec![], vec![], 0
    );
    assert!(result.is_ok());

    let stored = SETTINGS_STORE.get(index_name).unwrap();
    assert_eq!(
        stored.get_bloom_filter_enabled(), true,
        "Default bloom_filter_enabled should be true when no settings exist"
    );
    drop(stored);

    close_writer_and_cleanup_schema(&filename, schema_ptr);
    SETTINGS_STORE.remove(index_name);
}

// TODO: Re-enable when sort_small_file/sort_large_file are uncommented
// #[test]
// fn test_sort_large_file_permutation_maps_original_row_ids() {
//     run_sort_permutation_test(true, "large_file");
// }
//
// #[test]
// fn test_sort_small_file_permutation_maps_original_row_ids() {
//     run_sort_permutation_test(false, "small_file");
// }

// TODO: Re-enable when sort_small_file/sort_large_file are uncommented
#[allow(dead_code)]
/// Runs the full sort permutation test for either sort_small_file or sort_large_file.
/// Both paths must produce identical mapping[original_row_id] = new_position.
fn run_sort_permutation_test(_use_large_file: bool, _label: &str) {
    // Disabled: sort_small_file and sort_large_file are currently commented out in writer.rs
    unimplemented!("sort_small_file/sort_large_file are commented out");
}

#[allow(dead_code)]
/// Helper: creates an IPC file with (age: Int64, __row_id__: Int64) columns.
fn create_test_ipc_file(dir: &Path, ages: &[i64], row_ids: &[i64]) -> String {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_ipc::writer::FileWriter as IpcFileWriter;
    use crate::merge::schema::ROW_ID_COLUMN_NAME;

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int64, false),
        Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]));

    let ipc_path = dir.join("staging.arrow_ipc_staging").to_string_lossy().to_string();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ages.to_vec())),
            Arc::new(Int64Array::from(row_ids.to_vec())),
        ],
    ).unwrap();

    let file = File::create(&ipc_path).unwrap();
    let mut ipc_writer = IpcFileWriter::try_new(file, &schema).unwrap();
    ipc_writer.write(&batch).unwrap();
    ipc_writer.finish().unwrap();

    ipc_path
}

// ===== SortingChunkedWriter and finalize_sorted_chunks tests =====
// These tests exercise the chunked sort path by setting a very small memory threshold
// to force multiple flush_and_sort_chunk calls, then verify:
// 1. Row IDs are sequential 0..N in each chunk and in the final output
// 2. The permutation mapping correctly maps original_row_id → new_position
// 3. Data is correctly sorted in the final output

/// Helper: creates FFI schema pointer for a schema with (age: Int32, name: Utf8, __row_id__: Int64).
fn create_row_id_schema_ptr() -> (Arc<arrow::datatypes::Schema>, i64) {
    use arrow::datatypes::{DataType, Field, Schema};
    use crate::merge::schema::ROW_ID_COLUMN_NAME;

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]));
    let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (schema, schema_ptr)
}

/// Helper: creates FFI data with a schema that includes __row_id__ column.
/// Returns (array_ptr, schema_ptr) for use with write_data.
fn create_ffi_data_with_row_id(
    ages: Vec<i32>,
    names: Vec<Option<&str>>,
    row_ids: Vec<i64>,
) -> (i64, i64) {
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use crate::merge::schema::ROW_ID_COLUMN_NAME;

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]));
    let age_array: Arc<dyn Array> = Arc::new(Int32Array::from(ages));
    let name_array: Arc<dyn Array> = Arc::new(StringArray::from(names));
    let row_id_array: Arc<dyn Array> = Arc::new(Int64Array::from(row_ids));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![age_array, name_array, row_id_array],
    ).unwrap();
    let struct_array = arrow::array::StructArray::from(record_batch);
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&struct_array.to_data()).unwrap();
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (array_ptr, schema_ptr)
}

/// Helper: reads a Parquet file and returns the __row_id__ column values.
fn read_row_ids_from_parquet(filename: &str) -> Vec<i64> {
    use arrow::array::Int64Array;
    use arrow::compute::concat_batches;
    use crate::merge::schema::ROW_ID_COLUMN_NAME;

    let batches = read_parquet_file(filename);
    if batches.is_empty() {
        return vec![];
    }
    let combined = concat_batches(&batches[0].schema(), &batches).unwrap();
    let idx = combined.schema().index_of(ROW_ID_COLUMN_NAME)
        .expect("__row_id__ column should exist");
    let col = combined.column(idx).as_any().downcast_ref::<Int64Array>()
        .expect("__row_id__ should be Int64");
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// Helper: reads a Parquet file and returns the "age" column values as i32.
fn read_ages_from_parquet(filename: &str) -> Vec<i32> {
    use arrow::array::Int32Array;
    use arrow::compute::concat_batches;

    let batches = read_parquet_file(filename);
    if batches.is_empty() {
        return vec![];
    }
    let combined = concat_batches(&batches[0].schema(), &batches).unwrap();
    let idx = combined.schema().index_of("age").expect("age column should exist");
    let col = combined.column(idx).as_any().downcast_ref::<Int32Array>()
        .expect("age should be Int32");
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// Test: Single chunk (all data fits in memory) — verifies row IDs are sequential
/// and permutation mapping is correct.
#[test]
fn test_chunked_writer_single_chunk_row_ids_sequential() {
    let (_temp_dir, filename) = get_temp_file_path("chunked_single.parquet");
    let index_name = "test-chunked-single";

    // Configure with large threshold so everything fits in one chunk
    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024); // 10MB — won't chunk
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    // Create writer with sort on "age" ascending
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    let result = NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], 0,
    );
    assert!(result.is_ok(), "create_writer failed: {:?}", result.err());

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 50, 20, 40],
        vec![Some("C"), Some("A"), Some("E"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    // Verify output is sorted by age ascending
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![10, 20, 30, 40, 50], "Output should be sorted by age ASC");

    // Verify __row_id__ is sequential 0..N
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..5).collect();
    assert_eq!(row_ids, expected_sequential, "__row_id__ should be sequential 0..4");

    // Verify permutation mapping
    // Original: age=[30,10,50,20,40], row_ids=[0,1,2,3,4]
    // Sorted:   age=[10,20,30,40,50] → original row_ids were [1,3,0,4,2]
    // mapping[original_row_id] = new_position
    // mapping[0]=2, mapping[1]=0, mapping[2]=4, mapping[3]=1, mapping[4]=3
    let mapping = finalize_result.row_id_mapping.expect("Should have row_id_mapping");
    assert_eq!(mapping.len(), 5);
    assert_eq!(mapping, vec![2, 0, 4, 1, 3],
        "Permutation mapping should map original row IDs to new sorted positions");

    SETTINGS_STORE.remove(index_name);
}

/// Test: Multiple chunks forced by small memory threshold — verifies row IDs
/// are sequential in the final merged output and permutation is correct.
#[test]
fn test_chunked_writer_multi_chunk_row_ids_sequential() {
    let (_temp_dir, filename) = get_temp_file_path("chunked_multi.parquet");
    let index_name = "test-chunked-multi";

    // Configure with tiny threshold to force multiple chunks
    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Very small — forces chunking
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    let result = NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], 0,
    );
    assert!(result.is_ok(), "create_writer failed: {:?}", result.err());

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![Some("E"), Some("C"), Some("A"), Some("D"), Some("B"),
             Some("F"), Some("G"), Some("H"), Some("I"), Some("J")],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    // Verify output is sorted by age ascending
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![5, 10, 15, 20, 30, 35, 40, 45, 50, 60],
        "Output should be sorted by age ASC");

    // Verify __row_id__ is sequential 0..N
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..10).collect();
    assert_eq!(row_ids, expected_sequential,
        "__row_id__ should be sequential 0..9 in merged output");

    // Verify permutation mapping exists and has correct length
    let mapping = finalize_result.row_id_mapping.expect("Should have row_id_mapping");
    assert_eq!(mapping.len(), 10);

    // Verify permutation correctness:
    // Original: age=[50,30,10,40,20,60,5,45,35,15], row_ids=[0..9]
    // Sorted:   age=[5,10,15,20,30,35,40,45,50,60]
    // Original positions: [6, 2, 9, 4, 1, 8, 3, 7, 0, 5]
    // mapping[original_row_id] = new_position
    // mapping[0]=8 (age=50→pos8), mapping[1]=4 (age=30→pos4), mapping[2]=1 (age=10→pos1)
    // mapping[3]=6 (age=40→pos6), mapping[4]=3 (age=20→pos3), mapping[5]=9 (age=60→pos9)
    // mapping[6]=0 (age=5→pos0), mapping[7]=7 (age=45→pos7), mapping[8]=5 (age=35→pos5)
    // mapping[9]=2 (age=15→pos2)
    let expected_mapping: Vec<i64> = vec![8, 4, 1, 6, 3, 9, 0, 7, 5, 2];
    assert_eq!(mapping, expected_mapping,
        "Permutation mapping should correctly map original row IDs to sorted positions");

    SETTINGS_STORE.remove(index_name);
}

/// Test: Multiple chunks with descending sort — verifies correctness with reverse order.
#[test]
fn test_chunked_writer_multi_chunk_descending_sort() {
    let (_temp_dir, filename) = get_temp_file_path("chunked_desc.parquet");
    let index_name = "test-chunked-desc";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![true]; // descending
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![true], vec![false], 0,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![10, 50, 30, 20, 40],
        vec![Some("A"), Some("E"), Some("C"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    // Verify output is sorted by age descending
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![50, 40, 30, 20, 10], "Output should be sorted by age DESC");

    // Verify __row_id__ is sequential 0..N
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..5).collect();
    assert_eq!(row_ids, expected_sequential, "__row_id__ should be sequential 0..4");

    // Verify permutation:
    // Original: age=[10,50,30,20,40], row_ids=[0,1,2,3,4]
    // Sorted DESC: age=[50,40,30,20,10] → original row_ids [1,4,2,3,0]
    // mapping[0]=4, mapping[1]=0, mapping[2]=2, mapping[3]=3, mapping[4]=1
    let mapping = finalize_result.row_id_mapping.expect("Should have row_id_mapping");
    assert_eq!(mapping, vec![4, 0, 2, 3, 1]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: Multiple write_data calls with small threshold — verifies that data
/// written across multiple batches is correctly sorted and has sequential row IDs.
#[test]
fn test_chunked_writer_multiple_write_calls() {
    let (_temp_dir, filename) = get_temp_file_path("chunked_multi_write.parquet");
    let index_name = "test-chunked-multi-write";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(128); // Small enough to force chunking
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    // First batch: row_ids 0..4
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], 0,
    ).unwrap();

    let (ap1, sp1) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20],
        vec![Some("E"), Some("C"), Some("A"), Some("D"), Some("B")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();

    // Second batch: row_ids 5..9
    let (ap2, sp2) = create_ffi_data_with_row_id(
        vec![25, 55, 15, 35, 45],
        vec![Some("F"), Some("G"), Some("H"), Some("I"), Some("J")],
        vec![5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    // Verify sorted output
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![10, 15, 20, 25, 30, 35, 40, 45, 50, 55],
        "Output should be globally sorted by age ASC");

    // Verify sequential row IDs
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..10).collect();
    assert_eq!(row_ids, expected_sequential,
        "__row_id__ should be sequential 0..9");

    // Verify permutation mapping
    let mapping = finalize_result.row_id_mapping.expect("Should have row_id_mapping");
    assert_eq!(mapping.len(), 10);

    // Original: age=[50,30,10,40,20,25,55,15,35,45], row_ids=[0..9]
    // Sorted:   age=[10,15,20,25,30,35,40,45,50,55]
    // Original positions: [2, 7, 4, 5, 1, 8, 3, 9, 0, 6]
    // mapping[0]=8, mapping[1]=4, mapping[2]=0, mapping[3]=6, mapping[4]=2
    // mapping[5]=3, mapping[6]=9, mapping[7]=1, mapping[8]=5, mapping[9]=7
    let expected_mapping: Vec<i64> = vec![8, 4, 0, 6, 2, 3, 9, 1, 5, 7];
    assert_eq!(mapping, expected_mapping,
        "Permutation should correctly map across multiple write calls");

    SETTINGS_STORE.remove(index_name);
}

/// Test: Empty writer finalize — no data written, should produce valid empty Parquet.
#[test]
fn test_chunked_writer_empty_finalize() {
    let (_temp_dir, filename) = get_temp_file_path("chunked_empty.parquet");
    let index_name = "test-chunked-empty";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    // Just create the writer with the schema, don't write data
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], 0,
    ).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    assert_eq!(finalize_result.metadata.file_metadata().num_rows(), 0,
        "Empty writer should produce 0-row Parquet");
    assert!(finalize_result.row_id_mapping.is_none(),
        "Empty writer should have no row_id_mapping");

    SETTINGS_STORE.remove(index_name);
}

/// Test: Verifies that the permutation mapping is invertible — applying it to
/// the original row order produces the sorted order.
#[test]
fn test_chunked_writer_permutation_is_invertible() {
    let (_temp_dir, filename) = get_temp_file_path("chunked_invertible.parquet");
    let index_name = "test-chunked-invertible";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let original_ages = vec![55, 22, 88, 11, 44, 77, 33, 66, 99, 0];
    let row_ids: Vec<i64> = (0..10).collect();
    let names: Vec<Option<&str>> = (0..10).map(|_| Some("x")).collect();

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], 0,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        original_ages.clone(), names, row_ids,
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping.len(), 10);

    // Verify: for each original row, mapping[original_row_id] gives its position
    // in the sorted output. So sorted_ages[mapping[i]] should reconstruct the sort.
    let sorted_ages = read_ages_from_parquet(&filename);
    assert_eq!(sorted_ages, vec![0, 11, 22, 33, 44, 55, 66, 77, 88, 99]);

    // Verify mapping is a valid permutation (contains each value 0..N exactly once)
    let mut sorted_mapping = mapping.clone();
    sorted_mapping.sort();
    let expected_perm: Vec<i64> = (0..10).collect();
    assert_eq!(sorted_mapping, expected_perm,
        "Mapping should be a valid permutation of 0..N");

    // Verify: original_ages[i] should appear at position mapping[i] in sorted output
    for i in 0..10 {
        let new_pos = mapping[i] as usize;
        assert_eq!(sorted_ages[new_pos], original_ages[i],
            "original_ages[{}]={} should be at sorted position {}, but found {}",
            i, original_ages[i], new_pos, sorted_ages[new_pos]);
    }

    SETTINGS_STORE.remove(index_name);
}

/// Test: Large dataset with forced multi-chunk — stress test with more rows.
#[test]
fn test_chunked_writer_large_dataset_multi_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("chunked_large.parquet");
    let index_name = "test-chunked-large";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(256); // Force many chunks
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let num_rows = 50i32;
    let ages: Vec<i32> = (0..num_rows).map(|i| (i * 7 + 13) % 100).collect();
    let row_ids: Vec<i64> = (0..num_rows as i64).collect();
    let names: Vec<Option<&str>> = (0..num_rows as usize).map(|_| Some("x")).collect();

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], 0,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        ages.clone(), names, row_ids,
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    // Verify output is sorted
    let sorted_ages = read_ages_from_parquet(&filename);
    let mut expected_sorted = ages.clone();
    expected_sorted.sort();
    assert_eq!(sorted_ages, expected_sorted, "Output should be sorted ascending");

    // Verify sequential row IDs
    let row_ids_in_file = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..num_rows as i64).collect();
    assert_eq!(row_ids_in_file, expected_sequential,
        "__row_id__ should be sequential 0..{}", num_rows - 1);

    // Verify permutation
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping.len(), num_rows as usize);

    // Verify it's a valid permutation
    let mut sorted_mapping = mapping.clone();
    sorted_mapping.sort();
    assert_eq!(sorted_mapping, expected_sequential, "Mapping should be a valid permutation");

    // Verify correctness: original_ages[i] at sorted position mapping[i]
    for i in 0..num_rows as usize {
        let new_pos = mapping[i] as usize;
        assert_eq!(sorted_ages[new_pos], ages[i],
            "ages[{}]={} should be at sorted position {}", i, ages[i], new_pos);
    }

    SETTINGS_STORE.remove(index_name);
}

// ===== Writer generation tests =====

/// Helper: reads the writer_generation from Parquet file metadata.
fn read_writer_generation_from_parquet(filename: &str) -> Option<i64> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = File::open(filename).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata().file_metadata();
    metadata.key_value_metadata()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "opensearch.writer_generation")
                .and_then(|kv| kv.value.as_ref())
                .and_then(|v| v.parse::<i64>().ok())
        })
}

/// Test: writer_generation is stored in Parquet metadata for sorted single-chunk output.
#[test]
fn test_chunked_writer_generation_in_metadata_single_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("gen_single.parquet");
    let index_name = "test-gen-single";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let writer_generation = 42i64;
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], writer_generation,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 20],
        vec![Some("C"), Some("A"), Some("B")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(gen, Some(42), "writer_generation=42 should be in Parquet metadata");

    SETTINGS_STORE.remove(index_name);
}

/// Test: writer_generation is stored in Parquet metadata for multi-chunk merged output.
/// NOTE: Currently the k-way merge path (merge_sorted) does not propagate writer_generation
/// into the final merged file's metadata. This test documents the current behavior.
/// TODO: Fix merge_sorted to accept and propagate writer_generation.
#[test]
fn test_chunked_writer_generation_in_metadata_multi_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("gen_multi.parquet");
    let index_name = "test-gen-multi";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Force multiple chunks
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let writer_generation = 7i64;
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], writer_generation,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![Some("E"), Some("C"), Some("A"), Some("D"), Some("B"),
             Some("F"), Some("G"), Some("H"), Some("I"), Some("J")],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Known limitation: merge_sorted doesn't propagate writer_generation to output metadata.
    // The generation is lost during k-way merge. This should be fixed.
    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(gen, None,
        "BUG: writer_generation is currently lost in multi-chunk merge path");

    // Data correctness is still maintained
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![5, 10, 15, 20, 30, 35, 40, 45, 50, 60]);

    let row_ids = read_row_ids_from_parquet(&filename);
    let expected: Vec<i64> = (0..10).collect();
    assert_eq!(row_ids, expected);

    SETTINGS_STORE.remove(index_name);
}

/// Test: writer_generation=0 is correctly stored (edge case — zero is a valid generation).
#[test]
fn test_chunked_writer_generation_zero() {
    let (_temp_dir, filename) = get_temp_file_path("gen_zero.parquet");
    let index_name = "test-gen-zero";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], 0,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![20, 10],
        vec![Some("B"), Some("A")],
        vec![0, 1],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(gen, Some(0), "writer_generation=0 should be stored in metadata");

    SETTINGS_STORE.remove(index_name);
}

/// Test: Large writer_generation value is preserved through the single-chunk sort path.
#[test]
fn test_chunked_writer_generation_large_value() {
    let (_temp_dir, filename) = get_temp_file_path("gen_large.parquet");
    let index_name = "test-gen-large";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024); // Large threshold — single chunk
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let writer_generation = 999_999i64;
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], writer_generation,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![80, 20, 60, 40, 10, 90, 50, 30, 70],
        vec![Some("H"), Some("B"), Some("F"), Some("D"), Some("A"),
             Some("I"), Some("E"), Some("C"), Some("G")],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    // Verify writer_generation in metadata
    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(gen, Some(999_999),
        "Large writer_generation should be preserved in Parquet metadata");

    // Verify sort correctness
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![10, 20, 30, 40, 50, 60, 70, 80, 90]);

    // Verify sequential row IDs
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected: Vec<i64> = (0..9).collect();
    assert_eq!(row_ids, expected);

    // Verify permutation
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping.len(), 9);
    // Original: age=[80,20,60,40,10,90,50,30,70], row_ids=[0..8]
    // Sorted:   age=[10,20,30,40,50,60,70,80,90]
    // mapping[0]=7(80→pos7), mapping[1]=1(20→pos1), mapping[2]=5(60→pos5),
    // mapping[3]=3(40→pos3), mapping[4]=0(10→pos0), mapping[5]=8(90→pos8),
    // mapping[6]=4(50→pos4), mapping[7]=2(30→pos2), mapping[8]=6(70→pos6)
    assert_eq!(mapping, vec![7, 1, 5, 3, 0, 8, 4, 2, 6]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: writer_generation is preserved for unsorted (direct Parquet) writer path.
#[test]
fn test_unsorted_writer_generation_in_metadata() {
    let (_temp_dir, filename) = get_temp_file_path("gen_unsorted.parquet");
    let index_name = "test-gen-unsorted";

    let settings = NativeSettings::default();
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let writer_generation = 17i64;
    let (_, schema_ptr) = create_row_id_schema_ptr();
    // No sort columns — uses direct Parquet writer path
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec![], vec![], vec![], writer_generation,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 20],
        vec![Some("C"), Some("A"), Some("B")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(gen, Some(17),
        "writer_generation should be in metadata for unsorted writer too");

    // Unsorted: data should preserve insertion order
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![30, 10, 20]);

    SETTINGS_STORE.remove(index_name);
}

// ===== CRC32 tests for sorted chunked writer =====

/// Test: CRC32 is non-zero and matches file content for single-chunk sorted output.
#[test]
fn test_chunked_writer_crc32_single_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("crc_single.parquet");
    let index_name = "test-crc-single";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024); // Single chunk
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], 0,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 50, 20, 40],
        vec![Some("C"), Some("A"), Some("E"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    // CRC should be non-zero for a file with data
    assert_ne!(finalize_result.crc32, 0,
        "CRC32 should be non-zero for single-chunk sorted output");

    // Verify CRC matches actual file content
    let actual_crc = compute_file_crc32(&filename);
    assert_eq!(finalize_result.crc32, actual_crc,
        "CRC32 from finalize should match recomputed CRC of the file on disk");

    SETTINGS_STORE.remove(index_name);
}

/// Test: CRC32 is non-zero for multi-chunk merged output.
#[test]
fn test_chunked_writer_crc32_multi_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("crc_multi.parquet");
    let index_name = "test-crc-multi";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Force multiple chunks
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(), index_name.to_string(), schema_ptr,
        vec!["age".to_string()], vec![false], vec![false], 0,
    ).unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![Some("E"), Some("C"), Some("A"), Some("D"), Some("B"),
             Some("F"), Some("G"), Some("H"), Some("I"), Some("J")],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone()).unwrap().unwrap();

    // Multi-chunk merge also produces a CRC (from merge_sorted's CrcWriter)
    // Verify it matches the actual file
    let actual_crc = compute_file_crc32(&filename);
    assert_eq!(finalize_result.crc32, actual_crc,
        "CRC32 from finalize should match recomputed CRC for multi-chunk output");

    SETTINGS_STORE.remove(index_name);
}

/// Test: Different data produces different CRC32 values (sanity check).
#[test]
fn test_chunked_writer_crc32_differs_for_different_data() {
    let (_temp_dir1, filename1) = get_temp_file_path("crc_diff1.parquet");
    let (_temp_dir2, filename2) = get_temp_file_path("crc_diff2.parquet");
    let index_name1 = "test-crc-diff1";
    let index_name2 = "test-crc-diff2";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name1.to_string(), settings.clone());
    SETTINGS_STORE.insert(index_name2.to_string(), settings);

    // Writer 1
    let (_schema, schema_ptr1) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename1.clone(), index_name1.to_string(), schema_ptr1,
        vec!["age".to_string()], vec![false], vec![false], 0,
    ).unwrap();
    let (ap1, sp1) = create_ffi_data_with_row_id(
        vec![10, 20, 30],
        vec![Some("A"), Some("B"), Some("C")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename1.clone(), ap1, sp1).unwrap();
    let result1 = NativeParquetWriter::finalize_writer(filename1.clone()).unwrap().unwrap();

    // Writer 2 — different data
    let (_schema, schema_ptr2) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename2.clone(), index_name2.to_string(), schema_ptr2,
        vec!["age".to_string()], vec![false], vec![false], 0,
    ).unwrap();
    let (ap2, sp2) = create_ffi_data_with_row_id(
        vec![99, 88, 77],
        vec![Some("X"), Some("Y"), Some("Z")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename2.clone(), ap2, sp2).unwrap();
    let result2 = NativeParquetWriter::finalize_writer(filename2.clone()).unwrap().unwrap();

    assert_ne!(result1.crc32, result2.crc32,
        "Different data should produce different CRC32 values");

    SETTINGS_STORE.remove(index_name1);
    SETTINGS_STORE.remove(index_name2);
}
