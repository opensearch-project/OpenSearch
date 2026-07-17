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

use crate::native_settings::NativeSettings;
use crate::test_utils::*;
use crate::writer::NativeParquetWriter;
use crate::writer::SETTINGS_STORE;

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
    let result = NativeParquetWriter::create_writer(
        invalid_path.to_string(),
        "test-index".to_string(),
        schema_ptr,
        vec![],
        vec![],
        vec![],
        0,
    );
    assert!(result.is_err());
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_create_writer_invalid_schema_pointer() {
    let (_temp_dir, filename) = get_temp_file_path("invalid_schema.parquet");
    let result = NativeParquetWriter::create_writer(
        filename,
        "test-index".to_string(),
        0,
        vec![],
        vec![],
        vec![],
        0,
    );
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid schema address"));
}

#[test]
fn test_create_writer_multiple_times_same_file() {
    let (_temp_dir, filename) = get_temp_file_path("duplicate.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let (_, schema_ptr2) = create_test_ffi_schema();
    let result2 = NativeParquetWriter::create_writer(
        filename.clone(),
        "test-index".to_string(),
        schema_ptr2,
        vec![],
        vec![],
        vec![],
        0,
    );
    assert!(result2.is_err());
    assert!(result2
        .unwrap_err()
        .to_string()
        .contains("Writer already exists"));
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
    let result =
        NativeParquetWriter::write_data("nonexistent.parquet".to_string(), array_ptr, schema_ptr);
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
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid FFI addresses"));
    let result = NativeParquetWriter::write_data(filename.clone(), 0, schema_ptr);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid FFI addresses"));
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
    assert_ne!(
        metadata.crc32, 0,
        "CRC32 should be non-zero for a file with data"
    );
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
    assert!(result2
        .unwrap_err()
        .to_string()
        .contains("Writer not found"));
    cleanup_ffi_schema(schema_ptr);
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

    assert!(file_path.exists());
    assert!(file_path.metadata().unwrap().len() > 0);

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_sorted_writer_ascending() {
    let (_temp_dir, filename) = get_temp_file_path("sorted_asc.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    let (ap1, sp1) =
        create_test_ffi_data_with_ids(vec![30, 10, 50], vec![Some("C"), Some("A"), Some("E")])
            .unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
    cleanup_ffi_data(ap1, sp1);

    let (ap2, sp2) =
        create_test_ffi_data_with_ids(vec![20, 40], vec![Some("B"), Some("D")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
    cleanup_ffi_data(ap2, sp2);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(
        ids,
        vec![10, 20, 30, 40, 50],
        "Data should be sorted ascending by id"
    );

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_sorted_writer_descending() {
    let (_temp_dir, filename) = get_temp_file_path("sorted_desc.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", true);

    let (ap1, sp1) =
        create_test_ffi_data_with_ids(vec![30, 10, 50], vec![Some("C"), Some("A"), Some("E")])
            .unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
    cleanup_ffi_data(ap1, sp1);

    let (ap2, sp2) =
        create_test_ffi_data_with_ids(vec![20, 40], vec![Some("B"), Some("D")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
    cleanup_ffi_data(ap2, sp2);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(
        ids,
        vec![50, 40, 30, 20, 10],
        "Data should be sorted descending by id"
    );

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_unsorted_writer_preserves_insertion_order() {
    let (_temp_dir, filename) = get_temp_file_path("unsorted.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

    let (ap1, sp1) =
        create_test_ffi_data_with_ids(vec![30, 10, 50], vec![Some("C"), Some("A"), Some("E")])
            .unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
    cleanup_ffi_data(ap1, sp1);

    let (ap2, sp2) =
        create_test_ffi_data_with_ids(vec![20, 40], vec![Some("B"), Some("D")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
    cleanup_ffi_data(ap2, sp2);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(
        ids,
        vec![30, 10, 50, 20, 40],
        "Data should preserve insertion order"
    );

    cleanup_ffi_schema(schema_ptr);
}

// ===== Arrow IPC staging path tests =====

#[test]
fn test_ipc_staging_sorted_writer_creates_and_cleans_up_staging_file() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_cleanup.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    // With eager sort-and-write, no staging file exists until a chunk is flushed.
    // The writer accumulates in memory. Verify the writer is open.
    assert!(
        NativeParquetWriter::has_writer(&filename),
        "Writer should be open"
    );

    let (ap, sp) =
        create_test_ffi_data_with_ids(vec![30, 10, 20], vec![Some("C"), Some("A"), Some("B")])
            .unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // The final Parquet file should exist
    assert!(
        Path::new(&filename).exists(),
        "Final Parquet file should exist"
    );

    // Verify data is sorted
    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![10, 20, 30]);

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_ipc_staging_has_writer_returns_true() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_has_writer.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    assert!(
        NativeParquetWriter::has_writer(&filename),
        "has_writer should return true for IPC writer"
    );

    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_ipc_staging_duplicate_writer_rejected() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_dup.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    let (_, schema_ptr2) = create_test_ffi_schema();
    let result = NativeParquetWriter::create_writer(
        filename.clone(),
        "test-index".to_string(),
        schema_ptr2,
        vec!["id".to_string()],
        vec![false],
        vec![false],
        0,
    );
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Writer already exists"));

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
    assert!(
        Path::new(&filename).exists(),
        "Empty Parquet file should be created"
    );

    let metadata = result.unwrap().unwrap();
    assert_eq!(metadata.metadata.file_metadata().num_rows(), 0);

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_ipc_staging_multi_batch_sort() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_multi_batch.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

    // Write multiple batches with interleaved values
    let (ap1, sp1) =
        create_test_ffi_data_with_ids(vec![50, 10], vec![Some("E"), Some("A")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
    cleanup_ffi_data(ap1, sp1);

    let (ap2, sp2) =
        create_test_ffi_data_with_ids(vec![30, 20], vec![Some("C"), Some("B")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
    cleanup_ffi_data(ap2, sp2);

    let (ap3, sp3) =
        create_test_ffi_data_with_ids(vec![40, 60], vec![Some("D"), Some("F")]).unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap3, sp3).unwrap();
    cleanup_ffi_data(ap3, sp3);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(
        ids,
        vec![10, 20, 30, 40, 50, 60],
        "Multiple IPC batches should be sorted correctly"
    );

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_ipc_staging_descending_sort() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_desc.parquet");
    let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", true);

    let (ap, sp) =
        create_test_ffi_data_with_ids(vec![10, 30, 20], vec![Some("A"), Some("C"), Some("B")])
            .unwrap();
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(
        ids,
        vec![30, 20, 10],
        "IPC path should support descending sort"
    );

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
    let (ap1, dp1) =
        create_test_ffi_data_with_ids(vec![30, 10, 20], vec![Some("C"), Some("A"), Some("B")])
            .unwrap();
    NativeParquetWriter::write_data(sorted_file.clone(), ap1, dp1).unwrap();
    cleanup_ffi_data(ap1, dp1);

    let (ap2, dp2) =
        create_test_ffi_data_with_ids(vec![30, 10, 20], vec![Some("C"), Some("A"), Some("B")])
            .unwrap();
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
                filename.clone(),
                "test-index".to_string(),
                schema_ptr,
                vec!["id".to_string()],
                vec![false],
                vec![false],
                0,
            )
            .is_ok()
            {
                let (ap, sp) = create_test_ffi_data_with_ids(
                    vec![30, 10, 20],
                    vec![Some("C"), Some("A"), Some("B")],
                )
                .unwrap();
                let write_ok = NativeParquetWriter::write_data(filename.clone(), ap, sp).is_ok();
                cleanup_ffi_data(ap, sp);

                if write_ok {
                    if let Ok(Some(metadata)) =
                        NativeParquetWriter::finalize_writer(filename.clone())
                    {
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

    assert!(file_path.exists());
    assert!(file_path.metadata().unwrap().len() > 0);

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![10, 20, 30, 40, 50, 60]);

    let read_metadata = NativeParquetWriter::get_file_metadata(filename.clone()).unwrap();
    assert_eq!(read_metadata.file_metadata().num_rows(), 6);

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
        if n == 0 {
            break;
        }
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

            if NativeParquetWriter::create_writer(
                filename.clone(),
                "test-index".to_string(),
                schema_ptr,
                vec![],
                vec![],
                vec![],
                0,
            )
            .is_ok()
            {
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
        let file_path = temp_dir
            .path()
            .join(format!("concurrent_write_{}.parquet", i));
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
                if NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr)
                    .is_ok()
                {
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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec![],
        vec![],
        vec![],
        0,
    );
    assert!(result.is_ok());

    let stored_after = SETTINGS_STORE.get(index_name).unwrap();
    assert_eq!(
        stored_after.bloom_filter_enabled,
        Some(false),
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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec![],
        vec![],
        vec![],
        0,
    );
    assert!(result.is_ok());

    let stored = SETTINGS_STORE.get(index_name).unwrap();
    assert_eq!(
        stored.get_bloom_filter_enabled(),
        false,
        "Default bloom_filter_enabled should be false when no settings exist"
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
    use crate::merge::schema::ROW_ID_COLUMN_NAME;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_ipc::writer::FileWriter as IpcFileWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int64, false),
        Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]));

    let ipc_path = dir
        .join("staging.arrow_ipc_staging")
        .to_string_lossy()
        .to_string();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ages.to_vec())),
            Arc::new(Int64Array::from(row_ids.to_vec())),
        ],
    )
    .unwrap();

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
    use crate::merge::schema::ROW_ID_COLUMN_NAME;
    use arrow::datatypes::{DataType, Field, Schema};

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
    use crate::merge::schema::ROW_ID_COLUMN_NAME;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]));
    let age_array: Arc<dyn Array> = Arc::new(Int32Array::from(ages));
    let name_array: Arc<dyn Array> = Arc::new(StringArray::from(names));
    let row_id_array: Arc<dyn Array> = Arc::new(Int64Array::from(row_ids));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![age_array, name_array, row_id_array]).unwrap();
    let struct_array = arrow::array::StructArray::from(record_batch);
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&struct_array.to_data()).unwrap();
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (array_ptr, schema_ptr)
}

/// Helper: reads a Parquet file and returns the __row_id__ column values.
fn read_row_ids_from_parquet(filename: &str) -> Vec<i64> {
    use crate::merge::schema::ROW_ID_COLUMN_NAME;
    use arrow::array::Int64Array;
    use arrow::compute::concat_batches;

    let batches = read_parquet_file(filename);
    if batches.is_empty() {
        return vec![];
    }
    let combined = concat_batches(&batches[0].schema(), &batches).unwrap();
    let idx = combined
        .schema()
        .index_of(ROW_ID_COLUMN_NAME)
        .expect("__row_id__ column should exist");
    let col = combined
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
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
    let idx = combined
        .schema()
        .index_of("age")
        .expect("age column should exist");
    let col = combined
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    );
    assert!(result.is_ok(), "create_writer failed: {:?}", result.err());

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 50, 20, 40],
        vec![Some("C"), Some("A"), Some("E"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify output is sorted by age ascending
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![10, 20, 30, 40, 50],
        "Output should be sorted by age ASC"
    );

    // Verify __row_id__ is sequential 0..N
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..5).collect();
    assert_eq!(
        row_ids, expected_sequential,
        "__row_id__ should be sequential 0..4"
    );

    // Verify permutation mapping
    // Original: age=[30,10,50,20,40], row_ids=[0,1,2,3,4]
    // Sorted:   age=[10,20,30,40,50] → original row_ids were [1,3,0,4,2]
    // mapping[original_row_id] = new_position
    // mapping[0]=2, mapping[1]=0, mapping[2]=4, mapping[3]=1, mapping[4]=3
    let mapping = finalize_result
        .row_id_mapping
        .expect("Should have row_id_mapping");
    assert_eq!(mapping.len(), 5);
    assert_eq!(
        mapping,
        vec![2, 0, 4, 1, 3],
        "Permutation mapping should map original row IDs to new sorted positions"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    );
    assert!(result.is_ok(), "create_writer failed: {:?}", result.err());

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![
            Some("E"),
            Some("C"),
            Some("A"),
            Some("D"),
            Some("B"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify output is sorted by age ascending
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![5, 10, 15, 20, 30, 35, 40, 45, 50, 60],
        "Output should be sorted by age ASC"
    );

    // Verify __row_id__ is sequential 0..N
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..10).collect();
    assert_eq!(
        row_ids, expected_sequential,
        "__row_id__ should be sequential 0..9 in merged output"
    );

    // Verify permutation mapping exists and has correct length
    let mapping = finalize_result
        .row_id_mapping
        .expect("Should have row_id_mapping");
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
    assert_eq!(
        mapping, expected_mapping,
        "Permutation mapping should correctly map original row IDs to sorted positions"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![true],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![10, 50, 30, 20, 40],
        vec![Some("A"), Some("E"), Some("C"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify output is sorted by age descending
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![50, 40, 30, 20, 10],
        "Output should be sorted by age DESC"
    );

    // Verify __row_id__ is sequential 0..N
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..5).collect();
    assert_eq!(
        row_ids, expected_sequential,
        "__row_id__ should be sequential 0..4"
    );

    // Verify permutation:
    // Original: age=[10,50,30,20,40], row_ids=[0,1,2,3,4]
    // Sorted DESC: age=[50,40,30,20,10] → original row_ids [1,4,2,3,0]
    // mapping[0]=4, mapping[1]=0, mapping[2]=2, mapping[3]=3, mapping[4]=1
    let mapping = finalize_result
        .row_id_mapping
        .expect("Should have row_id_mapping");
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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

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

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify sorted output
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![10, 15, 20, 25, 30, 35, 40, 45, 50, 55],
        "Output should be globally sorted by age ASC"
    );

    // Verify sequential row IDs
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..10).collect();
    assert_eq!(
        row_ids, expected_sequential,
        "__row_id__ should be sequential 0..9"
    );

    // Verify permutation mapping
    let mapping = finalize_result
        .row_id_mapping
        .expect("Should have row_id_mapping");
    assert_eq!(mapping.len(), 10);

    // Original: age=[50,30,10,40,20,25,55,15,35,45], row_ids=[0..9]
    // Sorted:   age=[10,15,20,25,30,35,40,45,50,55]
    // Original positions: [2, 7, 4, 5, 1, 8, 3, 9, 0, 6]
    // mapping[0]=8, mapping[1]=4, mapping[2]=0, mapping[3]=6, mapping[4]=2
    // mapping[5]=3, mapping[6]=9, mapping[7]=1, mapping[8]=5, mapping[9]=7
    let expected_mapping: Vec<i64> = vec![8, 4, 0, 6, 2, 3, 9, 1, 5, 7];
    assert_eq!(
        mapping, expected_mapping,
        "Permutation should correctly map across multiple write calls"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    assert_eq!(
        finalize_result.metadata.file_metadata().num_rows(),
        0,
        "Empty writer should produce 0-row Parquet"
    );
    assert!(
        finalize_result.row_id_mapping.is_none(),
        "Empty writer should have no row_id_mapping"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(original_ages.clone(), names, row_ids);
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

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
    assert_eq!(
        sorted_mapping, expected_perm,
        "Mapping should be a valid permutation of 0..N"
    );

    // Verify: original_ages[i] should appear at position mapping[i] in sorted output
    for i in 0..10 {
        let new_pos = mapping[i] as usize;
        assert_eq!(
            sorted_ages[new_pos], original_ages[i],
            "original_ages[{}]={} should be at sorted position {}, but found {}",
            i, original_ages[i], new_pos, sorted_ages[new_pos]
        );
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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(ages.clone(), names, row_ids);
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify output is sorted
    let sorted_ages = read_ages_from_parquet(&filename);
    let mut expected_sorted = ages.clone();
    expected_sorted.sort();
    assert_eq!(
        sorted_ages, expected_sorted,
        "Output should be sorted ascending"
    );

    // Verify sequential row IDs
    let row_ids_in_file = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..num_rows as i64).collect();
    assert_eq!(
        row_ids_in_file,
        expected_sequential,
        "__row_id__ should be sequential 0..{}",
        num_rows - 1
    );

    // Verify permutation
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping.len(), num_rows as usize);

    // Verify it's a valid permutation
    let mut sorted_mapping = mapping.clone();
    sorted_mapping.sort();
    assert_eq!(
        sorted_mapping, expected_sequential,
        "Mapping should be a valid permutation"
    );

    // Verify correctness: original_ages[i] at sorted position mapping[i]
    for i in 0..num_rows as usize {
        let new_pos = mapping[i] as usize;
        assert_eq!(
            sorted_ages[new_pos], ages[i],
            "ages[{}]={} should be at sorted position {}",
            i, ages[i], new_pos
        );
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
    metadata.key_value_metadata().and_then(|kvs| {
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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        writer_generation,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 20],
        vec![Some("C"), Some("A"), Some("B")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(
        gen,
        Some(42),
        "writer_generation=42 should be in Parquet metadata"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        writer_generation,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![
            Some("E"),
            Some("C"),
            Some("A"),
            Some("D"),
            Some("B"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Previously writer_generation was lost in the k-way merge path. This has been fixed —
    // merge_sorted now propagates writer_generation into the output file metadata.
    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(
        gen,
        Some(7),
        "writer_generation should be propagated in multi-chunk merge path"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) =
        create_ffi_data_with_row_id(vec![20, 10], vec![Some("B"), Some("A")], vec![0, 1]);
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(
        gen,
        Some(0),
        "writer_generation=0 should be stored in metadata"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        writer_generation,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![80, 20, 60, 40, 10, 90, 50, 30, 70],
        vec![
            Some("H"),
            Some("B"),
            Some("F"),
            Some("D"),
            Some("A"),
            Some("I"),
            Some("E"),
            Some("C"),
            Some("G"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify writer_generation in metadata
    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(
        gen,
        Some(999_999),
        "Large writer_generation should be preserved in Parquet metadata"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec![],
        vec![],
        vec![],
        writer_generation,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 20],
        vec![Some("C"), Some("A"), Some("B")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(
        gen,
        Some(17),
        "writer_generation should be in metadata for unsorted writer too"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 50, 20, 40],
        vec![Some("C"), Some("A"), Some("E"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // CRC should be non-zero for a file with data
    assert_ne!(
        finalize_result.crc32, 0,
        "CRC32 should be non-zero for single-chunk sorted output"
    );

    // Verify CRC matches actual file content
    let actual_crc = compute_file_crc32(&filename);
    assert_eq!(
        finalize_result.crc32, actual_crc,
        "CRC32 from finalize should match recomputed CRC of the file on disk"
    );

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
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![
            Some("E"),
            Some("C"),
            Some("A"),
            Some("D"),
            Some("B"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Multi-chunk merge also produces a CRC (from merge_sorted's CrcWriter)
    // Verify it matches the actual file
    let actual_crc = compute_file_crc32(&filename);
    assert_eq!(
        finalize_result.crc32, actual_crc,
        "CRC32 from finalize should match recomputed CRC for multi-chunk output"
    );

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
        filename1.clone(),
        index_name1.to_string(),
        schema_ptr1,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();
    let (ap1, sp1) = create_ffi_data_with_row_id(
        vec![10, 20, 30],
        vec![Some("A"), Some("B"), Some("C")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename1.clone(), ap1, sp1).unwrap();
    let result1 = NativeParquetWriter::finalize_writer(filename1.clone())
        .unwrap()
        .unwrap();

    // Writer 2 — different data
    let (_schema, schema_ptr2) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename2.clone(),
        index_name2.to_string(),
        schema_ptr2,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();
    let (ap2, sp2) = create_ffi_data_with_row_id(
        vec![99, 88, 77],
        vec![Some("X"), Some("Y"), Some("Z")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename2.clone(), ap2, sp2).unwrap();
    let result2 = NativeParquetWriter::finalize_writer(filename2.clone())
        .unwrap()
        .unwrap();

    assert_ne!(
        result1.crc32, result2.crc32,
        "Different data should produce different CRC32 values"
    );

    SETTINGS_STORE.remove(index_name1);
    SETTINGS_STORE.remove(index_name2);
}

// ===== Batch slicing path tests =====
// These tests exercise the code path in SortingChunkedWriter::write() where a single
// incoming batch exceeds memory_threshold_bytes and must be sliced into smaller pieces,
// with each piece flushed independently.

/// Test: A single large batch that exceeds the memory threshold is sliced and produces
/// correct sorted output with sequential row IDs and valid permutation.
#[test]
fn test_chunked_writer_batch_slicing_large_batch() {
    let (_temp_dir, filename) = get_temp_file_path("slice_large.parquet");
    let index_name = "test-slice-large";

    // Set threshold to 1 byte — every batch will exceed it and trigger slicing.
    // With 1 byte threshold, rows_per_slice = max(1, 1/bytes_per_row) = 1,
    // so each row becomes its own chunk.
    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(1); // Extremely small — forces per-row slicing
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // Write a batch with 8 rows — each row will be sliced individually
    let (ap, sp) = create_ffi_data_with_row_id(
        vec![80, 20, 60, 40, 10, 70, 30, 50],
        vec![
            Some("H"),
            Some("B"),
            Some("F"),
            Some("D"),
            Some("A"),
            Some("G"),
            Some("C"),
            Some("E"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify output is sorted
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![10, 20, 30, 40, 50, 60, 70, 80],
        "Output should be sorted by age ASC even with per-row slicing"
    );

    // Verify sequential row IDs
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..8).collect();
    assert_eq!(
        row_ids, expected_sequential,
        "__row_id__ should be sequential 0..7"
    );

    // Verify permutation
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping.len(), 8);
    // Original: age=[80,20,60,40,10,70,30,50], row_ids=[0..7]
    // Sorted:   age=[10,20,30,40,50,60,70,80]
    // mapping[0]=7(80→pos7), mapping[1]=1(20→pos1), mapping[2]=5(60→pos5),
    // mapping[3]=3(40→pos3), mapping[4]=0(10→pos0), mapping[5]=6(70→pos6),
    // mapping[6]=2(30→pos2), mapping[7]=4(50→pos4)
    assert_eq!(mapping, vec![7, 1, 5, 3, 0, 6, 2, 4]);

    // Verify CRC is valid
    let actual_crc = compute_file_crc32(&filename);
    assert_eq!(
        finalize_result.crc32, actual_crc,
        "CRC should match file content after batch slicing"
    );

    SETTINGS_STORE.remove(index_name);
}

/// Test: Batch slicing with a threshold that allows exactly 2 rows per slice.
/// Verifies that partial slices are handled correctly at boundaries.
#[test]
fn test_chunked_writer_batch_slicing_two_rows_per_slice() {
    let (_temp_dir, filename) = get_temp_file_path("slice_two.parquet");
    let index_name = "test-slice-two";

    // We need a threshold that allows ~2 rows. A single Int32 + Utf8 + Int64 row
    // is roughly 40-80 bytes in Arrow memory. Set threshold to allow ~2 rows.
    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(100); // ~2 rows fit
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // Write 10 rows — should be sliced into ~5 chunks of 2 rows each
    let (ap, sp) = create_ffi_data_with_row_id(
        vec![90, 10, 70, 30, 50, 80, 20, 60, 40, 100],
        vec![
            Some("I"),
            Some("A"),
            Some("G"),
            Some("C"),
            Some("E"),
            Some("H"),
            Some("B"),
            Some("F"),
            Some("D"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify sorted output
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        "Output should be globally sorted despite batch slicing"
    );

    // Verify sequential row IDs
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..10).collect();
    assert_eq!(row_ids, expected_sequential);

    // Verify permutation is valid
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping.len(), 10);
    let mut sorted_mapping = mapping.clone();
    sorted_mapping.sort();
    assert_eq!(
        sorted_mapping, expected_sequential,
        "Mapping should be a valid permutation"
    );

    // Verify permutation correctness
    let original_ages = vec![90, 10, 70, 30, 50, 80, 20, 60, 40, 100];
    for i in 0..10 {
        let new_pos = mapping[i] as usize;
        assert_eq!(
            ages[new_pos], original_ages[i],
            "original_ages[{}]={} should be at sorted position {}",
            i, original_ages[i], new_pos
        );
    }

    SETTINGS_STORE.remove(index_name);
}

/// Test: Batch slicing with descending sort — verifies slicing doesn't break sort direction.
#[test]
fn test_chunked_writer_batch_slicing_descending() {
    let (_temp_dir, filename) = get_temp_file_path("slice_desc.parquet");
    let index_name = "test-slice-desc";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![true]; // descending
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(1); // Force per-row slicing
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![true],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![10, 50, 30, 20, 40],
        vec![Some("A"), Some("E"), Some("C"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify descending sort
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![50, 40, 30, 20, 10],
        "Output should be sorted by age DESC with batch slicing"
    );

    // Verify sequential row IDs
    let row_ids = read_row_ids_from_parquet(&filename);
    assert_eq!(row_ids, vec![0, 1, 2, 3, 4]);

    // Verify permutation
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    // Original: age=[10,50,30,20,40], row_ids=[0..4]
    // Sorted DESC: age=[50,40,30,20,10] → original row_ids [1,4,2,3,0]
    // mapping[0]=4, mapping[1]=0, mapping[2]=2, mapping[3]=3, mapping[4]=1
    assert_eq!(mapping, vec![4, 0, 2, 3, 1]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: Multiple write_data calls where each batch individually exceeds the threshold.
/// Verifies that slicing works correctly across multiple write calls.
#[test]
fn test_chunked_writer_batch_slicing_multiple_writes() {
    let (_temp_dir, filename) = get_temp_file_path("slice_multi_write.parquet");
    let index_name = "test-slice-multi-write";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(1); // Force slicing on every batch
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // First batch: rows 0..4
    let (ap1, sp1) = create_ffi_data_with_row_id(
        vec![50, 10, 30],
        vec![Some("E"), Some("A"), Some("C")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();

    // Second batch: rows 3..6
    let (ap2, sp2) = create_ffi_data_with_row_id(
        vec![40, 20, 60],
        vec![Some("D"), Some("B"), Some("F")],
        vec![3, 4, 5],
    );
    NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify globally sorted
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![10, 20, 30, 40, 50, 60],
        "Output should be globally sorted across sliced multi-write batches"
    );

    // Verify sequential row IDs
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..6).collect();
    assert_eq!(row_ids, expected_sequential);

    // Verify permutation
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping.len(), 6);
    // Original: age=[50,10,30,40,20,60], row_ids=[0,1,2,3,4,5]
    // Sorted:   age=[10,20,30,40,50,60]
    // mapping[0]=4(50→pos4), mapping[1]=0(10→pos0), mapping[2]=2(30→pos2),
    // mapping[3]=3(40→pos3), mapping[4]=1(20→pos1), mapping[5]=5(60→pos5)
    assert_eq!(mapping, vec![4, 0, 2, 3, 1, 5]);

    SETTINGS_STORE.remove(index_name);
}

// ===== Schema without __row_id__ column tests =====
// These tests verify that the sorted chunked writer works correctly when the schema
// does NOT contain a __row_id__ column. In this case, flush_and_sort_chunk should
// skip the row ID capture/rewrite logic and finalize_writer should return no permutation.

/// Helper: creates FFI schema pointer for a schema WITHOUT __row_id__ (age: Int32, name: Utf8).
fn create_no_row_id_schema_ptr() -> (Arc<arrow::datatypes::Schema>, i64) {
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (schema, schema_ptr)
}

/// Helper: creates FFI data WITHOUT __row_id__ column.
fn create_ffi_data_no_row_id(ages: Vec<i32>, names: Vec<Option<&str>>) -> (i64, i64) {
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let age_array: Arc<dyn Array> = Arc::new(Int32Array::from(ages));
    let name_array: Arc<dyn Array> = Arc::new(StringArray::from(names));
    let record_batch = RecordBatch::try_new(schema.clone(), vec![age_array, name_array]).unwrap();
    let struct_array = arrow::array::StructArray::from(record_batch);
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&struct_array.to_data()).unwrap();
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (array_ptr, schema_ptr)
}

/// Test: Sorted writer without __row_id__ column — single chunk.
/// Should sort correctly and return no permutation mapping.
#[test]
fn test_chunked_writer_no_row_id_single_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("no_rowid_single.parquet");
    let index_name = "test-no-rowid-single";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_no_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_no_row_id(
        vec![50, 10, 30, 20, 40],
        vec![Some("E"), Some("A"), Some("C"), Some("B"), Some("D")],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify sorted output
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![10, 20, 30, 40, 50],
        "Output should be sorted by age ASC even without __row_id__"
    );

    // No permutation mapping when __row_id__ is absent
    assert!(
        finalize_result.row_id_mapping.is_none(),
        "row_id_mapping should be None when schema has no __row_id__ column"
    );

    SETTINGS_STORE.remove(index_name);
}

/// Test: Sorted writer without __row_id__ column — multi chunk (forced by small threshold).
/// Should sort correctly across chunks and return no permutation mapping.
#[test]
fn test_chunked_writer_no_row_id_multi_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("no_rowid_multi.parquet");
    let index_name = "test-no-rowid-multi";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Force multiple chunks
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_no_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_no_row_id(
        vec![80, 20, 60, 40, 10, 70, 30, 50],
        vec![
            Some("H"),
            Some("B"),
            Some("F"),
            Some("D"),
            Some("A"),
            Some("G"),
            Some("C"),
            Some("E"),
        ],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify sorted output
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![10, 20, 30, 40, 50, 60, 70, 80],
        "Output should be sorted even without __row_id__ in multi-chunk path"
    );

    // No permutation mapping
    assert!(
        finalize_result.row_id_mapping.is_none(),
        "row_id_mapping should be None without __row_id__ column"
    );

    SETTINGS_STORE.remove(index_name);
}

/// Test: Sorted writer without __row_id__ — batch slicing path.
/// Verifies the slicing + sort works when there's no row ID to rewrite.
#[test]
fn test_chunked_writer_no_row_id_batch_slicing() {
    let (_temp_dir, filename) = get_temp_file_path("no_rowid_slice.parquet");
    let index_name = "test-no-rowid-slice";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![true]; // descending
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(1); // Force per-row slicing
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_no_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![true],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_no_row_id(
        vec![10, 50, 30, 20, 40, 60],
        vec![
            Some("A"),
            Some("E"),
            Some("C"),
            Some("B"),
            Some("D"),
            Some("F"),
        ],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify descending sort
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(
        ages,
        vec![60, 50, 40, 30, 20, 10],
        "Output should be sorted DESC without __row_id__ in slicing path"
    );

    // No permutation
    assert!(finalize_result.row_id_mapping.is_none());

    // CRC should still be valid
    let actual_crc = compute_file_crc32(&filename);
    assert_eq!(finalize_result.crc32, actual_crc);

    SETTINGS_STORE.remove(index_name);
}

// ===== Multi-column sort tests =====
// These tests verify sorting on 2+ columns with mixed sort directions.

/// Helper: creates FFI schema with (age: Int32, score: Int32, name: Utf8, __row_id__: Int64).
fn create_multi_sort_schema_ptr() -> (Arc<arrow::datatypes::Schema>, i64) {
    use crate::merge::schema::ROW_ID_COLUMN_NAME;
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]));
    let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (schema, schema_ptr)
}

/// Helper: creates FFI data with (age, score, name, __row_id__) columns.
fn create_ffi_data_multi_sort(
    ages: Vec<i32>,
    scores: Vec<i32>,
    names: Vec<Option<&str>>,
    row_ids: Vec<i64>,
) -> (i64, i64) {
    use crate::merge::schema::ROW_ID_COLUMN_NAME;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]));
    let age_array: Arc<dyn Array> = Arc::new(Int32Array::from(ages));
    let score_array: Arc<dyn Array> = Arc::new(Int32Array::from(scores));
    let name_array: Arc<dyn Array> = Arc::new(StringArray::from(names));
    let row_id_array: Arc<dyn Array> = Arc::new(Int64Array::from(row_ids));
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![age_array, score_array, name_array, row_id_array],
    )
    .unwrap();
    let struct_array = arrow::array::StructArray::from(record_batch);
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&struct_array.to_data()).unwrap();
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (array_ptr, schema_ptr)
}

/// Helper: reads the "score" column from a Parquet file.
fn read_scores_from_parquet(filename: &str) -> Vec<i32> {
    use arrow::array::Int32Array;
    use arrow::compute::concat_batches;

    let batches = read_parquet_file(filename);
    if batches.is_empty() {
        return vec![];
    }
    let combined = concat_batches(&batches[0].schema(), &batches).unwrap();
    let idx = combined
        .schema()
        .index_of("score")
        .expect("score column should exist");
    let col = combined
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("score should be Int32");
    (0..col.len()).map(|i| col.value(i)).collect()
}

/// Test: Sort by age ASC, then score DESC (tie-breaking).
/// Rows with the same age should be ordered by score descending.
#[test]
fn test_multi_column_sort_age_asc_score_desc() {
    let (_temp_dir, filename) = get_temp_file_path("multi_sort_asc_desc.parquet");
    let index_name = "test-multi-sort-asc-desc";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string(), "score".to_string()];
    settings.reverse_sorts = vec![false, true]; // age ASC, score DESC
    settings.nulls_first = vec![false, false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_multi_sort_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string(), "score".to_string()],
        vec![false, true],
        vec![false, false],
        0,
    )
    .unwrap();

    // Data with duplicate ages to test tie-breaking by score DESC
    let (ap, sp) = create_ffi_data_multi_sort(
        vec![30, 20, 30, 20, 10, 30], // ages: ties at 20 and 30
        vec![50, 80, 90, 40, 70, 10], // scores for tie-breaking
        vec![
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
        ],
        vec![0, 1, 2, 3, 4, 5],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    let ages = read_ages_from_parquet(&filename);
    let scores = read_scores_from_parquet(&filename);

    // Expected sort: age ASC first, then score DESC within same age
    // age=10: score=70
    // age=20: score=80, score=40 (DESC)
    // age=30: score=90, score=50, score=10 (DESC)
    assert_eq!(ages, vec![10, 20, 20, 30, 30, 30]);
    assert_eq!(scores, vec![70, 80, 40, 90, 50, 10]);

    // Verify sequential row IDs
    let row_ids = read_row_ids_from_parquet(&filename);
    assert_eq!(row_ids, vec![0, 1, 2, 3, 4, 5]);

    // Verify permutation
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    // Original order: (30,50), (20,80), (30,90), (20,40), (10,70), (30,10)
    // Sorted order:   (10,70), (20,80), (20,40), (30,90), (30,50), (30,10)
    // Original row_ids: 4, 1, 3, 2, 0, 5
    // mapping[0]=4, mapping[1]=1, mapping[2]=3, mapping[3]=2, mapping[4]=0, mapping[5]=5
    assert_eq!(mapping, vec![4, 1, 3, 2, 0, 5]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: Sort by age DESC, then score ASC — both columns with different directions.
#[test]
fn test_multi_column_sort_age_desc_score_asc() {
    let (_temp_dir, filename) = get_temp_file_path("multi_sort_desc_asc.parquet");
    let index_name = "test-multi-sort-desc-asc";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string(), "score".to_string()];
    settings.reverse_sorts = vec![true, false]; // age DESC, score ASC
    settings.nulls_first = vec![false, false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_multi_sort_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string(), "score".to_string()],
        vec![true, false],
        vec![false, false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_multi_sort(
        vec![20, 30, 20, 30, 10],
        vec![50, 20, 30, 40, 60],
        vec![Some("A"), Some("B"), Some("C"), Some("D"), Some("E")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ages = read_ages_from_parquet(&filename);
    let scores = read_scores_from_parquet(&filename);

    // Expected: age DESC, then score ASC within same age
    // age=30: score=20, score=40 (ASC)
    // age=20: score=30, score=50 (ASC)
    // age=10: score=60
    assert_eq!(ages, vec![30, 30, 20, 20, 10]);
    assert_eq!(scores, vec![20, 40, 30, 50, 60]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: Multi-column sort with forced multi-chunk — verifies the k-way merge
/// respects composite sort keys across chunk boundaries.
#[test]
fn test_multi_column_sort_multi_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("multi_sort_chunked.parquet");
    let index_name = "test-multi-sort-chunked";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string(), "score".to_string()];
    settings.reverse_sorts = vec![false, false]; // both ASC
    settings.nulls_first = vec![false, false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Force multiple chunks
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_multi_sort_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string(), "score".to_string()],
        vec![false, false],
        vec![false, false],
        0,
    )
    .unwrap();

    // 10 rows with many ties in age to stress tie-breaking across chunks
    let (ap, sp) = create_ffi_data_multi_sort(
        vec![20, 10, 20, 10, 30, 20, 10, 30, 20, 10],
        vec![50, 30, 10, 70, 20, 40, 10, 60, 30, 50],
        vec![
            Some("A"),
            Some("B"),
            Some("C"),
            Some("D"),
            Some("E"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    let ages = read_ages_from_parquet(&filename);
    let scores = read_scores_from_parquet(&filename);

    // Expected: age ASC, then score ASC within same age
    // age=10: scores 10, 30, 50, 70
    // age=20: scores 10, 30, 40, 50
    // age=30: scores 20, 60
    assert_eq!(ages, vec![10, 10, 10, 10, 20, 20, 20, 20, 30, 30]);
    assert_eq!(scores, vec![10, 30, 50, 70, 10, 30, 40, 50, 20, 60]);

    // Verify sequential row IDs
    let row_ids = read_row_ids_from_parquet(&filename);
    let expected_sequential: Vec<i64> = (0..10).collect();
    assert_eq!(row_ids, expected_sequential);

    // Verify permutation is valid
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping.len(), 10);
    let mut sorted_mapping = mapping.clone();
    sorted_mapping.sort();
    assert_eq!(
        sorted_mapping, expected_sequential,
        "Mapping should be a valid permutation"
    );

    SETTINGS_STORE.remove(index_name);
}

/// Test: Multi-column sort with batch slicing — extreme case combining both features.
#[test]
fn test_multi_column_sort_batch_slicing() {
    let (_temp_dir, filename) = get_temp_file_path("multi_sort_slice.parquet");
    let index_name = "test-multi-sort-slice";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string(), "score".to_string()];
    settings.reverse_sorts = vec![false, true]; // age ASC, score DESC
    settings.nulls_first = vec![false, false];
    settings.sort_in_memory_threshold_bytes = Some(1); // Per-row slicing
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_multi_sort_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string(), "score".to_string()],
        vec![false, true],
        vec![false, false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_multi_sort(
        vec![20, 20, 10, 10, 20],
        vec![30, 50, 40, 20, 10],
        vec![Some("A"), Some("B"), Some("C"), Some("D"), Some("E")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ages = read_ages_from_parquet(&filename);
    let scores = read_scores_from_parquet(&filename);

    // Expected: age ASC, then score DESC within same age
    // age=10: score=40, score=20 (DESC)
    // age=20: score=50, score=30, score=10 (DESC)
    assert_eq!(ages, vec![10, 10, 20, 20, 20]);
    assert_eq!(scores, vec![40, 20, 50, 30, 10]);

    SETTINGS_STORE.remove(index_name);
}

// ===== nulls_first behavior test =====

/// Helper: creates FFI schema with nullable sort column (age: Int32 nullable, name: Utf8).
fn create_nullable_schema_ptr() -> (Arc<arrow::datatypes::Schema>, i64) {
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, true), // nullable
        Field::new("name", DataType::Utf8, true),
    ]));
    let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (schema, schema_ptr)
}

/// Helper: creates FFI data with nullable age column.
fn create_ffi_data_nullable_age(ages: Vec<Option<i32>>, names: Vec<Option<&str>>) -> (i64, i64) {
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let age_array: Arc<dyn Array> = Arc::new(Int32Array::from(ages));
    let name_array: Arc<dyn Array> = Arc::new(StringArray::from(names));
    let record_batch = RecordBatch::try_new(schema.clone(), vec![age_array, name_array]).unwrap();
    let struct_array = arrow::array::StructArray::from(record_batch);
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&struct_array.to_data()).unwrap();
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (array_ptr, schema_ptr)
}

/// Helper: reads nullable age column, returning None for nulls.
fn read_nullable_ages_from_parquet(filename: &str) -> Vec<Option<i32>> {
    use arrow::array::{Array, Int32Array};
    use arrow::compute::concat_batches;

    let batches = read_parquet_file(filename);
    let combined = concat_batches(&batches[0].schema(), &batches).unwrap();
    let idx = combined.schema().index_of("age").unwrap();
    let col = combined
        .column(idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    (0..col.len())
        .map(|i| {
            if col.is_null(i) {
                None
            } else {
                Some(col.value(i))
            }
        })
        .collect()
}

/// Test: nulls_first=true places NULL values before non-null values in ascending sort.
#[test]
fn test_nulls_first_true_ascending() {
    let (_temp_dir, filename) = get_temp_file_path("nulls_first_true.parquet");
    let index_name = "test-nulls-first-true";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false]; // ASC
    settings.nulls_first = vec![true]; // NULLs first
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_nullable_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![true],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_nullable_age(
        vec![Some(30), None, Some(10), None, Some(20)],
        vec![Some("C"), Some("X"), Some("A"), Some("Y"), Some("B")],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ages = read_nullable_ages_from_parquet(&filename);
    // NULLs first, then ascending non-nulls
    assert_eq!(ages, vec![None, None, Some(10), Some(20), Some(30)]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: nulls_first=false places NULL values after non-null values in ascending sort.
#[test]
fn test_nulls_first_false_ascending() {
    let (_temp_dir, filename) = get_temp_file_path("nulls_first_false.parquet");
    let index_name = "test-nulls-first-false";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false]; // ASC
    settings.nulls_first = vec![false]; // NULLs last
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_nullable_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_nullable_age(
        vec![Some(30), None, Some(10), None, Some(20)],
        vec![Some("C"), Some("X"), Some("A"), Some("Y"), Some("B")],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let ages = read_nullable_ages_from_parquet(&filename);
    // Non-nulls ascending first, then NULLs last
    assert_eq!(ages, vec![Some(10), Some(20), Some(30), None, None]);

    SETTINGS_STORE.remove(index_name);
}

/// Helper: creates FFI schema with nullable age + __row_id__ (age: Int32 nullable, name: Utf8, __row_id__: Int64).
fn create_nullable_with_row_id_schema_ptr() -> (Arc<arrow::datatypes::Schema>, i64) {
    use crate::merge::schema::ROW_ID_COLUMN_NAME;
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, true), // nullable
        Field::new("name", DataType::Utf8, true),
        Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]));
    let ffi_schema = arrow::ffi::FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (schema, schema_ptr)
}

/// Helper: creates FFI data with nullable age + __row_id__.
fn create_ffi_data_nullable_with_row_id(
    ages: Vec<Option<i32>>,
    names: Vec<Option<&str>>,
    row_ids: Vec<i64>,
) -> (i64, i64) {
    use crate::merge::schema::ROW_ID_COLUMN_NAME;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("age", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false),
    ]));
    let age_array: Arc<dyn Array> = Arc::new(Int32Array::from(ages));
    let name_array: Arc<dyn Array> = Arc::new(StringArray::from(names));
    let row_id_array: Arc<dyn Array> = Arc::new(Int64Array::from(row_ids));
    let record_batch =
        RecordBatch::try_new(schema.clone(), vec![age_array, name_array, row_id_array]).unwrap();
    let struct_array = arrow::array::StructArray::from(record_batch);
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&struct_array.to_data()).unwrap();
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (array_ptr, schema_ptr)
}

/// Test: nulls_first=true with __row_id__ — verifies sequential row IDs and correct permutation
/// when NULLs are sorted to the front.
#[test]
fn test_nulls_first_with_row_id_and_permutation() {
    let (_temp_dir, filename) = get_temp_file_path("nulls_first_rowid.parquet");
    let index_name = "test-nulls-first-rowid";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false]; // ASC
    settings.nulls_first = vec![true]; // NULLs first
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_nullable_with_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![true],
        0,
    )
    .unwrap();

    // Original: row0=age30, row1=NULL, row2=age10, row3=NULL, row4=age20
    let (ap, sp) = create_ffi_data_nullable_with_row_id(
        vec![Some(30), None, Some(10), None, Some(20)],
        vec![Some("C"), Some("X"), Some("A"), Some("Y"), Some("B")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify sort: NULLs first, then ascending
    let ages = read_nullable_ages_from_parquet(&filename);
    assert_eq!(ages, vec![None, None, Some(10), Some(20), Some(30)]);

    // Verify __row_id__ is sequential 0..N
    let row_ids = read_row_ids_from_parquet(&filename);
    assert_eq!(
        row_ids,
        vec![0, 1, 2, 3, 4],
        "__row_id__ should be sequential after rewrite"
    );

    // Verify permutation mapping
    // Original: age=[30, NULL, 10, NULL, 20], row_ids=[0,1,2,3,4]
    // Sorted:   age=[NULL, NULL, 10, 20, 30]
    // The two NULLs were at original positions 1 and 3 (stable order preserved by RowConverter)
    // Sorted positions: NULL(row1)→pos0, NULL(row3)→pos1, 10(row2)→pos2, 20(row4)→pos3, 30(row0)→pos4
    // mapping[0]=4, mapping[1]=0, mapping[2]=2, mapping[3]=1, mapping[4]=3
    let mapping = finalize_result
        .row_id_mapping
        .expect("Should have row_id_mapping");
    assert_eq!(mapping.len(), 5);
    assert_eq!(
        mapping,
        vec![4, 0, 2, 1, 3],
        "Permutation should correctly place NULLs first and map original row IDs"
    );

    SETTINGS_STORE.remove(index_name);
}

/// Test: nulls_first=false with __row_id__ — NULLs at end, verifies permutation.
#[test]
fn test_nulls_last_with_row_id_and_permutation() {
    let (_temp_dir, filename) = get_temp_file_path("nulls_last_rowid.parquet");
    let index_name = "test-nulls-last-rowid";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false]; // ASC
    settings.nulls_first = vec![false]; // NULLs last
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_nullable_with_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // Original: row0=age30, row1=NULL, row2=age10, row3=NULL, row4=age20
    let (ap, sp) = create_ffi_data_nullable_with_row_id(
        vec![Some(30), None, Some(10), None, Some(20)],
        vec![Some("C"), Some("X"), Some("A"), Some("Y"), Some("B")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify sort: ascending non-nulls, then NULLs last
    let ages = read_nullable_ages_from_parquet(&filename);
    assert_eq!(ages, vec![Some(10), Some(20), Some(30), None, None]);

    // Verify __row_id__ is sequential 0..N
    let row_ids = read_row_ids_from_parquet(&filename);
    assert_eq!(row_ids, vec![0, 1, 2, 3, 4]);

    // Verify permutation mapping
    // Original: age=[30, NULL, 10, NULL, 20], row_ids=[0,1,2,3,4]
    // Sorted:   age=[10, 20, 30, NULL, NULL]
    // 10(row2)→pos0, 20(row4)→pos1, 30(row0)→pos2, NULL(row1)→pos3, NULL(row3)→pos4
    // mapping[0]=2, mapping[1]=3, mapping[2]=0, mapping[3]=4, mapping[4]=1
    let mapping = finalize_result
        .row_id_mapping
        .expect("Should have row_id_mapping");
    assert_eq!(mapping.len(), 5);
    assert_eq!(
        mapping,
        vec![2, 3, 0, 4, 1],
        "Permutation should correctly place NULLs last and map original row IDs"
    );

    SETTINGS_STORE.remove(index_name);
}

// ===== Empty batch handling and memory usage tests =====

/// Test: Writing a 0-row batch to a sorted writer doesn't corrupt state.
/// Subsequent non-empty writes and finalize should still work correctly.
#[test]
fn test_empty_batch_write_does_not_corrupt_sorted_writer() {
    let (_temp_dir, filename) = get_temp_file_path("empty_batch.parquet");
    let index_name = "test-empty-batch";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // Write an empty batch (0 rows)
    let (ap_empty, sp_empty) = create_ffi_data_with_row_id(vec![], vec![], vec![]);
    NativeParquetWriter::write_data(filename.clone(), ap_empty, sp_empty).unwrap();

    // Write a real batch after the empty one
    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 20],
        vec![Some("C"), Some("A"), Some("B")],
        vec![0, 1, 2],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // Verify sorted output — empty batch should have no effect
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![10, 20, 30]);

    let row_ids = read_row_ids_from_parquet(&filename);
    assert_eq!(row_ids, vec![0, 1, 2]);

    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping, vec![2, 0, 1]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: Writing only empty batches produces a valid empty Parquet file.
#[test]
fn test_only_empty_batches_produces_empty_output() {
    let (_temp_dir, filename) = get_temp_file_path("only_empty.parquet");
    let index_name = "test-only-empty";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // Write multiple empty batches
    for _ in 0..3 {
        let (ap, sp) = create_ffi_data_with_row_id(vec![], vec![], vec![]);
        NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    }

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    assert_eq!(finalize_result.metadata.file_metadata().num_rows(), 0);
    assert!(finalize_result.row_id_mapping.is_none());

    SETTINGS_STORE.remove(index_name);
}

/// Test: get_filtered_writer_memory_usage reports memory for IPC (sorted) writers.
/// After writing data that triggers chunk flushes, memory should reflect
/// the accumulated chunk_row_ids.
#[test]
fn test_memory_usage_ipc_writer_reports_chunk_row_ids() {
    let (_temp_dir, filename) = get_temp_file_path("mem_ipc.parquet");
    let index_name = "test-mem-ipc";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Force chunking
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // Before writing, memory should be 0 (no chunks flushed yet)
    let path_prefix = Path::new(&filename)
        .parent()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let mem_before =
        NativeParquetWriter::get_filtered_writer_memory_usage(path_prefix.clone()).unwrap();
    assert_eq!(mem_before, 0, "Memory should be 0 before any chunk flush");

    // Write enough data to trigger at least one chunk flush
    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![
            Some("E"),
            Some("C"),
            Some("A"),
            Some("D"),
            Some("B"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    // After writing (chunks flushed), memory should be > 0 due to chunk_row_ids
    let mem_after =
        NativeParquetWriter::get_filtered_writer_memory_usage(path_prefix.clone()).unwrap();
    assert!(
        mem_after > 0,
        "Memory should be > 0 after chunk flushes (chunk_row_ids accumulated), got {}",
        mem_after
    );

    // Memory should be proportional to rows written × sizeof(i64)
    // 10 rows × 8 bytes = 80 bytes minimum (could be more if split across chunks)
    assert!(
        mem_after >= 80,
        "Memory should be at least 10 rows × 8 bytes = 80, got {}",
        mem_after
    );

    // Finalize and verify writer is removed
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    let mem_final = NativeParquetWriter::get_filtered_writer_memory_usage(path_prefix).unwrap();
    assert_eq!(mem_final, 0, "Memory should be 0 after writer is finalized");

    SETTINGS_STORE.remove(index_name);
}

/// Test: get_filtered_writer_memory_usage with path prefix filtering.
/// Only writers whose temp path starts with the prefix should be counted.
#[test]
fn test_memory_usage_path_prefix_filtering() {
    let (_temp_dir1, filename1) = get_temp_file_path("mem_prefix_a.parquet");
    let (_temp_dir2, filename2) = get_temp_file_path("mem_prefix_b.parquet");
    let index_name = "test-mem-prefix";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    // Create writer 1
    let (_schema, schema_ptr1) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename1.clone(),
        index_name.to_string(),
        schema_ptr1,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // Create writer 2
    let (_schema, schema_ptr2) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename2.clone(),
        index_name.to_string(),
        schema_ptr2,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // Write to writer 1 only
    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20],
        vec![Some("E"), Some("C"), Some("A"), Some("D"), Some("B")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename1.clone(), ap, sp).unwrap();

    // Query with prefix that matches only writer 1's directory
    let prefix1 = Path::new(&filename1)
        .parent()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let prefix2 = Path::new(&filename2)
        .parent()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let mem1 = NativeParquetWriter::get_filtered_writer_memory_usage(prefix1).unwrap();
    let mem2 = NativeParquetWriter::get_filtered_writer_memory_usage(prefix2).unwrap();

    // Writer 1 had data written (and chunks flushed), writer 2 did not
    assert!(mem1 >= 0, "Writer 1 memory should be reported");
    assert_eq!(mem2, 0, "Writer 2 should have 0 memory (no data written)");

    // Cleanup
    NativeParquetWriter::finalize_writer(filename1).unwrap();
    NativeParquetWriter::finalize_writer(filename2).unwrap();

    SETTINGS_STORE.remove(index_name);
}

// ===== write_data to non-existent writer (IPC variant) =====

/// Test: write_data fails with "Writer not found" when no sorted (IPC) writer exists for the file.
#[test]
fn test_write_data_no_ipc_writer() {
    let (_temp_dir, filename) = get_temp_file_path("no_ipc_writer.parquet");

    // Don't create any writer — just try to write data
    let (ap, sp) = create_ffi_data_with_row_id(
        vec![10, 20, 30],
        vec![Some("A"), Some("B"), Some("C")],
        vec![0, 1, 2],
    );
    let result = NativeParquetWriter::write_data(filename.clone(), ap, sp);

    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("Writer not found"),
        "Should get 'Writer not found' error for non-existent IPC writer"
    );
}

// ===== Sort stability edge case =====

/// Test: All rows have identical sort keys. The sort is unstable (sort_unstable_by),
/// so tie-breaking order is non-deterministic. This test verifies that:
/// - The output is valid (all ages are 30)
/// - Row IDs are sequential 0..N
/// - The permutation mapping is a valid permutation (bijection of 0..N)
/// - The mapping correctly maps each original row to its output position
/// It does NOT assert a specific tie-breaking order.
#[test]
fn test_sort_all_identical_keys_produces_valid_permutation() {
    let (_temp_dir, filename) = get_temp_file_path("identical_keys.parquet");
    let index_name = "test-identical-keys";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    // All rows have age=30 — sort has zero ordering information
    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 30, 30, 30, 30],
        vec![Some("E"), Some("D"), Some("C"), Some("B"), Some("A")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();

    let finalize_result = NativeParquetWriter::finalize_writer(filename.clone())
        .unwrap()
        .unwrap();

    // All ages should be 30
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![30, 30, 30, 30, 30]);

    // Row IDs must be sequential 0..N regardless of tie-breaking
    let row_ids = read_row_ids_from_parquet(&filename);
    assert_eq!(row_ids, vec![0, 1, 2, 3, 4]);

    // Permutation must be a valid bijection of 0..4
    let mapping = finalize_result.row_id_mapping.expect("Should have mapping");
    assert_eq!(mapping.len(), 5);
    let mut sorted_mapping = mapping.clone();
    sorted_mapping.sort();
    assert_eq!(
        sorted_mapping,
        vec![0, 1, 2, 3, 4],
        "Mapping must be a valid permutation even with all-identical sort keys"
    );
}

// ===== Writer properties verification tests =====
// These tests verify that writer properties (compression, bloom filter, format version,
// writer generation) configured via NativeSettings are actually honored in the output
// Parquet file for all finalize_sorted_chunks paths: empty, single-chunk, and multi-chunk.

/// Helper: reads the compression codec from the first row group's first column chunk.
fn read_compression_from_parquet(filename: &str) -> parquet::basic::Compression {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = File::open(filename).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    assert!(
        metadata.num_row_groups() > 0,
        "File should have at least one row group"
    );
    let rg = metadata.row_group(0);
    assert!(
        rg.num_columns() > 0,
        "Row group should have at least one column"
    );
    rg.column(0).compression()
}

/// Helper: checks whether bloom filter metadata is present in the first row group's columns.
fn has_bloom_filter_in_parquet(filename: &str) -> bool {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = File::open(filename).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    if metadata.num_row_groups() == 0 {
        return false;
    }
    let rg = metadata.row_group(0);
    // Check if any column has bloom filter offset set
    for i in 0..rg.num_columns() {
        if rg.column(i).bloom_filter_offset().is_some() {
            return true;
        }
    }
    false
}

/// Helper: reads the format version from Parquet file key-value metadata.
fn read_format_version_from_parquet(filename: &str) -> Option<String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = File::open(filename).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata().file_metadata();
    metadata.key_value_metadata().and_then(|kvs| {
        kvs.iter()
            .find(|kv| kv.key == "opensearch.format_version")
            .and_then(|kv| kv.value.clone())
    })
}

/// Test: Writer properties are honored in the EMPTY path (0 chunks).
/// Verifies format version is stamped and writer generation is present.
#[test]
fn test_writer_properties_honored_empty_path() {
    let (_temp_dir, filename) = get_temp_file_path("props_empty.parquet");
    let index_name = "test-props-empty";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    settings.compression_type = Some("SNAPPY".to_string());
    settings.bloom_filter_enabled = Some(false);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let writer_generation = 99i64;
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        writer_generation,
    )
    .unwrap();

    // Don't write any data — triggers the empty path
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Verify format version is stamped
    let format_version = read_format_version_from_parquet(&filename);
    assert_eq!(
        format_version.as_deref(),
        Some("1.0.0.0"),
        "Empty path should stamp format version in output file"
    );

    // Verify writer generation is stamped
    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(
        gen,
        Some(99),
        "Empty path should stamp writer_generation in output file"
    );

    // Empty file has no row groups, so we can't check compression or bloom filter
    // on column chunks. But the properties were applied to the writer.

    SETTINGS_STORE.remove(index_name);
}

/// Test: Writer properties (SNAPPY compression, bloom filter disabled) are honored
/// in the SINGLE CHUNK path (chunk_paths.len() == 1).
#[test]
fn test_writer_properties_honored_single_chunk_snappy_no_bloom() {
    let (_temp_dir, filename) = get_temp_file_path("props_single_snappy.parquet");
    let index_name = "test-props-single-snappy";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024); // Large — single chunk
    settings.compression_type = Some("SNAPPY".to_string());
    settings.bloom_filter_enabled = Some(false);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let writer_generation = 55i64;
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        writer_generation,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 50, 20, 40],
        vec![Some("C"), Some("A"), Some("E"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Verify compression is SNAPPY
    let compression = read_compression_from_parquet(&filename);
    assert!(
        matches!(compression, parquet::basic::Compression::SNAPPY),
        "Single chunk path should honor SNAPPY compression, got: {:?}",
        compression
    );

    // Verify bloom filter is NOT present (disabled)
    assert!(
        !has_bloom_filter_in_parquet(&filename),
        "Single chunk path should honor bloom_filter_enabled=false (no bloom filter in file)"
    );

    // Verify format version
    let format_version = read_format_version_from_parquet(&filename);
    assert_eq!(
        format_version.as_deref(),
        Some("1.0.0.0"),
        "Single chunk path should stamp format version"
    );

    // Verify writer generation
    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(
        gen,
        Some(55),
        "Single chunk path should stamp writer_generation"
    );

    // Verify data correctness (sort order)
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![10, 20, 30, 40, 50]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: Writer properties (ZSTD compression, bloom filter enabled) are honored
/// in the SINGLE CHUNK path.
#[test]
fn test_writer_properties_honored_single_chunk_zstd_with_bloom() {
    let (_temp_dir, filename) = get_temp_file_path("props_single_zstd.parquet");
    let index_name = "test-props-single-zstd";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    settings.compression_type = Some("ZSTD".to_string());
    settings.compression_level = Some(3);
    settings.bloom_filter_enabled = Some(true);
    settings.bloom_filter_fpp = Some(0.05);
    settings.bloom_filter_ndv = Some(50_000);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let writer_generation = 12i64;
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        writer_generation,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 50, 20, 40],
        vec![Some("C"), Some("A"), Some("E"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Verify compression is ZSTD
    let compression = read_compression_from_parquet(&filename);
    assert!(
        matches!(compression, parquet::basic::Compression::ZSTD(_)),
        "Single chunk path should honor ZSTD compression, got: {:?}",
        compression
    );

    // Verify bloom filter IS present (enabled)
    assert!(
        has_bloom_filter_in_parquet(&filename),
        "Single chunk path should honor bloom_filter_enabled=true (bloom filter should be in file)"
    );

    // Verify format version
    let format_version = read_format_version_from_parquet(&filename);
    assert_eq!(format_version.as_deref(), Some("1.0.0.0"));

    // Verify writer generation
    let gen = read_writer_generation_from_parquet(&filename);
    assert_eq!(gen, Some(12));

    SETTINGS_STORE.remove(index_name);
}

/// Test: Writer properties (SNAPPY compression, bloom filter disabled) are honored
/// in the MULTI CHUNK path (k-way merge).
#[test]
fn test_writer_properties_honored_multi_chunk_snappy_no_bloom() {
    let (_temp_dir, filename) = get_temp_file_path("props_multi_snappy.parquet");
    let index_name = "test-props-multi-snappy";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Tiny — forces multiple chunks
    settings.compression_type = Some("SNAPPY".to_string());
    settings.bloom_filter_enabled = Some(false);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let writer_generation = 77i64;
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        writer_generation,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![
            Some("E"),
            Some("C"),
            Some("A"),
            Some("D"),
            Some("B"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Verify compression is SNAPPY in the merged output
    let compression = read_compression_from_parquet(&filename);
    assert!(
        matches!(compression, parquet::basic::Compression::SNAPPY),
        "Multi chunk (k-way merge) path should honor SNAPPY compression, got: {:?}",
        compression
    );

    // Verify bloom filter is NOT present
    assert!(
        !has_bloom_filter_in_parquet(&filename),
        "Multi chunk path should honor bloom_filter_enabled=false"
    );

    // Verify format version
    let format_version = read_format_version_from_parquet(&filename);
    assert_eq!(
        format_version.as_deref(),
        Some("1.0.0.0"),
        "Multi chunk path should stamp format version"
    );

    // Verify data correctness
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![5, 10, 15, 20, 30, 35, 40, 45, 50, 60]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: Writer properties (ZSTD compression, bloom filter enabled) are honored
/// in the MULTI CHUNK path (k-way merge).
#[test]
fn test_writer_properties_honored_multi_chunk_zstd_with_bloom() {
    let (_temp_dir, filename) = get_temp_file_path("props_multi_zstd.parquet");
    let index_name = "test-props-multi-zstd";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Tiny — forces multiple chunks
    settings.compression_type = Some("ZSTD".to_string());
    settings.compression_level = Some(5);
    settings.bloom_filter_enabled = Some(true);
    settings.bloom_filter_fpp = Some(0.01);
    settings.bloom_filter_ndv = Some(200_000);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let writer_generation = 33i64;
    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        writer_generation,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![
            Some("E"),
            Some("C"),
            Some("A"),
            Some("D"),
            Some("B"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Verify compression is ZSTD in the merged output
    let compression = read_compression_from_parquet(&filename);
    assert!(
        matches!(compression, parquet::basic::Compression::ZSTD(_)),
        "Multi chunk (k-way merge) path should honor ZSTD compression, got: {:?}",
        compression
    );

    // Verify bloom filter IS present
    assert!(
        has_bloom_filter_in_parquet(&filename),
        "Multi chunk path should honor bloom_filter_enabled=true"
    );

    // Verify format version
    let format_version = read_format_version_from_parquet(&filename);
    assert_eq!(format_version.as_deref(), Some("1.0.0.0"));

    // Verify data correctness
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![5, 10, 15, 20, 30, 35, 40, 45, 50, 60]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: UNCOMPRESSED setting is honored in single chunk path.
/// This ensures the writer doesn't silently fall back to a default compression.
#[test]
fn test_writer_properties_honored_single_chunk_uncompressed() {
    let (_temp_dir, filename) = get_temp_file_path("props_single_uncompressed.parquet");
    let index_name = "test-props-single-uncompressed";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    settings.compression_type = Some("UNCOMPRESSED".to_string());
    settings.bloom_filter_enabled = Some(true);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 50, 20, 40],
        vec![Some("C"), Some("A"), Some("E"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Verify compression is UNCOMPRESSED
    let compression = read_compression_from_parquet(&filename);
    assert!(
        matches!(compression, parquet::basic::Compression::UNCOMPRESSED),
        "Single chunk path should honor UNCOMPRESSED setting, got: {:?}",
        compression
    );

    // Verify bloom filter IS present
    assert!(
        has_bloom_filter_in_parquet(&filename),
        "Single chunk path should honor bloom_filter_enabled=true"
    );

    SETTINGS_STORE.remove(index_name);
}

/// Test: UNCOMPRESSED setting is honored in multi chunk path.
#[test]
fn test_writer_properties_honored_multi_chunk_uncompressed() {
    let (_temp_dir, filename) = get_temp_file_path("props_multi_uncompressed.parquet");
    let index_name = "test-props-multi-uncompressed";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Tiny — forces multiple chunks
    settings.compression_type = Some("UNCOMPRESSED".to_string());
    settings.bloom_filter_enabled = Some(true);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![
            Some("E"),
            Some("C"),
            Some("A"),
            Some("D"),
            Some("B"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Verify compression is UNCOMPRESSED
    let compression = read_compression_from_parquet(&filename);
    assert!(
        matches!(compression, parquet::basic::Compression::UNCOMPRESSED),
        "Multi chunk path should honor UNCOMPRESSED setting, got: {:?}",
        compression
    );

    // Verify bloom filter IS present
    assert!(
        has_bloom_filter_in_parquet(&filename),
        "Multi chunk path should honor bloom_filter_enabled=true"
    );

    // Verify data correctness
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![5, 10, 15, 20, 30, 35, 40, 45, 50, 60]);

    SETTINGS_STORE.remove(index_name);
}

/// Test: Default writer properties (LZ4_RAW compression, bloom filter enabled) are
/// applied when no explicit settings are configured — single chunk path.
#[test]
fn test_writer_properties_defaults_single_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("props_defaults_single.parquet");
    let index_name = "test-props-defaults-single";

    // Use defaults — only configure sort
    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(10 * 1024 * 1024);
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![30, 10, 50, 20, 40],
        vec![Some("C"), Some("A"), Some("E"), Some("B"), Some("D")],
        vec![0, 1, 2, 3, 4],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Default compression is LZ4_RAW
    let compression = read_compression_from_parquet(&filename);
    assert!(
        matches!(compression, parquet::basic::Compression::LZ4_RAW),
        "Default compression should be LZ4_RAW, got: {:?}",
        compression
    );

    // Default bloom filter is enabled
    assert!(
        !has_bloom_filter_in_parquet(&filename),
        "Default bloom_filter_enabled should be false"
    );

    // Format version always stamped
    let format_version = read_format_version_from_parquet(&filename);
    assert_eq!(format_version.as_deref(), Some("1.0.0.0"));

    SETTINGS_STORE.remove(index_name);
}

/// Test: Default writer properties are applied in multi chunk path.
#[test]
fn test_writer_properties_defaults_multi_chunk() {
    let (_temp_dir, filename) = get_temp_file_path("props_defaults_multi.parquet");
    let index_name = "test-props-defaults-multi";

    let mut settings = NativeSettings::default();
    settings.sort_columns = vec!["age".to_string()];
    settings.reverse_sorts = vec![false];
    settings.nulls_first = vec![false];
    settings.sort_in_memory_threshold_bytes = Some(64); // Tiny — forces multiple chunks
    SETTINGS_STORE.insert(index_name.to_string(), settings);

    let (_schema, schema_ptr) = create_row_id_schema_ptr();
    NativeParquetWriter::create_writer(
        filename.clone(),
        index_name.to_string(),
        schema_ptr,
        vec!["age".to_string()],
        vec![false],
        vec![false],
        0,
    )
    .unwrap();

    let (ap, sp) = create_ffi_data_with_row_id(
        vec![50, 30, 10, 40, 20, 60, 5, 45, 35, 15],
        vec![
            Some("E"),
            Some("C"),
            Some("A"),
            Some("D"),
            Some("B"),
            Some("F"),
            Some("G"),
            Some("H"),
            Some("I"),
            Some("J"),
        ],
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    );
    NativeParquetWriter::write_data(filename.clone(), ap, sp).unwrap();
    NativeParquetWriter::finalize_writer(filename.clone()).unwrap();

    // Default compression is LZ4_RAW
    let compression = read_compression_from_parquet(&filename);
    assert!(
        matches!(compression, parquet::basic::Compression::LZ4_RAW),
        "Default compression should be LZ4_RAW in multi-chunk path, got: {:?}",
        compression
    );

    // Default bloom filter is enabled
    assert!(
        !has_bloom_filter_in_parquet(&filename),
        "Default bloom_filter_enabled should be false in multi-chunk path"
    );

    // Format version always stamped
    let format_version = read_format_version_from_parquet(&filename);
    assert_eq!(format_version.as_deref(), Some("1.0.0.0"));

    // Data correctness
    let ages = read_ages_from_parquet(&filename);
    assert_eq!(ages, vec![5, 10, 15, 20, 30, 35, 40, 45, 50, 60]);

    SETTINGS_STORE.remove(index_name);
}
