/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use crate::test_utils::*;
use crate::writer::{NativeParquetWriter, WRITER_MANAGER, FILE_MANAGER};

#[test]
fn test_create_writer_success() {
    let (_temp_dir, filename) = get_temp_file_path("test.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    assert!(WRITER_MANAGER.contains_key(&filename));
    assert!(FILE_MANAGER.contains_key(&filename));
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_create_writer_invalid_path() {
    let invalid_path = "/invalid/path/that/does/not/exist/test.parquet";
    let (_schema, schema_ptr) = create_test_ffi_schema();
    let result = NativeParquetWriter::create_writer(invalid_path.to_string(), schema_ptr);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("No such file or directory"));
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_create_writer_invalid_schema_pointer() {
    let (_temp_dir, filename) = get_temp_file_path("invalid_schema.parquet");
    let result = NativeParquetWriter::create_writer(filename, 0);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid schema address"));
}

#[test]
fn test_create_writer_multiple_times_same_file() {
    let (_temp_dir, filename) = get_temp_file_path("duplicate.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    let result2 = NativeParquetWriter::create_writer(filename.clone(), schema_ptr);
    assert!(result2.is_err());
    assert!(result2.unwrap_err().to_string().contains("Writer already exists"));
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
    let result = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(result.is_ok());
    let metadata = result.unwrap();
    assert!(metadata.is_some());
    let metadata = metadata.unwrap();
    assert_eq!(metadata.num_rows, 0);
    assert!(metadata.version > 0);
    assert!(!WRITER_MANAGER.contains_key(&filename));
    assert!(FILE_MANAGER.contains_key(&filename));
    FILE_MANAGER.remove(&filename);
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
    assert_eq!(metadata.num_rows, 6);
    assert!(metadata.version > 0);
    assert_eq!(metadata.schema.len(), 3); // root + 2 fields (id, name)
    FILE_MANAGER.remove(&filename);
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
    let result1 = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(result1.is_ok());
    let metadata = result1.unwrap();
    assert!(metadata.is_some());
    assert_eq!(metadata.unwrap().num_rows, 0);
    assert!(!WRITER_MANAGER.contains_key(&filename));
    assert!(FILE_MANAGER.contains_key(&filename));
    let result2 = NativeParquetWriter::finalize_writer(filename.clone());
    assert!(result2.is_err());
    assert!(result2.unwrap_err().to_string().contains("Writer not found"));
    FILE_MANAGER.remove(&filename);
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_sync_to_disk_success() {
    let (_temp_dir, filename) = get_temp_file_path("test_flush.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
    assert!(FILE_MANAGER.contains_key(&filename));
    let result = NativeParquetWriter::sync_to_disk(filename.clone());
    assert!(result.is_ok());
    assert!(!FILE_MANAGER.contains_key(&filename));
    close_writer_and_cleanup_schema(&filename, schema_ptr);
}

#[test]
fn test_flush_nonexistent_file() {
    let result = NativeParquetWriter::sync_to_disk("nonexistent.parquet".to_string());
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().to_string(), "File not found");
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
    let _memory_usage = result.unwrap();
    assert!(_memory_usage >= 0);
    close_writer_and_cleanup_schema(&filename1, schema_ptr1);
    close_writer_and_cleanup_schema(&filename2, schema_ptr2);
}
