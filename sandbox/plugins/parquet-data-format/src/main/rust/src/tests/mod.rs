/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use crate::test_utils::*;
use crate::writer::NativeParquetWriter;

#[test]
fn test_create_writer_success() {
    let (_temp_dir, store, path) = get_temp_store_and_path("test.parquet");
    let (_schema, schema_ptr, writer) = create_writer_and_assert_success(store, &path);
    assert_eq!(writer.path(), path);
    drop(writer);
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_create_writer_invalid_schema_pointer() {
    let (_temp_dir, store, path) = get_temp_store_and_path("invalid_schema.parquet");
    let result = NativeParquetWriter::create(store, path, 0);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid schema address"));
}

#[test]
fn test_write_data_success() {
    let (_temp_dir, store, path) = get_temp_store_and_path("write_success.parquet");
    let (_schema, schema_ptr, mut writer) = create_writer_and_assert_success(store, &path);
    let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
    let result = writer.write_data(array_ptr, data_schema_ptr);
    assert!(result.is_ok());
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    let _ = writer.finalize_writer();
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_write_data_invalid_pointers() {
    let (_temp_dir, store, path) = get_temp_store_and_path("invalid_ffi.parquet");
    let (_schema, schema_ptr, mut writer) = create_writer_and_assert_success(store, &path);
    let result = writer.write_data(0, 0);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Invalid FFI addresses"));
    let _ = writer.finalize_writer();
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_write_data_incompatible_schema() {
    let (_temp_dir, store, path) = get_temp_store_and_path("write_mismatch.parquet");
    let (_schema, schema_ptr, mut writer) = create_writer_and_assert_success(store, &path);
    let (array_ptr, data_schema_ptr) = create_mismatched_ffi_data().unwrap();
    let result = writer.write_data(array_ptr, data_schema_ptr);
    assert!(result.is_err());
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    let _ = writer.finalize_writer();
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_finalize_writer_success() {
    let (_temp_dir, store, path) = get_temp_store_and_path("test_close.parquet");
    let (_schema, schema_ptr, mut writer) = create_writer_and_assert_success(store, &path);
    let result = writer.finalize_writer();
    assert!(result.is_ok());
    let metadata = result.unwrap();
    assert!(metadata.is_some());
    let metadata = metadata.unwrap();
    assert_eq!(metadata.num_rows, 0);
    assert!(metadata.version > 0);
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_finalize_writer_with_data_returns_correct_metadata() {
    let (_temp_dir, store, path) = get_temp_store_and_path("close_with_data.parquet");
    let (_schema, schema_ptr, mut writer) = create_writer_and_assert_success(store, &path);
    for _ in 0..2 {
        let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
        writer.write_data(array_ptr, data_schema_ptr).unwrap();
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }
    let result = writer.finalize_writer();
    assert!(result.is_ok());
    let metadata = result.unwrap().unwrap();
    assert_eq!(metadata.num_rows, 6);
    assert!(metadata.version > 0);
    assert_eq!(metadata.schema.len(), 3); // root + 2 fields (id, name)
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_finalize_writer_twice_fails() {
    let (_temp_dir, store, path) = get_temp_store_and_path("double_finalize.parquet");
    let (_schema, schema_ptr, mut writer) = create_writer_and_assert_success(store, &path);
    let result1 = writer.finalize_writer();
    assert!(result1.is_ok());
    let result2 = writer.finalize_writer();
    assert!(result2.is_err());
    assert!(result2.unwrap_err().to_string().contains("Writer already finalized"));
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_write_after_finalize_fails() {
    let (_temp_dir, store, path) = get_temp_store_and_path("write_after_finalize.parquet");
    let (_schema, schema_ptr, mut writer) = create_writer_and_assert_success(store, &path);
    writer.finalize_writer().unwrap();
    let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
    let result = writer.write_data(array_ptr, data_schema_ptr);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Writer already finalized"));
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_memory_size() {
    let (_temp_dir, store, path) = get_temp_store_and_path("test_memory.parquet");
    let (_schema, schema_ptr, mut writer) = create_writer_and_assert_success(store, &path);
    assert!(writer.memory_size() > 0 || writer.memory_size() == 0); // just verify it doesn't panic
    writer.finalize_writer().unwrap();
    assert_eq!(writer.memory_size(), 0);
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_object_store_handle_roundtrip() {
    use crate::object_store_handle;
    let (_temp_dir, store, _path) = get_temp_store_and_path("unused.parquet");
    let handle = object_store_handle::store_to_handle(store);
    assert!(handle != 0);
    // Clone from handle
    let recovered = unsafe { object_store_handle::store_from_handle(handle) };
    drop(recovered);
    // Destroy original
    unsafe { object_store_handle::drop_store(handle) };
}

#[test]
fn test_create_object_store_local() {
    use crate::object_store_handle;
    let temp_dir = tempfile::tempdir().unwrap();
    let config = format!(r#"{{"root": "{}"}}"#, temp_dir.path().display());
    let store = object_store_handle::create_object_store("local", &config);
    assert!(store.is_ok());
}

#[test]
fn test_create_object_store_unknown_type() {
    use crate::object_store_handle;
    let result = object_store_handle::create_object_store("ftp", "{}");
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Unknown object store type"));
}
