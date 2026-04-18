/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use crate::object_store_handle;
use crate::test_utils::*;
use crate::writer;

use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::io::Read;

#[test]
fn test_create_writer_success() {
    let (_temp_dir, store_handle) = create_temp_store();
    let writer_handle = create_writer_on_store(store_handle, "test.parquet");
    assert!(writer_handle > 0);
    writer::destroy_writer(writer_handle);
    unsafe {
        object_store_handle::drop_store(store_handle);
    }
}

#[test]
fn test_create_writer_invalid_store() {
    let (_schema, schema_ptr) = create_test_ffi_schema();
    let result = writer::create_writer(0, "test.parquet", schema_ptr);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid store handle"));
    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_create_writer_invalid_schema_pointer() {
    let (_temp_dir, store_handle) = create_temp_store();
    let result = writer::create_writer(store_handle, "test.parquet", 0);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid schema address"));
    unsafe {
        object_store_handle::drop_store(store_handle);
    }
}

#[test]
fn test_write_data_success() {
    let (_temp_dir, store_handle) = create_temp_store();
    let writer_handle = create_writer_on_store(store_handle, "write_success.parquet");
    let (array_ptr, data_schema_ptr) = write_ffi_data_to_handle(writer_handle);
    cleanup_ffi_data(array_ptr, data_schema_ptr);
    writer::destroy_writer(writer_handle);
    unsafe {
        object_store_handle::drop_store(store_handle);
    }
}

#[test]
fn test_write_data_invalid_handle() {
    let (array_ptr, schema_ptr) = create_test_ffi_data().unwrap();
    let result = writer::write_data(0, array_ptr, schema_ptr);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid writer handle"));
    cleanup_ffi_data(array_ptr, schema_ptr);
}

#[test]
fn test_write_data_invalid_pointers() {
    let (_temp_dir, store_handle) = create_temp_store();
    let writer_handle = create_writer_on_store(store_handle, "invalid_ffi.parquet");
    let result = writer::write_data(writer_handle, 0, 0);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid FFI addresses"));
    writer::destroy_writer(writer_handle);
    unsafe {
        object_store_handle::drop_store(store_handle);
    }
}

#[test]
fn test_finalize_writer_empty() {
    let (_temp_dir, store_handle) = create_temp_store();
    let writer_handle = create_writer_on_store(store_handle, "empty.parquet");
    let result = writer::finalize_writer(writer_handle);
    assert!(result.is_ok());
    let finalize = result.unwrap();
    assert_eq!(finalize.num_rows, 0);
    unsafe {
        object_store_handle::drop_store(store_handle);
    }
}

#[test]
fn test_finalize_writer_with_data() {
    let (_temp_dir, store_handle) = create_temp_store();
    let writer_handle = create_writer_on_store(store_handle, "with_data.parquet");

    for _ in 0..2 {
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_handle(writer_handle);
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }

    let result = writer::finalize_writer(writer_handle);
    assert!(result.is_ok());
    let finalize = result.unwrap();
    assert_eq!(finalize.num_rows, 6); // 2 batches × 3 rows
    assert_ne!(
        finalize.crc32, 0,
        "CRC32 should be non-zero for a file with data"
    );

    // Verify the file is readable from disk
    let file_path = _temp_dir.path().join("with_data.parquet");
    assert!(file_path.exists(), "Parquet file should exist on disk");
    let file = File::open(&file_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    assert_eq!(reader.metadata().file_metadata().num_rows(), 6);

    unsafe {
        object_store_handle::drop_store(store_handle);
    }
}

#[test]
fn test_finalize_invalid_handle() {
    let result = writer::finalize_writer(0);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid writer handle"));
}

/// Verify CRC32 from streaming matches re-read from disk.
#[test]
fn test_streaming_crc32_matches_reread() {
    let (_temp_dir, store_handle) = create_temp_store();
    let writer_handle = create_writer_on_store(store_handle, "crc32_test.parquet");

    for _ in 0..3 {
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_handle(writer_handle);
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }

    let finalize = writer::finalize_writer(writer_handle).unwrap();
    let streaming_crc32 = finalize.crc32;

    // Re-read file and compute CRC32
    let file_path = _temp_dir.path().join("crc32_test.parquet");
    let mut file = File::open(&file_path).unwrap();
    let mut hasher = crc32fast::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).unwrap();
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let reread_crc32 = hasher.finalize();

    assert_eq!(
        streaming_crc32, reread_crc32,
        "Streaming CRC32 ({:#010x}) must match re-read CRC32 ({:#010x})",
        streaming_crc32, reread_crc32
    );

    unsafe {
        object_store_handle::drop_store(store_handle);
    }
}

/// Verify scoped store writes to the correct prefixed path.
#[test]
fn test_scoped_store_writer() {
    let (_temp_dir, store_handle) = create_temp_store();
    let scoped_handle = unsafe {
        object_store_handle::create_scoped_store(store_handle, "indices/uuid1/0/parquet/")
    }
    .unwrap();

    let writer_handle = create_writer_on_store(scoped_handle, "gen_1/data_0.parquet");
    let (array_ptr, data_schema_ptr) = write_ffi_data_to_handle(writer_handle);
    cleanup_ffi_data(array_ptr, data_schema_ptr);

    let finalize = writer::finalize_writer(writer_handle).unwrap();
    assert_eq!(finalize.num_rows, 3);

    // Verify file landed at the prefixed path on disk
    let expected_path = _temp_dir
        .path()
        .join("indices/uuid1/0/parquet/gen_1/data_0.parquet");
    assert!(
        expected_path.exists(),
        "File should exist at prefixed path: {:?}",
        expected_path
    );

    unsafe {
        object_store_handle::drop_store(scoped_handle);
        object_store_handle::drop_store(store_handle);
    }
}
