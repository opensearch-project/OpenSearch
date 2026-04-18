/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for the Parquet writer and ObjectStore handle management.
//!
//! Return convention: `>= 0` success, `< 0` error pointer (negate to get ptr,
//! call `native_error_message`/`native_error_free`).

use std::slice;
use std::str;

use native_bridge_common::ffm_safe;

use crate::object_store_handle;
use crate::writer;

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    if len < 0 {
        return Err(format!("negative string length: {}", len));
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))
}

// ═══════════════════════════════════════════════════════════════════════════════
// Writer functions (handle-based, ObjectStore-backed)
// ═══════════════════════════════════════════════════════════════════════════════

/// Creates a new Parquet writer backed by an ObjectStore.
/// Returns a writer handle (i64 pointer) on success.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_create_writer(
    store_handle: i64,
    path_ptr: *const u8,
    path_len: i64,
    schema_address: i64,
) -> i64 {
    let object_path =
        str_from_raw(path_ptr, path_len).map_err(|e| format!("parquet_create_writer: {}", e))?;
    writer::create_writer(store_handle, object_path, schema_address).map_err(|e| e.to_string())
}

/// Writes an Arrow record batch to the writer.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_write(
    writer_handle: i64,
    array_address: i64,
    schema_address: i64,
) -> i64 {
    writer::write_data(writer_handle, array_address, schema_address)
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

/// Finalizes the writer: flushes footer, uploads to ObjectStore, returns metadata.
/// The writer handle is consumed and invalid after this call.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_finalize_writer(
    writer_handle: i64,
    num_rows_out: *mut i64,
    crc32_out: *mut i64,
) -> i64 {
    let result = writer::finalize_writer(writer_handle).map_err(|e| e.to_string())?;
    if !num_rows_out.is_null() {
        *num_rows_out = result.num_rows;
    }
    if !crc32_out.is_null() {
        *crc32_out = result.crc32 as i64;
    }
    Ok(0)
}

/// Destroys a writer without finalizing (error cleanup).
#[no_mangle]
pub unsafe extern "C" fn parquet_destroy_writer(writer_handle: i64) {
    writer::destroy_writer(writer_handle);
}

/// Reads Parquet file metadata from a local file path.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_file_metadata(
    file_ptr: *const u8,
    file_len: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_get_file_metadata: {}", e))?
        .to_string();
    let fm = writer::get_file_metadata(filename).map_err(|e| e.to_string())?;
    if !version_out.is_null() {
        *version_out = fm.version();
    }
    if !num_rows_out.is_null() {
        *num_rows_out = fm.num_rows();
    }
    if let Some(cb) = fm.created_by() {
        if !created_by_buf.is_null() && created_by_buf_len > 0 {
            let bytes = cb.as_bytes();
            let n = bytes.len().min(created_by_buf_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
            if !created_by_len_out.is_null() {
                *created_by_len_out = n as i64;
            }
        }
    } else if !created_by_len_out.is_null() {
        *created_by_len_out = -1;
    }
    Ok(0)
}

// ═══════════════════════════════════════════════════════════════════════════════
// ObjectStore handle management
// ═══════════════════════════════════════════════════════════════════════════════

/// Creates a local filesystem ObjectStore rooted at the given path.
/// Returns a new store handle.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_create_local_store(path_ptr: *const u8, path_len: i64) -> i64 {
    let path = str_from_raw(path_ptr, path_len)
        .map_err(|e| format!("parquet_create_local_store: {}", e))?;
    object_store_handle::create_local_store(path)
        .map_err(|e| format!("parquet_create_local_store: {}", e))
}

/// Creates a PrefixStore scoping a parent ObjectStore to the given prefix.
/// Returns a new store handle (i64 pointer). Parent handle remains valid.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_create_scoped_store(
    parent_handle: i64,
    prefix_ptr: *const u8,
    prefix_len: i64,
) -> i64 {
    let prefix = str_from_raw(prefix_ptr, prefix_len)
        .map_err(|e| format!("parquet_create_scoped_store: {}", e))?;
    object_store_handle::create_scoped_store(parent_handle, prefix)
        .map_err(|e| format!("parquet_create_scoped_store: {}", e))
}

/// Drops an ObjectStore handle, decrementing the Arc refcount.
#[no_mangle]
pub unsafe extern "C" fn parquet_destroy_store(handle: i64) {
    object_store_handle::drop_store(handle);
}
