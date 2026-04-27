/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for the Parquet writer.
//!
//! Return convention: `>= 0` success, `< 0` error pointer (negate to get ptr,
//! call `native_error_message`/`native_error_free`).

use std::slice;
use std::str;

use native_bridge_common::ffm_safe;

use crate::native_settings::NativeSettings;
use crate::merge;
use crate::writer::{NativeParquetWriter, SETTINGS_STORE};

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

/// Decode a parallel (pointers, lengths, count) triple into `Vec<String>`.
unsafe fn str_array_from_raw(
    ptrs: *const *const u8,
    lens: *const i64,
    count: i64,
) -> Result<Vec<String>, String> {
    if count == 0 {
        return Ok(vec![]);
    }
    if ptrs.is_null() || lens.is_null() {
        return Err("null string array pointer".to_string());
    }
    let n = count as usize;
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let p = *ptrs.add(i);
        let l = *lens.add(i);
        out.push(str_from_raw(p, l)?.to_string());
    }
    Ok(out)
}

/// Decode a parallel (pointers, count) array of i64 values interpreted as booleans (0 = false).
unsafe fn bool_array_from_raw(
    vals: *const i64,
    count: i64,
) -> Vec<bool> {
    if count == 0 || vals.is_null() {
        return vec![];
    }
    let n = count as usize;
    (0..n).map(|i| *vals.add(i) != 0).collect()
}

// ---------------------------------------------------------------------------
// Writer lifecycle
// ---------------------------------------------------------------------------

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_create_writer(
    file_ptr: *const u8,
    file_len: i64,
    index_name_ptr: *const u8,
    index_name_len: i64,
    schema_address: i64,
    sort_ptrs: *const *const u8,
    sort_lens: *const i64,
    sort_count: i64,
    reverse_vals: *const i64,
    reverse_count: i64,
    nulls_first_vals: *const i64,
    nulls_first_count: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_create_writer file: {}", e))?.to_string();
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_create_writer index_name: {}", e))?.to_string();
    let sort_columns = str_array_from_raw(sort_ptrs, sort_lens, sort_count)
        .map_err(|e| format!("parquet_create_writer sort_columns: {}", e))?;
    let reverse_sorts = bool_array_from_raw(reverse_vals, reverse_count);
    let nulls_first = bool_array_from_raw(nulls_first_vals, nulls_first_count);

    NativeParquetWriter::create_writer(filename, index_name, schema_address, sort_columns, reverse_sorts, nulls_first)
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_write(
    file_ptr: *const u8,
    file_len: i64,
    array_address: i64,
    schema_address: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_write: {}", e))?.to_string();
    NativeParquetWriter::write_data(filename, array_address, schema_address)
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

/// Returns 0 with metadata in out-pointers, 1 if no writer found.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_finalize_writer(
    file_ptr: *const u8,
    file_len: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
    crc32_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_finalize_writer: {}", e))?.to_string();
    match NativeParquetWriter::finalize_writer(filename) {
        Ok(Some(result)) => {
            let fm = result.metadata.file_metadata();
            if !version_out.is_null() { *version_out = fm.version(); }
            if !num_rows_out.is_null() { *num_rows_out = fm.num_rows(); }
            if let Some(cb) = fm.created_by() {
                if !created_by_buf.is_null() && created_by_buf_len > 0 {
                    let bytes = cb.as_bytes();
                    let n = bytes.len().min(created_by_buf_len as usize);
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
                    if !created_by_len_out.is_null() { *created_by_len_out = n as i64; }
                }
            } else if !created_by_len_out.is_null() {
                *created_by_len_out = -1;
            }
            if !crc32_out.is_null() { *crc32_out = result.crc32 as i64; }
            Ok(0)
        }
        Ok(None) => Ok(1),
        Err(e) => Err(e.to_string()),
    }
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_sync_to_disk(
    file_ptr: *const u8,
    file_len: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_sync_to_disk: {}", e))?.to_string();
    NativeParquetWriter::sync_to_disk(filename)
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

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
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_get_file_metadata: {}", e))?.to_string();
    let fm = NativeParquetWriter::get_file_metadata(filename).map_err(|e| e.to_string())?;
    if !version_out.is_null() { *version_out = fm.version(); }
    if !num_rows_out.is_null() { *num_rows_out = fm.num_rows(); }
    if let Some(cb) = fm.created_by() {
        if !created_by_buf.is_null() && created_by_buf_len > 0 {
            let bytes = cb.as_bytes();
            let n = bytes.len().min(created_by_buf_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
            if !created_by_len_out.is_null() { *created_by_len_out = n as i64; }
        }
    } else if !created_by_len_out.is_null() {
        *created_by_len_out = -1;
    }
    Ok(0)
}

#[no_mangle]
pub unsafe extern "C" fn parquet_get_filtered_native_bytes_used(
    prefix_ptr: *const u8,
    prefix_len: i64,
) -> i64 {
    let prefix = str_from_raw(prefix_ptr, prefix_len).unwrap_or("").to_string();
    NativeParquetWriter::get_filtered_writer_memory_usage(prefix).unwrap_or(0) as i64
}

// ---------------------------------------------------------------------------
// Settings management
// ---------------------------------------------------------------------------

/// Update native settings for an index. Nullable fields use sentinel -1 for "not set".
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_on_settings_update(
    index_name_ptr: *const u8,
    index_name_len: i64,
    compression_type_ptr: *const u8,
    compression_type_len: i64,
    compression_level: i64,
    page_size_bytes: i64,
    page_row_limit: i64,
    dict_size_bytes: i64,
    row_group_size_bytes: i64,
    bloom_filter_enabled: i64,
    bloom_filter_fpp: f64,
    bloom_filter_ndv: i64,
    sort_in_memory_threshold_bytes: i64,
    sort_batch_size: i64,
) -> i64 {
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_on_settings_update index_name: {}", e))?.to_string();

    let compression_type = if compression_type_ptr.is_null() || compression_type_len < 0 {
        None
    } else {
        Some(str_from_raw(compression_type_ptr, compression_type_len)
            .map_err(|e| format!("parquet_on_settings_update compression_type: {}", e))?.to_string())
    };

    fn opt_i32(v: i64) -> Option<i32> { if v < 0 { None } else { Some(v as i32) } }
    fn opt_usize(v: i64) -> Option<usize> { if v < 0 { None } else { Some(v as usize) } }
    fn opt_bool(v: i64) -> Option<bool> { if v < 0 { None } else { Some(v != 0) } }
    fn opt_f64(v: f64) -> Option<f64> { if v < 0.0 { None } else { Some(v) } }
    fn opt_u64(v: i64) -> Option<u64> { if v < 0 { None } else { Some(v as u64) } }

    let config = NativeSettings {
        index_name: Some(index_name.clone()),
        compression_type,
        compression_level: opt_i32(compression_level),
        page_size_bytes: opt_usize(page_size_bytes),
        page_row_limit: opt_usize(page_row_limit),
        dict_size_bytes: opt_usize(dict_size_bytes),
        row_group_size_bytes: opt_usize(row_group_size_bytes),
        bloom_filter_enabled: opt_bool(bloom_filter_enabled),
        bloom_filter_fpp: opt_f64(bloom_filter_fpp),
        bloom_filter_ndv: opt_u64(bloom_filter_ndv),
        sort_in_memory_threshold_bytes: opt_u64(sort_in_memory_threshold_bytes),
        sort_batch_size: opt_usize(sort_batch_size),
        ..Default::default()
    };

    SETTINGS_STORE.insert(index_name, config);
    Ok(0)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_remove_settings(
    index_name_ptr: *const u8,
    index_name_len: i64,
) -> i64 {
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_remove_settings: {}", e))?.to_string();
    SETTINGS_STORE.remove(&index_name);
    Ok(0)
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_merge_files(
    input_ptrs: *const *const u8,
    input_lens: *const i64,
    input_count: i64,
    output_ptr: *const u8,
    output_len: i64,
    index_name_ptr: *const u8,
    index_name_len: i64,
) -> i64 {
    let input_files = str_array_from_raw(input_ptrs, input_lens, input_count)
        .map_err(|e| format!("parquet_merge_files inputs: {}", e))?;
    let output_path = str_from_raw(output_ptr, output_len)
        .map_err(|e| format!("parquet_merge_files output: {}", e))?;
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_merge_files index_name: {}", e))?;

    let (sort_cols, reverse_flags, nulls_first_flags) = match SETTINGS_STORE.get(index_name) {
        Some(s) => {
            let sc = s.sort_columns.clone();
            let rf = s.reverse_sorts.clone();
            let nf = s.nulls_first.clone();
            if !sc.is_empty() && rf.is_empty() {
                crate::log_info!("parquet_merge_files: sort columns present but reverse_sorts is empty for index '{}', defaulting to ascending", index_name);
            }
            if !sc.is_empty() && nf.is_empty() {
                crate::log_info!("parquet_merge_files: sort columns present but nulls_first is empty for index '{}', defaulting to nulls last", index_name);
            }
            (sc, rf, nf)
        }
        None => {
            crate::log_info!("parquet_merge_files: no settings found for index '{}', proceeding with unsorted merge", index_name);
            (vec![], vec![], vec![])
        }
    };

    if sort_cols.is_empty() {
        merge::merge_unsorted(&input_files, output_path, index_name)
    } else {
        merge::merge_sorted(
            &input_files,
            output_path,
            index_name,
            &sort_cols,
            &reverse_flags,
            &nulls_first_flags,
        )
    }
    .map(|_| 0)
    .map_err(|e| format!("{}", e))
}
