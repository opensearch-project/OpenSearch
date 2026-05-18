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

use native_bridge_common::{ffm_safe, log_debug};

use crate::native_settings::NativeSettings;
use crate::field_config::FieldConfig;
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
    writer_generation: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_create_writer file: {}", e))?.to_string();
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_create_writer index_name: {}", e))?.to_string();
    let sort_columns = str_array_from_raw(sort_ptrs, sort_lens, sort_count)
        .map_err(|e| format!("parquet_create_writer sort_columns: {}", e))?;
    let reverse_sorts = bool_array_from_raw(reverse_vals, reverse_count);
    let nulls_first = bool_array_from_raw(nulls_first_vals, nulls_first_count);

    NativeParquetWriter::create_writer(filename, index_name, schema_address, sort_columns, reverse_sorts, nulls_first, writer_generation)
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
    num_row_groups_out: *mut i64,
    sort_perm_ptr_out: *mut i64,
    sort_perm_len_out: *mut i64,
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
            if !num_row_groups_out.is_null() { *num_row_groups_out = result.metadata.num_row_groups() as i64; }

            // Return sort permutation if present
            if !sort_perm_ptr_out.is_null() && !sort_perm_len_out.is_null() {
                if let Some(perm) = result.row_id_mapping {
                    let len = perm.len();
                    let boxed = perm.into_boxed_slice();
                    *sort_perm_len_out = len as i64;
                    *sort_perm_ptr_out = Box::into_raw(boxed) as *mut i64 as i64;
                } else {
                    *sort_perm_len_out = 0;
                    *sort_perm_ptr_out = 0;
                }
            }
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
    num_row_groups_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_get_file_metadata: {}", e))?.to_string();
    let metadata = NativeParquetWriter::get_file_metadata(filename).map_err(|e| e.to_string())?;
    let fm = metadata.file_metadata();
    if !version_out.is_null() { *version_out = fm.version(); }
    if !num_rows_out.is_null() { *num_rows_out = fm.num_rows(); }
    if !num_row_groups_out.is_null() { *num_row_groups_out = metadata.num_row_groups() as i64; }
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

/// Returns a JSON string with per-column encoding and compression metadata.
/// Format: {"column_name": {"encodings": ["PLAIN", "RLE_DICTIONARY"], "compression": "LZ4_RAW"}, ...}
/// Reads from the first row group.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_column_metadata(
    file_ptr: *const u8,
    file_len: i64,
    out_buf: *mut u8,
    out_buf_len: i64,
    out_len: *mut i64,
) -> i64 {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_get_column_metadata: {}", e))?.to_string();
    let file = File::open(&filename).map_err(|e| format!("Failed to open file: {}", e))?;
    let reader = SerializedFileReader::new(file).map_err(|e| format!("Failed to read parquet: {}", e))?;
    let metadata = reader.metadata();

    if metadata.num_row_groups() == 0 {
        let json = "{}".to_string();
        let bytes = json.as_bytes();
        let n = bytes.len().min(out_buf_len as usize);
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n);
        if !out_len.is_null() { *out_len = n as i64; }
        return Ok(0);
    }

    let rg = metadata.row_group(0);
    let mut json = String::from("{");
    for i in 0..rg.num_columns() {
        let col = rg.column(i);
        let col_name = col.column_path().string();
        let encodings: Vec<String> = col.encodings().map(|e| format!("{:?}", e)).collect();
        let compression = format!("{:?}", col.compression());
        let has_bloom_filter = col.bloom_filter_offset().is_some();
        if i > 0 { json.push(','); }
        json.push_str(&format!(
            "\"{}\":{{\"encodings\":[{}],\"compression\":\"{}\",\"bloom_filter\":{}}}",
            col_name,
            encodings.iter().map(|e| format!("\"{}\"" , e)).collect::<Vec<_>>().join(","),
            compression,
            has_bloom_filter
        ));
    }
    json.push('}');

    let bytes = json.as_bytes();
    let n = bytes.len().min(out_buf_len as usize);
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n);
    if !out_len.is_null() { *out_len = n as i64; }
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
    bloom_filter_enabled: i64,
    bloom_filter_fpp: f64,
    bloom_filter_ndv: i64,
    sort_in_memory_threshold_bytes: i64,
    sort_batch_size: i64,
    row_group_max_rows: i64,
    row_group_max_bytes: i64,
    merge_batch_size: i64,
    merge_rayon_threads: i64,
    merge_io_threads: i64,
    field_name_ptrs: *const *const u8,
    field_name_lens: *const i64,
    field_encoding_ptrs: *const *const u8,
    field_encoding_lens: *const i64,
    field_count: i64,
    field_compression_name_ptrs: *const *const u8,
    field_compression_name_lens: *const i64,
    field_compression_value_ptrs: *const *const u8,
    field_compression_value_lens: *const i64,
    field_compression_count: i64,
    type_encoding_name_ptrs: *const *const u8,
    type_encoding_name_lens: *const i64,
    type_encoding_value_ptrs: *const *const u8,
    type_encoding_value_lens: *const i64,
    type_encoding_count: i64,
    type_compression_name_ptrs: *const *const u8,
    type_compression_name_lens: *const i64,
    type_compression_value_ptrs: *const *const u8,
    type_compression_value_lens: *const i64,
    type_compression_count: i64,
    bf_enabled_name_ptrs: *const *const u8,
    bf_enabled_name_lens: *const i64,
    bf_enabled_vals: *const i64,
    bf_enabled_count: i64,
    type_bf_enabled_name_ptrs: *const *const u8,
    type_bf_enabled_name_lens: *const i64,
    type_bf_enabled_vals: *const i64,
    type_bf_enabled_count: i64,
    type_bf_fpp_name_ptrs: *const *const u8,
    type_bf_fpp_name_lens: *const i64,
    type_bf_fpp_vals: *const f64,
    type_bf_fpp_count: i64,
    type_bf_ndv_name_ptrs: *const *const u8,
    type_bf_ndv_name_lens: *const i64,
    type_bf_ndv_vals: *const i64,
    type_bf_ndv_count: i64,
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

    let field_names = str_array_from_raw(field_name_ptrs, field_name_lens, field_count)
        .map_err(|e| format!("parquet_on_settings_update field_names: {}", e))?;
    let field_encodings = str_array_from_raw(field_encoding_ptrs, field_encoding_lens, field_count)
        .map_err(|e| format!("parquet_on_settings_update field_encodings: {}", e))?;
    let field_compression_names = str_array_from_raw(field_compression_name_ptrs, field_compression_name_lens, field_compression_count)
        .map_err(|e| format!("parquet_on_settings_update field_compression_names: {}", e))?;
    let field_compressions = str_array_from_raw(field_compression_value_ptrs, field_compression_value_lens, field_compression_count)
        .map_err(|e| format!("parquet_on_settings_update field_compressions: {}", e))?;

    let type_encoding_names = str_array_from_raw(type_encoding_name_ptrs, type_encoding_name_lens, type_encoding_count)
        .map_err(|e| format!("parquet_on_settings_update type_encoding_names: {}", e))?;
    let type_encodings = str_array_from_raw(type_encoding_value_ptrs, type_encoding_value_lens, type_encoding_count)
        .map_err(|e| format!("parquet_on_settings_update type_encodings: {}", e))?;
    let type_compression_names = str_array_from_raw(type_compression_name_ptrs, type_compression_name_lens, type_compression_count)
        .map_err(|e| format!("parquet_on_settings_update type_compression_names: {}", e))?;
    let type_compressions = str_array_from_raw(type_compression_value_ptrs, type_compression_value_lens, type_compression_count)
        .map_err(|e| format!("parquet_on_settings_update type_compressions: {}", e))?;

    // Parse per-field bloom filter arrays
    let bf_enabled_names = str_array_from_raw(bf_enabled_name_ptrs, bf_enabled_name_lens, bf_enabled_count)
        .map_err(|e| format!("parquet_on_settings_update bf_enabled_names: {}", e))?;

    let field_configs = {
        let mut map = std::collections::HashMap::new();
        for (name, encoding) in field_names.into_iter().zip(field_encodings.into_iter()) {
            map.insert(name, FieldConfig { encoding_type: Some(encoding), ..Default::default() });
        }
        for (name, compression) in field_compression_names.into_iter().zip(field_compressions.into_iter()) {
            map.entry(name)
               .and_modify(|fc| fc.compression_type = Some(compression.clone()))
               .or_insert(FieldConfig { compression_type: Some(compression), ..Default::default() });
        }
        for (i, name) in bf_enabled_names.into_iter().enumerate() {
            let val = *bf_enabled_vals.add(i) != 0;
            map.entry(name)
               .and_modify(|fc| fc.bloom_filter_enabled = Some(val))
               .or_insert(FieldConfig { bloom_filter_enabled: Some(val), ..Default::default() });
        }
        if map.is_empty() { None } else { Some(map) }
    };

    let type_encoding_configs: Option<std::collections::HashMap<String, String>> = {
        let map: std::collections::HashMap<_, _> = type_encoding_names.into_iter().zip(type_encodings.into_iter()).collect();
        if map.is_empty() { None } else { Some(map) }
    };
    let type_compression_configs: Option<std::collections::HashMap<String, String>> = {
        let map: std::collections::HashMap<_, _> = type_compression_names.into_iter().zip(type_compressions.into_iter()).collect();
        if map.is_empty() { None } else { Some(map) }
    };

    // Parse type-level bloom filter arrays
    let type_bf_enabled_names = str_array_from_raw(type_bf_enabled_name_ptrs, type_bf_enabled_name_lens, type_bf_enabled_count)
        .map_err(|e| format!("parquet_on_settings_update type_bf_enabled_names: {}", e))?;
    let type_bf_fpp_names = str_array_from_raw(type_bf_fpp_name_ptrs, type_bf_fpp_name_lens, type_bf_fpp_count)
        .map_err(|e| format!("parquet_on_settings_update type_bf_fpp_names: {}", e))?;
    let type_bf_ndv_names = str_array_from_raw(type_bf_ndv_name_ptrs, type_bf_ndv_name_lens, type_bf_ndv_count)
        .map_err(|e| format!("parquet_on_settings_update type_bf_ndv_names: {}", e))?;

    let type_bloom_filter_enabled: Option<std::collections::HashMap<String, bool>> = {
        let map: std::collections::HashMap<_, _> = type_bf_enabled_names.into_iter().enumerate()
            .map(|(i, name)| (name, *type_bf_enabled_vals.add(i) != 0)).collect();
        if map.is_empty() { None } else { Some(map) }
    };
    let type_bloom_filter_fpp: Option<std::collections::HashMap<String, f64>> = {
        let map: std::collections::HashMap<_, _> = type_bf_fpp_names.into_iter().enumerate()
            .map(|(i, name)| (name, *type_bf_fpp_vals.add(i))).collect();
        if map.is_empty() { None } else { Some(map) }
    };
    let type_bloom_filter_ndv: Option<std::collections::HashMap<String, u64>> = {
        let map: std::collections::HashMap<_, _> = type_bf_ndv_names.into_iter().enumerate()
            .map(|(i, name)| (name, *type_bf_ndv_vals.add(i) as u64)).collect();
        if map.is_empty() { None } else { Some(map) }
    };

    let config = NativeSettings {
        index_name: Some(index_name.clone()),
        compression_type,
        compression_level: opt_i32(compression_level),
        page_size_bytes: opt_usize(page_size_bytes),
        page_row_limit: opt_usize(page_row_limit),
        dict_size_bytes: opt_usize(dict_size_bytes),
        bloom_filter_enabled: opt_bool(bloom_filter_enabled),
        bloom_filter_fpp: opt_f64(bloom_filter_fpp),
        bloom_filter_ndv: opt_u64(bloom_filter_ndv),
        sort_in_memory_threshold_bytes: opt_u64(sort_in_memory_threshold_bytes),
        sort_batch_size: opt_usize(sort_batch_size),
        row_group_max_rows: opt_usize(row_group_max_rows),
        row_group_max_bytes: opt_usize(row_group_max_bytes),
        merge_batch_size: opt_usize(merge_batch_size),
        merge_rayon_threads: opt_usize(merge_rayon_threads),
        merge_io_threads: opt_usize(merge_io_threads),
        field_configs,
        type_encoding_configs,
        type_compression_configs,
        type_bloom_filter_enabled,
        type_bloom_filter_fpp,
        type_bloom_filter_ndv,
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
    output_writer_generation: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
    crc32_out: *mut i64,
    out_mapping_ptr: *mut i64,
    out_mapping_len: *mut i64,
    out_gen_keys_ptr: *mut i64,
    out_gen_offsets_ptr: *mut i64,
    out_gen_sizes_ptr: *mut i64,
    out_gen_count: *mut i64,
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

    let result = if sort_cols.is_empty() {
        merge::merge_unsorted(&input_files, output_path, index_name, output_writer_generation)
    } else {
        merge::merge_sorted(
            &input_files,
            output_path,
            index_name,
            &sort_cols,
            &reverse_flags,
            &nulls_first_flags,
            output_writer_generation,
        )
    }
    .map_err(|e| format!("{}", e))?;

    // Write Parquet file metadata to out-pointers.
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

    // Write row-ID mapping into out-pointers as heap-allocated arrays.
    // Java reads them and then calls parquet_free_merge_result to deallocate.
    let mapping = result.mapping.into_boxed_slice();
    *out_mapping_len = mapping.len() as i64;
    *out_mapping_ptr = Box::into_raw(mapping) as *mut i64 as i64;

    let count = result.gen_keys.len();
    let keys = result.gen_keys.into_boxed_slice();
    let offsets = result.gen_offsets.into_boxed_slice();
    let sizes = result.gen_sizes.into_boxed_slice();
    *out_gen_count = count as i64;
    *out_gen_keys_ptr = Box::into_raw(keys) as *mut i64 as i64;
    *out_gen_offsets_ptr = Box::into_raw(offsets) as *mut i32 as i64;
    *out_gen_sizes_ptr = Box::into_raw(sizes) as *mut i32 as i64;

    Ok(0)
}

/// Frees the heap-allocated arrays returned by `parquet_merge_files`.
#[no_mangle]
pub unsafe extern "C" fn parquet_free_merge_result(
    mapping_ptr: i64,
    mapping_len: i64,
    gen_keys_ptr: i64,
    gen_offsets_ptr: i64,
    gen_sizes_ptr: i64,
    gen_count: i64,
) {
    if mapping_ptr != 0 && mapping_len > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(mapping_ptr as *mut i64, mapping_len as usize));
    }
    let n = gen_count as usize;
    if gen_keys_ptr != 0 && n > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(gen_keys_ptr as *mut i64, n));
    }
    if gen_offsets_ptr != 0 && n > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(gen_offsets_ptr as *mut i32, n));
    }
    if gen_sizes_ptr != 0 && n > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(gen_sizes_ptr as *mut i32, n));
    }
}

// ---------------------------------------------------------------------------
// Parquet reader (for test verification)
// ---------------------------------------------------------------------------

/// Reads a parquet file and returns its contents as a JSON string.
/// Each row is a JSON object. The result is a JSON array of objects.
/// The JSON bytes are written into `out_buf`, actual length into `out_len`.
/// Returns 0 on success.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_read_as_json(
    file_ptr: *const u8,
    file_len: i64,
    out_buf: *mut u8,
    buf_capacity: i64,
    out_len: *mut i64,
) -> i64 {
    use arrow::array::Array;

    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_read_as_json: {}", e))?.to_string();

    let file = std::fs::File::open(&filename)
        .map_err(|e| format!("Failed to open {}: {}", filename, e))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("Failed to read parquet: {}", e))?;
    let reader = builder.with_batch_size(8192).build()
        .map_err(|e| format!("Failed to build reader: {}", e))?;

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("Read error: {}", e))?;
        let schema = batch.schema();
        for row_idx in 0..batch.num_rows() {
            let mut obj = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val = if col.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    match col.data_type() {
                        arrow::datatypes::DataType::Int32 => {
                            let arr = col.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
                            serde_json::Value::Number(arr.value(row_idx).into())
                        }
                        arrow::datatypes::DataType::Int64 => {
                            let arr = col.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
                            serde_json::Value::Number(arr.value(row_idx).into())
                        }
                        arrow::datatypes::DataType::Utf8 => {
                            let arr = col.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                            serde_json::Value::String(arr.value(row_idx).to_string())
                        }
                        arrow::datatypes::DataType::Boolean => {
                            let arr = col.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
                            serde_json::Value::Bool(arr.value(row_idx))
                        }
                        arrow::datatypes::DataType::Float64 => {
                            let arr = col.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
                            serde_json::json!(arr.value(row_idx))
                        }
                        _ => serde_json::Value::String(format!("<unsupported:{}>", col.data_type())),
                    }
                };
                obj.insert(field.name().clone(), val);
            }
            rows.push(serde_json::Value::Object(obj));
        }
    }

    let json_str = serde_json::to_string(&rows)
        .map_err(|e| format!("JSON serialization failed: {}", e))?;
    let bytes = json_str.as_bytes();
    if bytes.len() > buf_capacity as usize {
        return Err(format!("JSON output ({} bytes) exceeds buffer capacity ({})", bytes.len(), buf_capacity));
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, bytes.len());
    *out_len = bytes.len() as i64;
    Ok(0)
}

// ---------------------------------------------------------------------------
// Sort permutation memory management
// ---------------------------------------------------------------------------

/// Frees the heap-allocated row ID mapping array returned as part of `parquet_finalize_writer`.
#[no_mangle]
pub unsafe extern "C" fn parquet_free_row_id_mapping(
    mapping_ptr: i64,
    mapping_len: i64,
) {
    if mapping_ptr != 0 && mapping_len > 0 {
        let _ = Box::from_raw(slice::from_raw_parts_mut(mapping_ptr as *mut i64, mapping_len as usize));
    }
}

// ---------------------------------------------------------------------------
// Parquet file analysis
// ---------------------------------------------------------------------------

/// Analyzes a parquet file and returns detailed metadata as JSON including
/// row group information, column statistics, compression, encodings,
/// bloom filter info, sort order, page-level stats, and footer size.
/// The JSON bytes are written into `out_buf`, actual length into `out_len`.
/// Returns 0 on success.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_analyze_file(
    file_ptr: *const u8,
    file_len: i64,
    out_buf: *mut u8,
    buf_capacity: i64,
    out_len: *mut i64,
) -> i64 {
    use parquet::file::metadata::ParquetMetaDataReader;

    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_analyze_file: {}", e))?.to_string();

    let file = std::fs::File::open(&filename)
        .map_err(|e| format!("Failed to open {}: {}", filename, e))?;
    let file_size = file.metadata()
        .map(|m| m.len() as i64)
        .unwrap_or(-1);

    // Use ParquetMetaDataReader to load page index (column_index + offset_index)
    let metadata = ParquetMetaDataReader::new()
        .with_page_indexes(true)
        .parse_and_finish(&file)
        .map_err(|e| format!("Failed to read parquet metadata: {}", e))?;
    let file_meta = metadata.file_metadata();

    // Compute footer size: file_size - (data start + total data)
    let footer_size = if metadata.num_row_groups() > 0 {
        let first_rg = metadata.row_group(0);
        let total_data: i64 = (0..metadata.num_row_groups())
            .map(|i| metadata.row_group(i).compressed_size())
            .sum();
        let first_data_offset = first_rg.column(0).data_page_offset();
        file_size - (first_data_offset + total_data)
    } else {
        -1
    };

    // Extract sorting columns from first row group (file-level sort order)
    let sorting_columns = metadata.row_group(0).sorting_columns().map(|cols| {
        let rg = metadata.row_group(0);
        cols.iter().map(|sc| {
            let col_name = if (sc.column_idx as usize) < rg.num_columns() {
                rg.column(sc.column_idx as usize).column_path().to_string()
            } else {
                format!("column_{}", sc.column_idx)
            };
            serde_json::json!({
                "column_idx": sc.column_idx,
                "column_name": col_name,
                "descending": sc.descending,
                "nulls_first": sc.nulls_first,
            })
        }).collect::<Vec<_>>()
    });

    // Page-level indexes
    let col_index = metadata.column_index();
    let off_index = metadata.offset_index();

    let mut row_groups = Vec::new();
    for rg_idx in 0..metadata.num_row_groups() {
        let rg = metadata.row_group(rg_idx);
        let mut columns = Vec::new();
        for col_idx in 0..rg.num_columns() {
            let col = rg.column(col_idx);

            // Column statistics (min/max/null_count/distinct_count)
            let stats = col.statistics().map(|s| {
                let min_val = s.min_bytes_opt().map(|b| format_stat_bytes(b, col.column_type()));
                let max_val = s.max_bytes_opt().map(|b| format_stat_bytes(b, col.column_type()));
                serde_json::json!({
                    "min": min_val,
                    "max": max_val,
                    "null_count": s.null_count_opt(),
                    "distinct_count": s.distinct_count_opt(),
                    "min_is_exact": s.min_is_exact(),
                    "max_is_exact": s.max_is_exact(),
                })
            });

            // Bloom filter info
            let has_bloom_filter = col.bloom_filter_offset().is_some();
            let bloom_filter_size = col.bloom_filter_length().unwrap_or(0) as i64;

            // Page-level stats for this column
            let page_stats = build_page_stats(col_index, off_index, rg_idx, col_idx, col.column_type());

            let null_count = col.statistics()
                .and_then(|s| s.null_count_opt())
                .map(|n| n as i64)
                .unwrap_or(-1);

            columns.push(serde_json::json!({
                "name": col.column_path().to_string(),
                "type": format!("{:?}", col.column_type()),
                "compression": format!("{:?}", col.compression()),
                "encodings": col.encodings().map(|e| format!("{:?}", e)).collect::<Vec<_>>(),
                "compressed_bytes": col.compressed_size(),
                "uncompressed_bytes": col.uncompressed_size(),
                "null_count": null_count,
                "num_values": col.num_values(),
                "has_bloom_filter": has_bloom_filter,
                "bloom_filter_size": bloom_filter_size,
                "stats": stats,
                "page_stats": page_stats,
            }));
        }

        // Per-row-group sorting columns
        let rg_sorting = rg.sorting_columns().map(|cols| {
            cols.iter().map(|sc| {
                let col_name = if (sc.column_idx as usize) < rg.num_columns() {
                    rg.column(sc.column_idx as usize).column_path().to_string()
                } else {
                    format!("column_{}", sc.column_idx)
                };
                serde_json::json!({
                    "column_idx": sc.column_idx,
                    "column_name": col_name,
                    "descending": sc.descending,
                    "nulls_first": sc.nulls_first,
                })
            }).collect::<Vec<_>>()
        });

        row_groups.push(serde_json::json!({
            "ordinal": rg_idx,
            "num_rows": rg.num_rows(),
            "total_compressed_bytes": rg.compressed_size(),
            "total_uncompressed_bytes": rg.total_byte_size(),
            "sorting_columns": rg_sorting,
            "columns": columns,
        }));
    }

    let result = serde_json::json!({
        "num_row_groups": metadata.num_row_groups(),
        "num_rows": file_meta.num_rows(),
        "version": file_meta.version(),
        "created_by": file_meta.created_by().unwrap_or("unknown"),
        "footer_size": footer_size,
        "sorting_columns": sorting_columns,
        "row_groups": row_groups,
    });

    let json_str = serde_json::to_string(&result)
        .map_err(|e| format!("JSON serialization failed: {}", e))?;
    let bytes = json_str.as_bytes();
    if bytes.len() > buf_capacity as usize {
        return Err(format!("JSON output ({} bytes) exceeds buffer capacity ({})", bytes.len(), buf_capacity));
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, bytes.len());
    *out_len = bytes.len() as i64;
    Ok(0)
}

/// Formats raw statistics bytes into a human-readable string based on physical type.
fn format_stat_bytes(bytes: &[u8], physical_type: parquet::basic::Type) -> String {
    use parquet::basic::Type;
    match physical_type {
        Type::BOOLEAN => {
            if bytes.is_empty() { "null".to_string() }
            else { format!("{}", bytes[0] != 0) }
        }
        Type::INT32 => {
            if bytes.len() >= 4 {
                format!("{}", i32::from_le_bytes(bytes[..4].try_into().unwrap_or([0; 4])))
            } else { bytes_to_hex(bytes) }
        }
        Type::INT64 => {
            if bytes.len() >= 8 {
                format!("{}", i64::from_le_bytes(bytes[..8].try_into().unwrap_or([0; 8])))
            } else { bytes_to_hex(bytes) }
        }
        Type::FLOAT => {
            if bytes.len() >= 4 {
                format!("{}", f32::from_le_bytes(bytes[..4].try_into().unwrap_or([0; 4])))
            } else { bytes_to_hex(bytes) }
        }
        Type::DOUBLE => {
            if bytes.len() >= 8 {
                format!("{}", f64::from_le_bytes(bytes[..8].try_into().unwrap_or([0; 8])))
            } else { bytes_to_hex(bytes) }
        }
        Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => {
            String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| bytes_to_hex(bytes))
        }
        Type::INT96 => bytes_to_hex(bytes),
    }
}

/// Converts bytes to hex string without external dependency.
fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Builds page-level statistics for a column from column_index and offset_index.
fn build_page_stats(
    col_index: Option<&parquet::file::metadata::ParquetColumnIndex>,
    off_index: Option<&parquet::file::metadata::ParquetOffsetIndex>,
    rg_idx: usize,
    col_idx: usize,
    physical_type: parquet::basic::Type,
) -> Option<serde_json::Value> {
    use parquet::file::page_index::column_index::ColumnIndexMetaData;

    let rg_col_indexes = col_index?.get(rg_idx)?;
    let ci = rg_col_indexes.get(col_idx)?;
    let oi = off_index.and_then(|o| o.get(rg_idx)).and_then(|r| r.get(col_idx));

    let num_pages = ci.num_pages() as usize;
    if num_pages == 0 {
        return None;
    }

    let boundary_order = ci.get_boundary_order().map(|bo| format!("{:?}", bo))
        .unwrap_or_else(|| "UNORDERED".to_string());

    let pages: Vec<serde_json::Value> = (0..num_pages).map(|page_idx| {
        let min_val = format_page_min(ci, page_idx, physical_type);
        let max_val = format_page_max(ci, page_idx, physical_type);
        let null_count = ci.null_count(page_idx);

        let mut page_json = serde_json::json!({
            "page_idx": page_idx,
            "min": min_val,
            "max": max_val,
            "null_count": null_count,
        });

        if let Some(oi_meta) = oi {
            let locations = oi_meta.page_locations();
            if page_idx < locations.len() {
                let loc = &locations[page_idx];
                page_json["first_row_index"] = serde_json::json!(loc.first_row_index);
                page_json["compressed_size"] = serde_json::json!(loc.compressed_page_size);
            }
        }
        page_json
    }).collect();

    Some(serde_json::json!({
        "num_pages": num_pages,
        "boundary_order": boundary_order,
        "pages": pages,
    }))
}

/// Extracts the min value for a page from a ColumnIndexMetaData.
fn format_page_min(
    ci: &parquet::file::page_index::column_index::ColumnIndexMetaData,
    page_idx: usize,
    _physical_type: parquet::basic::Type,
) -> Option<String> {
    use parquet::file::page_index::column_index::ColumnIndexMetaData;
    match ci {
        ColumnIndexMetaData::NONE => None,
        ColumnIndexMetaData::BOOLEAN(idx) => idx.min_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::INT32(idx) => idx.min_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::INT64(idx) => idx.min_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::INT96(idx) => idx.min_value(page_idx).map(|v| format!("{:?}", v)),
        ColumnIndexMetaData::FLOAT(idx) => idx.min_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::DOUBLE(idx) => idx.min_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::BYTE_ARRAY(idx) => idx.min_value(page_idx).map(|b: &[u8]| {
            String::from_utf8(b.to_vec()).unwrap_or_else(|_| bytes_to_hex(b))
        }),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(idx) => idx.min_value(page_idx).map(|b: &[u8]| {
            String::from_utf8(b.to_vec()).unwrap_or_else(|_| bytes_to_hex(b))
        }),
    }
}

/// Extracts the max value for a page from a ColumnIndexMetaData.
fn format_page_max(
    ci: &parquet::file::page_index::column_index::ColumnIndexMetaData,
    page_idx: usize,
    _physical_type: parquet::basic::Type,
) -> Option<String> {
    use parquet::file::page_index::column_index::ColumnIndexMetaData;
    match ci {
        ColumnIndexMetaData::NONE => None,
        ColumnIndexMetaData::BOOLEAN(idx) => idx.max_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::INT32(idx) => idx.max_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::INT64(idx) => idx.max_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::INT96(idx) => idx.max_value(page_idx).map(|v| format!("{:?}", v)),
        ColumnIndexMetaData::FLOAT(idx) => idx.max_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::DOUBLE(idx) => idx.max_value(page_idx).map(|v| format!("{}", v)),
        ColumnIndexMetaData::BYTE_ARRAY(idx) => idx.max_value(page_idx).map(|b: &[u8]| {
            String::from_utf8(b.to_vec()).unwrap_or_else(|_| bytes_to_hex(b))
        }),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(idx) => idx.max_value(page_idx).map(|b: &[u8]| {
            String::from_utf8(b.to_vec()).unwrap_or_else(|_| bytes_to_hex(b))
        }),
    }
}
