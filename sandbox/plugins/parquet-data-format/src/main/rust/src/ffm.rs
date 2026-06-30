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
                    let mapping_bytes = len * std::mem::size_of::<i64>();
                    // Track mapping handoff to Java — Java holds until parquet_free_row_id_mapping
                    crate::memory::write_pool().grow(mapping_bytes);
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
    // Per-merge stats forwarded to the per-shard ParquetShardStatsTracker on the Java side.
    out_flush_and_sort_chunk_count: *mut i64,
    out_flush_and_sort_chunk_time_millis: *mut i64,
    out_row_id_mapping_max: *mut i64,
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
    let mapping_bytes = mapping.len() * std::mem::size_of::<i64>();
    // Track merge mapping handoff to Java — Java holds until parquet_free_merge_result
    crate::memory::merge_pool().grow(mapping_bytes);
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

    // Per-merge stats out-pointers — callers always pass valid pointers (matches existing convention).
    *out_flush_and_sort_chunk_count = result.flush_and_sort_chunk_count;
    *out_flush_and_sort_chunk_time_millis = result.flush_and_sort_chunk_time_millis;
    *out_row_id_mapping_max = result.row_id_mapping_max;

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
        let mapping_bytes = mapping_len as usize * std::mem::size_of::<i64>();
        // Java released merge mapping — free from pool
        crate::memory::merge_pool().shrink(mapping_bytes);
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
        let mapping_bytes = mapping_len as usize * std::mem::size_of::<i64>();
        // Java released write mapping — free from pool
        crate::memory::write_pool().shrink(mapping_bytes);
        let _ = Box::from_raw(slice::from_raw_parts_mut(mapping_ptr as *mut i64, mapping_len as usize));
    }
}

// ---------------------------------------------------------------------------
// Native runtime metrics
// ---------------------------------------------------------------------------

/// Collect a snapshot of native runtime stats for the parquet merge path.
///
/// The caller passes a buffer of 11 i64s. On success, writes the 11-stat snapshot in the order
/// documented in `ParquetNativeRuntimeStats.fromArray`. Returns 0 on success, or a negative
/// error pointer on failure (per FFM convention).
/// Returns 0 on success, negative error pointer on failure (per FFM convention).
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_collect_runtime_metrics(
    out_buf: *mut i64,
    out_len: i64,
) -> i64 {
    if out_buf.is_null() {
        return Err("parquet_collect_runtime_metrics: null out_buf".to_string());
    }
    if out_len < 17 {
        return Err(format!(
            "parquet_collect_runtime_metrics: out_len {} < 17",
            out_len
        ));
    }
    let s = crate::merge::metrics::collect();
    let arr: [i64; 17] = [
        s.rayon_configured_threads,
        s.rayon_merge_tasks_submitted,
        s.rayon_merge_tasks_started,
        s.rayon_merge_tasks_completed,
        s.rayon_merge_tasks_failed,
        s.rayon_merge_tasks_panicked,
        s.rayon_merge_wall_millis,
        s.tokio_num_workers,
        s.tokio_num_blocking_threads,
        s.tokio_active_tasks,
        s.tokio_global_queue_depth,
        s.tokio_blocking_queue_depth,
        s.tokio_local_queue_depth_total,
        s.tokio_polls_count_total,
        s.tokio_overflow_count_total,
        s.tokio_spawned_tasks_total,
        s.tokio_workers_busy_millis_total,
    ];
    std::ptr::copy_nonoverlapping(arr.as_ptr(), out_buf, 17);
    Ok(0)
}

// ---------------------------------------------------------------------------
// Memory pool management (Phase 1 stubs)
// ---------------------------------------------------------------------------

/// Initialize write and merge memory pool counters.
#[no_mangle]
pub extern "C" fn parquet_init_memory_pools(write_limit: i64, merge_limit: i64) {
    crate::memory::init_pools(write_limit as usize, merge_limit as usize);
}

/// Set write pool limit. Called by Java rebalancer via FFM.
#[no_mangle]
pub extern "C" fn parquet_set_write_pool_limit(new_limit: i64) {
    crate::memory::set_write_limit(new_limit as usize);
}

/// Set merge pool limit. Called by Java rebalancer via FFM.
#[no_mangle]
pub extern "C" fn parquet_set_merge_pool_limit(new_limit: i64) {
    crate::memory::set_merge_limit(new_limit as usize);
}

/// Get pool stats: writes 6 i64s to out_buf.
/// Layout: [write_limit, write_used, write_peak, merge_limit, merge_used, merge_peak]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_pool_stats(out_buf: *mut i64) {
    let stats = crate::memory::get_stats();
    for (i, val) in stats.iter().enumerate() {
        *out_buf.add(i) = *val as i64;
    }
}
