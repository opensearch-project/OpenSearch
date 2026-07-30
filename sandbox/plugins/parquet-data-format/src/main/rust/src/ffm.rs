/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for the Parquet data format: the writer/merge path plus the DocValues codec's
//! column-reader read path.
//!
//! Return convention: `>= 0` success, `< 0` error pointer (negate to get ptr,
//! call `native_error_message`/`native_error_free`).

use std::collections::HashMap;
use std::fs::File;
use std::slice;
use std::str;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, MutexGuard};

use lazy_static::lazy_static;
use native_bridge_common::{ffm_safe, log_debug};
use parquet::basic::Type as PhysicalType;
use parquet::column::reader::{ColumnReader, ColumnReaderImpl};
use parquet::data_type::DataType as ParquetDataType;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;

use crate::field_config::FieldConfig;
use crate::merge;
use crate::native_settings::NativeSettings;
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
unsafe fn bool_array_from_raw(vals: *const i64, count: i64) -> Vec<bool> {
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
        .map_err(|e| format!("parquet_create_writer file: {}", e))?
        .to_string();
    let index_name = str_from_raw(index_name_ptr, index_name_len)
        .map_err(|e| format!("parquet_create_writer index_name: {}", e))?
        .to_string();
    let sort_columns = str_array_from_raw(sort_ptrs, sort_lens, sort_count)
        .map_err(|e| format!("parquet_create_writer sort_columns: {}", e))?;
    let reverse_sorts = bool_array_from_raw(reverse_vals, reverse_count);
    let nulls_first = bool_array_from_raw(nulls_first_vals, nulls_first_count);

    NativeParquetWriter::create_writer(
        filename,
        index_name,
        schema_address,
        sort_columns,
        reverse_sorts,
        nulls_first,
        writer_generation,
    )
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
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_write: {}", e))?
        .to_string();
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
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_finalize_writer: {}", e))?
        .to_string();
    match NativeParquetWriter::finalize_writer(filename) {
        Ok(Some(result)) => {
            let fm = result.metadata.file_metadata();
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
            if !crc32_out.is_null() {
                *crc32_out = result.crc32 as i64;
            }
            if !num_row_groups_out.is_null() {
                *num_row_groups_out = result.metadata.num_row_groups() as i64;
            }

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
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_get_file_metadata: {}", e))?
        .to_string();
    let metadata = NativeParquetWriter::get_file_metadata(filename).map_err(|e| e.to_string())?;
    let fm = metadata.file_metadata();
    if !version_out.is_null() {
        *version_out = fm.version();
    }
    if !num_rows_out.is_null() {
        *num_rows_out = fm.num_rows();
    }
    if !num_row_groups_out.is_null() {
        *num_row_groups_out = metadata.num_row_groups() as i64;
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

    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_get_column_metadata: {}", e))?
        .to_string();
    let file = File::open(&filename).map_err(|e| format!("Failed to open file: {}", e))?;
    let reader =
        SerializedFileReader::new(file).map_err(|e| format!("Failed to read parquet: {}", e))?;
    let metadata = reader.metadata();

    if metadata.num_row_groups() == 0 {
        let json = "{}".to_string();
        let bytes = json.as_bytes();
        let n = bytes.len().min(out_buf_len as usize);
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n);
        if !out_len.is_null() {
            *out_len = n as i64;
        }
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
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            "\"{}\":{{\"encodings\":[{}],\"compression\":\"{}\",\"bloom_filter\":{}}}",
            col_name,
            encodings
                .iter()
                .map(|e| format!("\"{}\"", e))
                .collect::<Vec<_>>()
                .join(","),
            compression,
            has_bloom_filter
        ));
    }
    json.push('}');

    let bytes = json.as_bytes();
    let n = bytes.len().min(out_buf_len as usize);
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n);
    if !out_len.is_null() {
        *out_len = n as i64;
    }
    Ok(0)
}

#[no_mangle]
pub unsafe extern "C" fn parquet_get_filtered_native_bytes_used(
    prefix_ptr: *const u8,
    prefix_len: i64,
) -> i64 {
    let prefix = str_from_raw(prefix_ptr, prefix_len)
        .unwrap_or("")
        .to_string();
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
        .map_err(|e| format!("parquet_on_settings_update index_name: {}", e))?
        .to_string();

    let compression_type = if compression_type_ptr.is_null() || compression_type_len < 0 {
        None
    } else {
        Some(
            str_from_raw(compression_type_ptr, compression_type_len)
                .map_err(|e| format!("parquet_on_settings_update compression_type: {}", e))?
                .to_string(),
        )
    };

    fn opt_i32(v: i64) -> Option<i32> {
        if v < 0 {
            None
        } else {
            Some(v as i32)
        }
    }
    fn opt_usize(v: i64) -> Option<usize> {
        if v < 0 {
            None
        } else {
            Some(v as usize)
        }
    }
    fn opt_bool(v: i64) -> Option<bool> {
        if v < 0 {
            None
        } else {
            Some(v != 0)
        }
    }
    fn opt_f64(v: f64) -> Option<f64> {
        if v < 0.0 {
            None
        } else {
            Some(v)
        }
    }
    fn opt_u64(v: i64) -> Option<u64> {
        if v < 0 {
            None
        } else {
            Some(v as u64)
        }
    }

    let field_names = str_array_from_raw(field_name_ptrs, field_name_lens, field_count)
        .map_err(|e| format!("parquet_on_settings_update field_names: {}", e))?;
    let field_encodings = str_array_from_raw(field_encoding_ptrs, field_encoding_lens, field_count)
        .map_err(|e| format!("parquet_on_settings_update field_encodings: {}", e))?;
    let field_compression_names = str_array_from_raw(
        field_compression_name_ptrs,
        field_compression_name_lens,
        field_compression_count,
    )
    .map_err(|e| format!("parquet_on_settings_update field_compression_names: {}", e))?;
    let field_compressions = str_array_from_raw(
        field_compression_value_ptrs,
        field_compression_value_lens,
        field_compression_count,
    )
    .map_err(|e| format!("parquet_on_settings_update field_compressions: {}", e))?;

    let type_encoding_names = str_array_from_raw(
        type_encoding_name_ptrs,
        type_encoding_name_lens,
        type_encoding_count,
    )
    .map_err(|e| format!("parquet_on_settings_update type_encoding_names: {}", e))?;
    let type_encodings = str_array_from_raw(
        type_encoding_value_ptrs,
        type_encoding_value_lens,
        type_encoding_count,
    )
    .map_err(|e| format!("parquet_on_settings_update type_encodings: {}", e))?;
    let type_compression_names = str_array_from_raw(
        type_compression_name_ptrs,
        type_compression_name_lens,
        type_compression_count,
    )
    .map_err(|e| format!("parquet_on_settings_update type_compression_names: {}", e))?;
    let type_compressions = str_array_from_raw(
        type_compression_value_ptrs,
        type_compression_value_lens,
        type_compression_count,
    )
    .map_err(|e| format!("parquet_on_settings_update type_compressions: {}", e))?;

    // Parse per-field bloom filter arrays
    let bf_enabled_names =
        str_array_from_raw(bf_enabled_name_ptrs, bf_enabled_name_lens, bf_enabled_count)
            .map_err(|e| format!("parquet_on_settings_update bf_enabled_names: {}", e))?;

    let field_configs = {
        let mut map = std::collections::HashMap::new();
        for (name, encoding) in field_names.into_iter().zip(field_encodings.into_iter()) {
            map.insert(
                name,
                FieldConfig {
                    encoding_type: Some(encoding),
                    ..Default::default()
                },
            );
        }
        for (name, compression) in field_compression_names
            .into_iter()
            .zip(field_compressions.into_iter())
        {
            map.entry(name)
                .and_modify(|fc| fc.compression_type = Some(compression.clone()))
                .or_insert(FieldConfig {
                    compression_type: Some(compression),
                    ..Default::default()
                });
        }
        for (i, name) in bf_enabled_names.into_iter().enumerate() {
            let val = *bf_enabled_vals.add(i) != 0;
            map.entry(name)
                .and_modify(|fc| fc.bloom_filter_enabled = Some(val))
                .or_insert(FieldConfig {
                    bloom_filter_enabled: Some(val),
                    ..Default::default()
                });
        }
        if map.is_empty() {
            None
        } else {
            Some(map)
        }
    };

    let type_encoding_configs: Option<std::collections::HashMap<String, String>> = {
        let map: std::collections::HashMap<_, _> = type_encoding_names
            .into_iter()
            .zip(type_encodings.into_iter())
            .collect();
        if map.is_empty() {
            None
        } else {
            Some(map)
        }
    };
    let type_compression_configs: Option<std::collections::HashMap<String, String>> = {
        let map: std::collections::HashMap<_, _> = type_compression_names
            .into_iter()
            .zip(type_compressions.into_iter())
            .collect();
        if map.is_empty() {
            None
        } else {
            Some(map)
        }
    };

    // Parse type-level bloom filter arrays
    let type_bf_enabled_names = str_array_from_raw(
        type_bf_enabled_name_ptrs,
        type_bf_enabled_name_lens,
        type_bf_enabled_count,
    )
    .map_err(|e| format!("parquet_on_settings_update type_bf_enabled_names: {}", e))?;
    let type_bf_fpp_names = str_array_from_raw(
        type_bf_fpp_name_ptrs,
        type_bf_fpp_name_lens,
        type_bf_fpp_count,
    )
    .map_err(|e| format!("parquet_on_settings_update type_bf_fpp_names: {}", e))?;
    let type_bf_ndv_names = str_array_from_raw(
        type_bf_ndv_name_ptrs,
        type_bf_ndv_name_lens,
        type_bf_ndv_count,
    )
    .map_err(|e| format!("parquet_on_settings_update type_bf_ndv_names: {}", e))?;

    let type_bloom_filter_enabled: Option<std::collections::HashMap<String, bool>> = {
        let map: std::collections::HashMap<_, _> = type_bf_enabled_names
            .into_iter()
            .enumerate()
            .map(|(i, name)| (name, *type_bf_enabled_vals.add(i) != 0))
            .collect();
        if map.is_empty() {
            None
        } else {
            Some(map)
        }
    };
    let type_bloom_filter_fpp: Option<std::collections::HashMap<String, f64>> = {
        let map: std::collections::HashMap<_, _> = type_bf_fpp_names
            .into_iter()
            .enumerate()
            .map(|(i, name)| (name, *type_bf_fpp_vals.add(i)))
            .collect();
        if map.is_empty() {
            None
        } else {
            Some(map)
        }
    };
    let type_bloom_filter_ndv: Option<std::collections::HashMap<String, u64>> = {
        let map: std::collections::HashMap<_, _> = type_bf_ndv_names
            .into_iter()
            .enumerate()
            .map(|(i, name)| (name, *type_bf_ndv_vals.add(i) as u64))
            .collect();
        if map.is_empty() {
            None
        } else {
            Some(map)
        }
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
        .map_err(|e| format!("parquet_remove_settings: {}", e))?
        .to_string();
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
        merge::merge_unsorted(
            &input_files,
            output_path,
            index_name,
            output_writer_generation,
        )
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
    if !crc32_out.is_null() {
        *crc32_out = result.crc32 as i64;
    }

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
        let _ = Box::from_raw(slice::from_raw_parts_mut(
            mapping_ptr as *mut i64,
            mapping_len as usize,
        ));
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
        .map_err(|e| format!("parquet_read_as_json: {}", e))?
        .to_string();

    let file = std::fs::File::open(&filename)
        .map_err(|e| format!("Failed to open {}: {}", filename, e))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("Failed to read parquet: {}", e))?;
    let reader = builder
        .with_batch_size(8192)
        .build()
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
                            let arr = col
                                .as_any()
                                .downcast_ref::<arrow::array::Int32Array>()
                                .unwrap();
                            serde_json::Value::Number(arr.value(row_idx).into())
                        }
                        arrow::datatypes::DataType::Int64 => {
                            let arr = col
                                .as_any()
                                .downcast_ref::<arrow::array::Int64Array>()
                                .unwrap();
                            serde_json::Value::Number(arr.value(row_idx).into())
                        }
                        arrow::datatypes::DataType::Utf8 => {
                            let arr = col
                                .as_any()
                                .downcast_ref::<arrow::array::StringArray>()
                                .unwrap();
                            serde_json::Value::String(arr.value(row_idx).to_string())
                        }
                        arrow::datatypes::DataType::Boolean => {
                            let arr = col
                                .as_any()
                                .downcast_ref::<arrow::array::BooleanArray>()
                                .unwrap();
                            serde_json::Value::Bool(arr.value(row_idx))
                        }
                        arrow::datatypes::DataType::Float64 => {
                            let arr = col
                                .as_any()
                                .downcast_ref::<arrow::array::Float64Array>()
                                .unwrap();
                            serde_json::json!(arr.value(row_idx))
                        }
                        _ => {
                            serde_json::Value::String(format!("<unsupported:{}>", col.data_type()))
                        }
                    }
                };
                obj.insert(field.name().clone(), val);
            }
            rows.push(serde_json::Value::Object(obj));
        }
    }

    let json_str =
        serde_json::to_string(&rows).map_err(|e| format!("JSON serialization failed: {}", e))?;
    let bytes = json_str.as_bytes();
    if bytes.len() > buf_capacity as usize {
        return Err(format!(
            "JSON output ({} bytes) exceeds buffer capacity ({})",
            bytes.len(),
            buf_capacity
        ));
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
pub unsafe extern "C" fn parquet_free_row_id_mapping(mapping_ptr: i64, mapping_len: i64) {
    if mapping_ptr != 0 && mapping_len > 0 {
        let mapping_bytes = mapping_len as usize * std::mem::size_of::<i64>();
        // Java released write mapping — free from pool
        crate::memory::write_pool().shrink(mapping_bytes);
        let _ = Box::from_raw(slice::from_raw_parts_mut(
            mapping_ptr as *mut i64,
            mapping_len as usize,
        ));
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
pub unsafe extern "C" fn parquet_collect_runtime_metrics(out_buf: *mut i64, out_len: i64) -> i64 {
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

/// Register the over-commit decision callbacks (FFM upcall stubs from the Java allocator).
///
/// `decider(requested_bytes) -> 1|0` decides whether a full pool may over-commit; `releaser(bytes)`
/// is called with the granted byte count when the reservation is released. Because all native
/// modules share one cdylib (and thus one `native-bridge-common` instance), this single
/// registration covers every pool that uses `Reject`.
#[no_mangle]
pub extern "C" fn parquet_register_overcommit_callbacks(
    decider: native_bridge_common::memory_pool::OverCommitDecider,
    releaser: native_bridge_common::memory_pool::OverCommitReleaser,
) {
    native_bridge_common::memory_pool::set_overcommit_callbacks(decider, releaser);
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

// ===========================================================================
// Parquet column reader
// ---------------------------------------------------------------------------
//
// Read-only per-column random-access reader used by the Lucene
// `ParquetDocValuesProducer`. The reader exposes the column's physical values
// by row position so the Java side can materialise per-document doc values.
//
// Return convention (consistent with the rest of this file):
//   - `>= 0` success. For reads, `0` (`RC_OK`) means "value(s) written"; the
//     positive sentinel `RC_OVERFLOW` (1) means "caller buffer too small —
//     required sizes were written to the out-parameters, retry once".
//   - `< 0` error pointer (negate, then `native_error_message`/`native_error_free`).
//
// Overflow is a positive status, not a negative one: negative returns are all
// reserved for error pointers, so a small negative constant would be
// dereferenced as an error pointer. This mirrors `parquet_finalize_writer`,
// which already returns `Ok(1)` for "no writer".

/// Read succeeded; value(s) written to the caller buffers.
pub(crate) const RC_OK: i64 = 0;
/// A caller buffer was too small. Required sizes were written to the
/// out-parameters; the caller should grow its buffers and retry once.
pub(crate) const RC_OVERFLOW: i64 = 1;

/// `expected_type` discriminants exchanged with Java
/// (matches `ParquetPhysicalType` on the Java side).
const TYPE_INT32: i32 = 0;
const TYPE_INT64: i32 = 1;
const TYPE_FLOAT: i32 = 2;
const TYPE_DOUBLE: i32 = 3;
const TYPE_BOOL: i32 = 4;
const TYPE_BYTE_ARRAY: i32 = 5;

/// Per-column reader state, one instance per open handle in the registry. Owns the
/// file handle (via `SerializedFileReader`) and the row-group layout that maps a global
/// row position to a `(row_group, local_offset)` pair. See the per-field comments for
/// what each read function uses.
struct ColumnReaderState {
    reader: SerializedFileReader<File>,
    /// Leaf column index within the Parquet schema descriptor.
    leaf_idx: usize,
    /// Physical type of the column (validated against the caller's expectation).
    physical_type: PhysicalType,
    /// True when the column has a repetition level > 0 (multi-valued).
    repeated: bool,
    /// Max definition level of the column (0 = required; >0 = optional/nested).
    max_def_level: i16,
    /// Total number of rows (records) in the file.
    row_count: i64,
    /// Global row index of the first row in each row group.
    rg_first_row: Vec<i64>,
    /// Number of rows in each row group.
    rg_num_rows: Vec<i64>,
    /// Per-page layout (row-range jump table + page stats), ascending by
    /// `global_first_row`. Built once at `open()` from the Parquet OffsetIndex +
    /// ColumnIndex when present, else one entry per row group as a fallback.
    pages: Vec<PageEntry>,
    /// Decode cursor retained by `parquet_decode_page_at_row` across calls. `None`
    /// until the first page decode and whenever it has been invalidated (row-group
    /// change, backwards seek, or a decode error). The single-row readers
    /// (`parquet_read_value_at_row`, `parquet_read_repeated_at_row`) do not use it.
    cursor: Option<CursorState>,
    /// Pre-allocated scratch buffers for page decoding, reused across
    /// `parquet_decode_page_at_row` calls to avoid per-call heap allocation. The
    /// Vecs keep their capacity across `.clear()`, so only the first decode allocates.
    scratch: DecodeScratch,
}

/// Retained cursor for `parquet_decode_page_at_row`. Keeps the typed column
/// reader open across consecutive page decodes within the same row group, so an
/// ascending scan skips forward from the current position instead of reopening
/// the row group (which re-reads row-group metadata and the dictionary page) for
/// every page.
struct CursorState {
    /// The row group index this cursor was opened for.
    rg_idx: usize,
    /// The typed column reader, retained across pages within the same row group.
    col_reader: ColumnReader,
    /// Global row position: the next record read from this cursor would start at this row.
    position: i64,
}

/// Pre-allocated scratch buffers for page decoding. Kept on `ColumnReaderState` and cleared (not
/// reallocated) between calls, so steady-state decoding is allocation-free after the first miss.
struct DecodeScratch {
    /// Definition levels returned by `read_records` (used to derive per-row presence).
    def_levels: Vec<i16>,
    /// Dense non-null values read from the page. One typed buffer per primitive type; a decode
    /// uses only the buffer matching the column's type. Kept as separate Vecs (not one reused
    /// byte buffer) so decoding is type-safe with no transmute.
    values_i64: Vec<i64>,
    values_i32: Vec<i32>,
    values_f32: Vec<f32>,
    values_f64: Vec<f64>,
    values_bool: Vec<bool>,
}

impl DecodeScratch {
    fn new() -> Self {
        DecodeScratch {
            def_levels: Vec::new(),
            values_i64: Vec::new(),
            values_i32: Vec::new(),
            values_f32: Vec::new(),
            values_f64: Vec::new(),
            values_bool: Vec::new(),
        }
    }
}

/// One row-aligned page in the column (or one row group, in the no-page-index
/// fallback). All row indices are global (file-relative).
#[derive(Clone)]
struct PageEntry {
    /// Global index of the first row in the page.
    global_first_row: i64,
    /// Number of rows in the page.
    num_rows: i64,
    /// Byte offset of the page in the file. 0 when unknown.
    file_offset: i64,
    /// Compressed page size in bytes. 0 when unknown.
    compressed_size: i32,
    /// Number of nulls in the page. -1 when unknown.
    null_count: i64,
    /// Min value raw bits; meaningful only for numeric columns with a
    /// page index, else 0.
    min_long: i64,
    /// Max value raw bits; meaningful only for numeric columns with a
    /// page index, else 0.
    max_long: i64,
    /// Row group containing the page.
    rg_idx: usize,
    /// Index of the page's first row within its row group.
    local_first_row: i64,
}

/// Gets or builds the per-file metadata cache entry. First call for a given path parses the
/// footer + page index and pre-computes every column's page layout; subsequent calls (other
/// columns, other queries) get an Arc clone in O(1) — no re-parse, no per-column layout work.
fn get_or_build_file_metadata(filename: &str) -> Result<std::sync::Arc<FileMetadataCache>, String> {
    let mut cache = FILE_METADATA_CACHE
        .lock()
        .map_err(|_| "file metadata cache mutex poisoned".to_string())?;
    if let Some(entry) = cache.get(filename) {
        return Ok(std::sync::Arc::clone(entry));
    }

    // First access to this file: parse footer + page index.
    let file = File::open(filename).map_err(|e| format!("Failed to open '{}': {}", filename, e))?;
    let options = ReadOptionsBuilder::new().with_page_index().build();
    let reader = SerializedFileReader::new_with_options(file, options)
        .map_err(|e| format!("Failed to read parquet metadata '{}': {}", filename, e))?;

    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr_ptr();
    let row_count = metadata.file_metadata().num_rows();

    let n_rg = metadata.num_row_groups();
    let mut rg_first_row = Vec::with_capacity(n_rg);
    let mut rg_num_rows = Vec::with_capacity(n_rg);
    let mut acc = 0i64;
    for i in 0..n_rg {
        let rn = metadata.row_group(i).num_rows();
        rg_first_row.push(acc);
        rg_num_rows.push(rn);
        acc += rn;
    }

    // Pre-compute per-column descriptors.
    let columns: Vec<_> = (0..schema.num_columns())
        .map(|i| {
            let d = schema.column(i);
            (i, d.physical_type(), d.max_rep_level(), d.max_def_level())
        })
        .collect();

    // Pre-compute page layouts for ALL columns (small per column; avoids per-column re-parse).
    let mut column_pages = HashMap::new();
    for &(leaf_idx, phys, _, _) in &columns {
        let pages = build_page_layout(metadata, leaf_idx, phys, &rg_first_row, &rg_num_rows);
        column_pages.insert(leaf_idx, pages);
    }

    let entry = std::sync::Arc::new(FileMetadataCache {
        schema,
        row_count,
        rg_first_row,
        rg_num_rows,
        column_pages,
    });
    cache.insert(filename.to_string(), std::sync::Arc::clone(&entry));
    Ok(entry)
}

impl ColumnReaderState {
    fn open(filename: &str, column: &str, expected_type: i32) -> Result<ColumnReaderState, String> {
        // Use the node-level metadata cache — first call parses; subsequent calls are O(1).
        let fmc = get_or_build_file_metadata(filename)?;

        // Resolve column from cached schema descriptor (no file I/O, no re-parse).
        let mut found: Option<(usize, PhysicalType, i16, i16)> = None;
        for i in 0..fmc.schema.num_columns() {
            let descr = fmc.schema.column(i);
            if descr.name() == column || descr.path().string() == column {
                found = Some((i, descr.physical_type(), descr.max_rep_level(), descr.max_def_level()));
                break;
            }
        }
        let (leaf_idx, phys, max_rep, max_def) = found.ok_or_else(|| {
            format!("Column '{}' not found in parquet file '{}'", column, filename)
        })?;

        let actual = physical_type_code(phys);
        if actual != expected_type {
            return Err(format!(
                "Column '{}' physical type mismatch in '{}': expected type code {}, found {:?} (code {})",
                column, filename, expected_type, phys, actual
            ));
        }

        // Get the pre-computed page layout from the cache.
        let pages = fmc.column_pages.get(&leaf_idx)
            .cloned()
            .unwrap_or_default();

        // Open a file handle for this column reader WITH the page index loaded: the retained
        // cursor's skip_records uses the page index internally for efficient page-hopping. The
        // per-file metadata cache above saves the JAVA-FACING costs (schema resolution, page
        // layout computation, ColumnPageIndex FFM marshal) — this reader's page-index load is
        // cheap (already in OS page cache from the first parse) and necessary for decode perf.
        //
        // TODO(consider optimisation): this re-opens the file and re-reads the footer, which
        // get_or_build_file_metadata already parsed above. Only the cheap footer read + syscall are
        // duplicated (the page-index parse is cached, and the bytes are warm), but the reader could
        // be constructed from the already-parsed ParquetMetaData to skip the second footer read.
        let file = File::open(filename).map_err(|e| format!("Failed to open '{}': {}", filename, e))?;
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(file, options)
            .map_err(|e| format!("Failed to read parquet '{}': {}", filename, e))?;

        Ok(ColumnReaderState {
            reader,
            leaf_idx,
            physical_type: phys,
            repeated: max_rep > 0,
            max_def_level: max_def,
            row_count: fmc.row_count,
            rg_first_row: fmc.rg_first_row.clone(),
            rg_num_rows: fmc.rg_num_rows.clone(),
            pages,
            cursor: None,
            scratch: DecodeScratch::new(),
        })
    }

    /// Translate a global row position into `(row_group_index, local_offset)`.
    ///
    /// TODO(consider optimisation): linear scan over row groups. Row groups are few (tens) and this
    /// is only on the single-row read path (not the page-decode aggregation path), so it is low
    /// priority, but it could be a binary search over `rg_first_row` like `page_for_row`.
    fn locate(&self, row: i64) -> Result<(usize, i64), String> {
        for i in 0..self.rg_first_row.len() {
            let start = self.rg_first_row[i];
            let end = start + self.rg_num_rows[i];
            if row >= start && row < end {
                return Ok((i, row - start));
            }
        }
        Err(format!("Row {} not found in any row group (row count {})", row, self.row_count))
    }

    /// Find the index of the page containing global row `row` (binary search over
    /// the ascending page layout).
    fn page_for_row(&self, row: i64) -> Result<usize, String> {
        // partition_point finds the first page whose global_first_row > row; the
        // page we want is the one immediately before it.
        let p = self.pages.partition_point(|e| e.global_first_row <= row);
        if p == 0 {
            return Err(format!("Row {} precedes the first page (row count {})", row, self.row_count));
        }
        let idx = p - 1;
        let entry = &self.pages[idx];
        if row >= entry.global_first_row && row < entry.global_first_row + entry.num_rows {
            Ok(idx)
        } else {
            Err(format!("Row {} not found in any page (row count {})", row, self.row_count))
        }
    }
}

/// Builds the per-page layout for a column. Prefers the Parquet OffsetIndex +
/// ColumnIndex (true page granularity); falls back to one entry per row group
/// when the file has no page index.
fn build_page_layout(
    metadata: &parquet::file::metadata::ParquetMetaData,
    leaf_idx: usize,
    phys: PhysicalType,
    rg_first_row: &[i64],
    rg_num_rows: &[i64],
) -> Vec<PageEntry> {
    let n_rg = metadata.num_row_groups();
    let offset_index = metadata.offset_index();
    let column_index = metadata.column_index();

    let mut pages: Vec<PageEntry> = Vec::new();

    for rg in 0..n_rg {
        let oi_pages = offset_index
            .and_then(|oi| oi.get(rg))
            .and_then(|cols| cols.get(leaf_idx));
        let ci = column_index
            .and_then(|ci| ci.get(rg))
            .and_then(|cols| cols.get(leaf_idx));

        match oi_pages {
            Some(oi) => {
                let locations = oi.page_locations();
                let rg_rows = rg_num_rows[rg];
                for (p, loc) in locations.iter().enumerate() {
                    let local_first = loc.first_row_index;
                    let next_local = if p + 1 < locations.len() {
                        locations[p + 1].first_row_index
                    } else {
                        rg_rows
                    };
                    let num_rows = next_local - local_first;
                    let null_count = ci.and_then(|c| c.null_count(p)).unwrap_or(-1);
                    let (min_long, max_long) = ci
                        .map(|c| page_min_max(c, p, phys))
                        .unwrap_or(MINMAX_UNKNOWN);
                    pages.push(PageEntry {
                        global_first_row: rg_first_row[rg] + local_first,
                        num_rows,
                        file_offset: loc.offset,
                        compressed_size: loc.compressed_page_size,
                        null_count,
                        min_long,
                        max_long,
                        rg_idx: rg,
                        local_first_row: local_first,
                    });
                }
            }
            None => {
                // Fallback: treat the whole row group as a single "page".
                let cc = metadata.row_group(rg).column(leaf_idx);
                let null_count = cc
                    .statistics()
                    .and_then(|s| s.null_count_opt())
                    .map(|n| n as i64)
                    .unwrap_or(-1);
                let compressed = cc.compressed_size().min(i32::MAX as i64) as i32;
                pages.push(PageEntry {
                    global_first_row: rg_first_row[rg],
                    num_rows: rg_num_rows[rg],
                    file_offset: cc.data_page_offset(),
                    compressed_size: compressed,
                    null_count,
                    min_long: MINMAX_UNKNOWN.0,
                    max_long: MINMAX_UNKNOWN.1,
                    rg_idx: rg,
                    local_first_row: 0,
                });
            }
        }
    }

    pages
}

/// Sentinel pair meaning "min/max unknown": the widest possible range, so a consumer making
/// skip decisions (the DocValuesSkipper) can never wrongly exclude the page. Distinguishable
/// from real data only in that real data spanning the full i64 range behaves identically —
/// which is exactly the safe behavior.
const MINMAX_UNKNOWN: (i64, i64) = (i64::MIN, i64::MAX);

/// Extracts the per-page min/max as raw i64 bits from a typed ColumnIndex.
/// Returns [`MINMAX_UNKNOWN`] for byte-array/unsupported columns (binary min/max is not
/// exchanged as i64) and for pages whose stats are absent.
fn page_min_max(ci: &ColumnIndexMetaData, idx: usize, _phys: PhysicalType) -> (i64, i64) {
    // A stat may be absent per page (stats disabled, or an all-null page). Report the unknown
    // sentinel rather than 0 — 0 is indistinguishable from a real value and would let a
    // skipper wrongly exclude pages.
    match ci {
        ColumnIndexMetaData::INT32(p) => match (p.min_value(idx), p.max_value(idx)) {
            (Some(min), Some(max)) => (*min as i64, *max as i64),
            _ => MINMAX_UNKNOWN,
        },
        ColumnIndexMetaData::INT64(p) => match (p.min_value(idx), p.max_value(idx)) {
            (Some(min), Some(max)) => (*min, *max),
            _ => MINMAX_UNKNOWN,
        },
        ColumnIndexMetaData::FLOAT(p) => match (p.min_value(idx), p.max_value(idx)) {
            (Some(min), Some(max)) => (min.to_bits() as i64, max.to_bits() as i64),
            _ => MINMAX_UNKNOWN,
        },
        ColumnIndexMetaData::DOUBLE(p) => match (p.min_value(idx), p.max_value(idx)) {
            (Some(min), Some(max)) => (min.to_bits() as i64, max.to_bits() as i64),
            _ => MINMAX_UNKNOWN,
        },
        ColumnIndexMetaData::BOOLEAN(p) => match (p.min_value(idx), p.max_value(idx)) {
            (Some(min), Some(max)) => (if *min { 1 } else { 0 }, if *max { 1 } else { 0 }),
            _ => MINMAX_UNKNOWN,
        },
        _ => MINMAX_UNKNOWN,
    }
}

/// Maps a Parquet physical type to the Java-facing `expected_type` discriminant.
/// Returns `-1` for unsupported physical types (e.g. INT96), which can never
/// match a valid expectation and therefore surfaces as a clear mismatch error.
fn physical_type_code(t: PhysicalType) -> i32 {
    match t {
        PhysicalType::BOOLEAN => TYPE_BOOL,
        PhysicalType::INT32 => TYPE_INT32,
        PhysicalType::INT64 => TYPE_INT64,
        PhysicalType::FLOAT => TYPE_FLOAT,
        PhysicalType::DOUBLE => TYPE_DOUBLE,
        PhysicalType::BYTE_ARRAY => TYPE_BYTE_ARRAY,
        PhysicalType::FIXED_LEN_BYTE_ARRAY => TYPE_BYTE_ARRAY,
        PhysicalType::INT96 => -1,
    }
}

/// Reads exactly one record (after skipping `skip` records) from a typed column
/// reader, returning that record's non-null values. For a single-valued column
/// the result holds 0 (null) or 1 value; for a repeated column it holds all the
/// values of the record.
fn read_record_values<T: ParquetDataType>(
    r: &mut ColumnReaderImpl<T>,
    skip: usize,
) -> Result<Vec<T::T>, String> {
    if skip > 0 {
        let skipped = r.skip_records(skip).map_err(|e| e.to_string())?;
        if skipped < skip {
            return Err(format!("requested skip of {} records but only {} available", skip, skipped));
        }
    }
    let mut def_levels: Vec<i16> = Vec::new();
    let mut rep_levels: Vec<i16> = Vec::new();
    let mut values: Vec<T::T> = Vec::new();
    r.read_records(1, Some(&mut def_levels), Some(&mut rep_levels), &mut values)
        .map_err(|e| e.to_string())?;
    Ok(values)
}

/// Cached per-file metadata: footer + page-index parse results shared across all column readers
/// for the same file. Parquet files are immutable (changed data = new file = new path), so entries
/// never need invalidation — they can only be evicted when a file is deleted (shard close). This
/// is the `.dvm` equivalent: parsed once at first column open, then every subsequent query reuses
/// it without FFM/IO, at node lifetime scope.
struct FileMetadataCache {
    /// Schema descriptor pointer (for column lookup).
    schema: std::sync::Arc<parquet::schema::types::SchemaDescriptor>,
    /// Number of rows in the file.
    row_count: i64,
    /// Per-row-group: global first row.
    rg_first_row: Vec<i64>,
    /// Per-row-group: number of rows.
    rg_num_rows: Vec<i64>,
    /// Per-column page layouts, keyed by leaf column index. Computed once per file.
    column_pages: HashMap<usize, Vec<PageEntry>>,
}

lazy_static! {
    /// Node-level file metadata cache. Keyed by absolute file path. Entries are never
    /// invalidated (immutable files) — evicted only on explicit `parquet_evict_file_metadata`.
    static ref FILE_METADATA_CACHE: Mutex<HashMap<String, std::sync::Arc<FileMetadataCache>>> = Mutex::new(HashMap::new());

    /// Per-handle registry of open column readers, keyed by an opaque i64 handle.
    /// Mirrors the writer-side handle pattern; serialised behind a single mutex
    /// since column readers are not shared across threads.
    static ref COLUMN_READERS: Mutex<HashMap<i64, ColumnReaderState>> = Mutex::new(HashMap::new());
}

/// Monotonic handle allocator. Always `>= 0`, so a returned handle is never
/// confused with the `< 0` error-pointer convention.
static NEXT_COLUMN_READER_HANDLE: AtomicI64 = AtomicI64::new(0);

/// Locks the column-reader registry, converting a poisoned mutex into a normal
/// FFM error instead of propagating the panic.
fn lock_readers<'a>() -> Result<MutexGuard<'a, HashMap<i64, ColumnReaderState>>, String> {
    COLUMN_READERS
        .lock()
        .map_err(|_| "column reader registry mutex poisoned".to_string())
}

/// Evicts a file's cached metadata (footer + page index) from the node-level cache, e.g. on
/// shard close or file deletion. No-op if the file isn't cached. Returns 0 on success.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_evict_file_metadata(
    file_ptr: *const u8,
    file_len: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_evict_file_metadata: {}", e))?
        .to_string();
    FILE_METADATA_CACHE
        .lock()
        .map_err(|_| "file metadata cache mutex poisoned".to_string())?
        .remove(&filename);
    Ok(RC_OK)
}

/// Opens a per-column reader over `file` for `col`, validating that the column
/// exists and its physical type matches `expected_type`
/// (0=INT32,1=INT64,2=FLOAT,3=DOUBLE,4=BOOL,5=BYTE_ARRAY).
///
/// Returns `>= 0` handle id on success, `< 0` negated error pointer on failure.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_open_column_reader(
    file_ptr: *const u8,
    file_len: i64,
    col_ptr: *const u8,
    col_len: i64,
    expected_type: i32,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_open_column_reader file: {}", e))?
        .to_string();
    let column = str_from_raw(col_ptr, col_len)
        .map_err(|e| format!("parquet_open_column_reader column: {}", e))?
        .to_string();

    let state = ColumnReaderState::open(&filename, &column, expected_type)?;

    let handle = NEXT_COLUMN_READER_HANDLE.fetch_add(1, Ordering::SeqCst);
    lock_readers()?.insert(handle, state);
    log_debug!(
        "parquet_open_column_reader: file={}, column={}, handle={}",
        filename, column, handle
    );
    Ok(handle)
}

/// Closes a column reader handle and releases its file handle and buffers.
/// Returns `0` on success, a `< 0` error pointer if the handle is unknown.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_close_column_reader(handle: i64) -> i64 {
    match lock_readers()?.remove(&handle) {
        Some(_) => {
            log_debug!("parquet_close_column_reader: handle={}", handle);
            Ok(RC_OK)
        }
        None => Err(format!("parquet_close_column_reader: unknown handle {}", handle)),
    }
}

/// Debug-only symbol: returns the number of currently open column-reader
/// handles. Used by Property 7 (native handle non-leakage). Never errors;
/// recovers from a poisoned mutex rather than panicking.
#[no_mangle]
pub unsafe extern "C" fn parquet_open_column_reader_count() -> i64 {
    match COLUMN_READERS.lock() {
        Ok(guard) => guard.len() as i64,
        Err(poisoned) => poisoned.into_inner().len() as i64,
    }
}

/// Reads the single value at `row` for a single-valued column.
///
/// On success writes:
///   - `out_present` = 1 if the row has a value, 0 if null/absent
///   - `out_long`    = the value's raw bits for primitive columns:
///                       INT32 sign-extended to i64; INT64 verbatim;
///                       FLOAT  = `f32::to_bits` (zero-extended);
///                       DOUBLE = `f64::to_bits`;
///                       BOOL   = 0 or 1
///   - for BYTE_ARRAY columns: the value bytes are copied into `out_buf` and
///     `out_len` is set to the byte length (or -1 when the value is null).
///
/// Returns a `< 0` error pointer naming the row when `row >= row_count`, when
/// the handle is unknown, or when `out_buf` is too small for a BYTE_ARRAY value
/// (in which case `out_len` is set to the required length first).
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_read_value_at_row(
    handle: i64,
    row: i64,
    out_present: *mut i64,
    out_long: *mut i64,
    out_buf: *mut u8,
    out_buf_cap: i64,
    out_len: *mut i64,
) -> i64 {
    let mut guard = lock_readers()?;
    let state = guard
        .get_mut(&handle)
        .ok_or_else(|| format!("parquet_read_value_at_row: unknown handle {}", handle))?;

    if row < 0 {
        return Err(format!("parquet_read_value_at_row: negative row {}", row));
    }
    if row >= state.row_count {
        return Err(format!(
            "parquet_read_value_at_row: row {} out of range (row count {})",
            row, state.row_count
        ));
    }

    // Default outputs: absent value.
    if !out_present.is_null() {
        *out_present = 0;
    }
    if !out_long.is_null() {
        *out_long = 0;
    }
    if !out_len.is_null() {
        *out_len = -1;
    }

    let (rg_idx, local) = state.locate(row)?;
    let rg = state.reader.get_row_group(rg_idx).map_err(|e| e.to_string())?;
    let col = rg.get_column_reader(state.leaf_idx).map_err(|e| e.to_string())?;
    let local = local as usize;

    match col {
        ColumnReader::Int32ColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, *v as i64);
            }
        }
        ColumnReader::Int64ColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, *v);
            }
        }
        ColumnReader::FloatColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, v.to_bits() as i64);
            }
        }
        ColumnReader::DoubleColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, v.to_bits() as i64);
            }
        }
        ColumnReader::BoolColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                set_present(out_present, out_long, if *v { 1 } else { 0 });
            }
        }
        ColumnReader::ByteArrayColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                return write_bytes_value(v.data(), out_present, out_buf, out_buf_cap, out_len);
            }
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut r) => {
            if let Some(v) = read_record_values(&mut r, local)?.first() {
                return write_bytes_value(v.data(), out_present, out_buf, out_buf_cap, out_len);
            }
        }
        ColumnReader::Int96ColumnReader(_) => {
            return Err("parquet_read_value_at_row: INT96 columns are not supported".to_string());
        }
    }

    Ok(RC_OK)
}

/// Marks a primitive value present and stores its raw bits.
unsafe fn set_present(out_present: *mut i64, out_long: *mut i64, bits: i64) {
    if !out_present.is_null() {
        *out_present = 1;
    }
    if !out_long.is_null() {
        *out_long = bits;
    }
}

/// Copies a single BYTE_ARRAY value into the caller buffer. Sets `out_present=1`
/// and `out_len` to the byte length. Returns `RC_OVERFLOW` (after recording the
/// required length in `out_len`) when the value does not fit in `out_buf_cap`,
/// so the caller can grow its buffer and retry once.
unsafe fn write_bytes_value(
    bytes: &[u8],
    out_present: *mut i64,
    out_buf: *mut u8,
    out_buf_cap: i64,
    out_len: *mut i64,
) -> Result<i64, String> {
    if !out_present.is_null() {
        *out_present = 1;
    }
    let n = bytes.len();
    if !out_len.is_null() {
        *out_len = n as i64;
    }
    if (n as i64) > out_buf_cap || (n > 0 && out_buf.is_null()) {
        return Ok(RC_OVERFLOW);
    }
    if n > 0 {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_buf, n);
    }
    Ok(RC_OK)
}

/// Reads all values at `row` for a repeated (multi-valued) column.
///
/// Capacity contract: `out_long_cap` is the maximum element count for *both*
/// primitive and BYTE_ARRAY columns; `out_byte_offsets` (BYTE_ARRAY only) must
/// have capacity `out_long_cap + 1`.
///
/// On success (`RC_OK`):
///   - `out_count` = number of values at the row
///   - primitive columns: raw bits (see `parquet_read_value_at_row`) written to
///     `out_longs`
///   - BYTE_ARRAY columns: concatenated bytes in `out_byte_buf`, CSR offsets
///     (length `count + 1`) in `out_byte_offsets`
///
/// On `RC_OVERFLOW`: `out_count` holds the required element count. When the
/// element count fits but only the byte buffer is too small, the full CSR
/// offsets are still written so `out_byte_offsets[count]` reports the required
/// total byte size, enabling a single retry.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_read_repeated_at_row(
    handle: i64,
    row: i64,
    out_count: *mut i64,
    out_longs: *mut i64,
    out_long_cap: i64,
    out_byte_buf: *mut u8,
    out_byte_offsets: *mut i64,
    out_byte_buf_cap: i64,
) -> i64 {
    let mut guard = lock_readers()?;
    let state = guard
        .get_mut(&handle)
        .ok_or_else(|| format!("parquet_read_repeated_at_row: unknown handle {}", handle))?;

    if row < 0 {
        return Err(format!("parquet_read_repeated_at_row: negative row {}", row));
    }
    if row >= state.row_count {
        return Err(format!(
            "parquet_read_repeated_at_row: row {} out of range (row count {})",
            row, state.row_count
        ));
    }

    if !out_count.is_null() {
        *out_count = 0;
    }

    let (rg_idx, local) = state.locate(row)?;
    let rg = state.reader.get_row_group(rg_idx).map_err(|e| e.to_string())?;
    let col = rg.get_column_reader(state.leaf_idx).map_err(|e| e.to_string())?;
    let local = local as usize;

    match col {
        ColumnReader::Int32ColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().map(|v| *v as i64), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::Int64ColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().copied(), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::FloatColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().map(|v| v.to_bits() as i64), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::DoubleColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().map(|v| v.to_bits() as i64), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::BoolColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            write_primitive_repeated(vals.iter().map(|v| if *v { 1i64 } else { 0i64 }), vals.len(), out_count, out_longs, out_long_cap)
        }
        ColumnReader::ByteArrayColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            let slices: Vec<&[u8]> = vals.iter().map(|v| v.data()).collect();
            write_bytes_repeated(&slices, out_count, out_long_cap, out_byte_buf, out_byte_offsets, out_byte_buf_cap)
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut r) => {
            let vals = read_record_values(&mut r, local)?;
            let slices: Vec<&[u8]> = vals.iter().map(|v| v.data()).collect();
            write_bytes_repeated(&slices, out_count, out_long_cap, out_byte_buf, out_byte_offsets, out_byte_buf_cap)
        }
        ColumnReader::Int96ColumnReader(_) => {
            Err("parquet_read_repeated_at_row: INT96 columns are not supported".to_string())
        }
    }
}

/// Writes repeated primitive values to `out_longs`, or reports overflow.
unsafe fn write_primitive_repeated(
    values: impl Iterator<Item = i64>,
    count: usize,
    out_count: *mut i64,
    out_longs: *mut i64,
    out_long_cap: i64,
) -> Result<i64, String> {
    if !out_count.is_null() {
        *out_count = count as i64;
    }
    if (count as i64) > out_long_cap || out_longs.is_null() {
        return Ok(RC_OVERFLOW);
    }
    for (i, v) in values.enumerate() {
        *out_longs.add(i) = v;
    }
    Ok(RC_OK)
}

/// Writes repeated BYTE_ARRAY values (CSR layout) to the caller buffers, or
/// reports overflow with required sizes.
unsafe fn write_bytes_repeated(
    slices: &[&[u8]],
    out_count: *mut i64,
    out_long_cap: i64,
    out_byte_buf: *mut u8,
    out_byte_offsets: *mut i64,
    out_byte_buf_cap: i64,
) -> Result<i64, String> {
    let count = slices.len();
    let total_bytes: usize = slices.iter().map(|s| s.len()).sum();
    if !out_count.is_null() {
        *out_count = count as i64;
    }

    // Element-count overflow: cannot safely write offsets (capacity is count+1).
    if (count as i64) > out_long_cap {
        return Ok(RC_OVERFLOW);
    }

    // Element count fits: write the full CSR offsets so that, even on a byte
    // overflow, out_byte_offsets[count] == total_bytes reports the required size.
    if !out_byte_offsets.is_null() {
        let mut acc = 0i64;
        for (i, s) in slices.iter().enumerate() {
            *out_byte_offsets.add(i) = acc;
            acc += s.len() as i64;
        }
        *out_byte_offsets.add(count) = acc;
    }

    if (total_bytes as i64) > out_byte_buf_cap || (total_bytes > 0 && out_byte_buf.is_null()) {
        return Ok(RC_OVERFLOW);
    }

    let mut acc = 0usize;
    for s in slices {
        if !s.is_empty() {
            std::ptr::copy_nonoverlapping(s.as_ptr(), out_byte_buf.add(acc), s.len());
        }
        acc += s.len();
    }
    Ok(RC_OK)
}

// ---------------------------------------------------------------------------
// Page-index loader + page decoder
// ---------------------------------------------------------------------------
//
// Page-oriented reads used by the Java `ParquetColumnReader` to decode a whole
// page per call rather than a row per call:
//   - `parquet_get_column_num_pages`  — page count, so Java can pre-size buffers
//   - `parquet_get_column_page_index` — per-page row-range jump table + page stats
//   - `parquet_decode_page_at_row`    — decoded values + presence bitset for a page
//
// All row indices exchanged here are global (file-relative).

/// Returns the number of pages in the column (`>= 0`), or a `< 0` error pointer
/// for an unknown handle. Java reads this first to size the parallel arrays
/// passed to `parquet_get_column_page_index`.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_column_num_pages(handle: i64) -> i64 {
    let guard = lock_readers()?;
    let state = guard
        .get(&handle)
        .ok_or_else(|| format!("parquet_get_column_num_pages: unknown handle {}", handle))?;
    Ok(state.pages.len() as i64)
}

/// Writes the column's per-page row-range jump table and page statistics into
/// caller-provided parallel arrays, each of capacity `out_buf_capacity`
/// (= the page count from `parquet_get_column_num_pages`).
///
/// Arrays (length = page count):
///   - `out_first_row`       global index of the page's first row
///   - `out_file_offset`     byte offset of the page in the file (0 if unknown)
///   - `out_compressed_size` compressed page size in bytes (0 if unknown)
///   - `out_null_count`      nulls in the page, or -1 when unknown
///   - `out_min_long`        per-page min raw bits (numeric only; 0 otherwise)
///   - `out_max_long`        per-page max raw bits (numeric only; 0 otherwise)
///
/// `out_actual_pages` always receives the true page count. Returns `RC_OVERFLOW`
/// (a positive sentinel) without writing the arrays when `out_buf_capacity` is
/// smaller than the page count, so the caller can grow and retry. Returns a
/// `< 0` error pointer for an unknown handle.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_column_page_index(
    handle: i64,
    out_first_row: *mut i64,
    out_file_offset: *mut i64,
    out_compressed_size: *mut i32,
    out_null_count: *mut i64,
    out_min_long: *mut i64,
    out_max_long: *mut i64,
    out_buf_capacity: i64,
    out_actual_pages: *mut i64,
) -> i64 {
    let guard = lock_readers()?;
    let state = guard
        .get(&handle)
        .ok_or_else(|| format!("parquet_get_column_page_index: unknown handle {}", handle))?;

    let n = state.pages.len();
    if !out_actual_pages.is_null() {
        *out_actual_pages = n as i64;
    }
    if (n as i64) > out_buf_capacity {
        return Ok(RC_OVERFLOW);
    }

    for (i, e) in state.pages.iter().enumerate() {
        if !out_first_row.is_null() {
            *out_first_row.add(i) = e.global_first_row;
        }
        if !out_file_offset.is_null() {
            *out_file_offset.add(i) = e.file_offset;
        }
        if !out_compressed_size.is_null() {
            *out_compressed_size.add(i) = e.compressed_size;
        }
        if !out_null_count.is_null() {
            *out_null_count.add(i) = e.null_count;
        }
        if !out_min_long.is_null() {
            *out_min_long.add(i) = e.min_long;
        }
        if !out_max_long.is_null() {
            *out_max_long.add(i) = e.max_long;
        }
    }
    Ok(RC_OK)
}

/// Decodes one primitive page through a (possibly retained) typed column reader and writes
/// values + packed presence straight into the caller's out-buffers. `skip` is relative to the
/// reader's current position, NOT the row group start — the retained-cursor caller computes it
/// from the cursor position so a reused reader only skips forward the remaining distance.
#[allow(clippy::too_many_arguments)]
unsafe fn decode_primitive_page<T: ParquetDataType>(
    r: &mut ColumnReaderImpl<T>,
    skip: usize,
    num_rows: usize,
    max_def_level: i16,
    effective_null_count: i64,
    def_scratch: &mut Vec<i16>,
    val_scratch: &mut Vec<T::T>,
    to_bits: impl Fn(T::T) -> i64,
    out_value_buf: *mut u8,
    out_presence_bitset: *mut i64,
) -> Result<(), String>
where
    T::T: Copy,
{
    decode_page_records(r, skip, num_rows, def_scratch, val_scratch)?;
    pack_presence_from_def_levels(def_scratch, max_def_level, num_rows, out_presence_bitset);
    expand_to_outbuf(val_scratch, to_bits, effective_null_count, num_rows, out_value_buf, out_presence_bitset as *const i64);
    Ok(())
}

/// Decodes one page's worth of single-valued records, returning a per-row
/// presence flag (`true` = value present) and the dense list of non-null values
/// in row order. `skip` records are skipped first, then `num_rows` records are
/// read. Works for required columns (`max_def_level == 0`, all present) and
/// optional non-repeated columns.
fn decode_page_records<T: ParquetDataType>(
    r: &mut ColumnReaderImpl<T>,
    skip: usize,
    num_rows: usize,
    scratch_def: &mut Vec<i16>,
    scratch_vals: &mut Vec<T::T>,
) -> Result<(), String> {
    if skip > 0 {
        let skipped = r.skip_records(skip).map_err(|e| e.to_string())?;
        if skipped < skip {
            return Err(format!(
                "page decode: requested skip of {} records but only {} available",
                skip, skipped
            ));
        }
    }

    scratch_def.clear();
    scratch_vals.clear();
    scratch_def.reserve(num_rows.saturating_sub(scratch_def.capacity()));
    scratch_vals.reserve(num_rows.saturating_sub(scratch_vals.capacity()));

    let (records_read, _values_read, _levels_read) = r
        .read_records(num_rows, Some(scratch_def), None, scratch_vals)
        .map_err(|e| e.to_string())?;
    if records_read < num_rows {
        return Err(format!(
            "page decode: expected {} records but read {}",
            num_rows, records_read
        ));
    }
    Ok(())
}

/// Packs definition levels directly into a little-endian `long[]` bitset in the
/// caller's out-buffer, with bit `i` set when `def_levels[i] == max_def_level`.
/// When `max_def_level == 0` (required column), all bits are set.
///
/// Writes `ceil(num_rows / 64)` words; the caller must ensure `out` has capacity
/// for that many. Uses a branchless comparison so the inner loop auto-vectorizes.
///
/// The primitive path uses this because it has def_levels in hand and no per-row
/// bool vector; the byte path instead uses `write_presence_bitset`, packing from
/// the `Vec<bool>` it already builds to slice its values.
#[inline]
unsafe fn pack_presence_from_def_levels(
    def_levels: &[i16],
    max_def_level: i16,
    num_rows: usize,
    out: *mut i64,
) {
    // The optional-column branch reads def_levels[0..num_rows] unchecked; assert the precondition
    // so a corrupt page index or decoder state fails loudly in tests rather than reading OOB.
    debug_assert!(def_levels.len() >= num_rows || max_def_level == 0);
    let words = (num_rows + 63) / 64;
    if max_def_level == 0 {
        // Required column: every row is present → all-ones, mask tail.
        for w in 0..words {
            let remaining = num_rows - w * 64;
            if remaining >= 64 {
                *out.add(w) = -1i64; // all bits set
            } else {
                *out.add(w) = ((1u64 << remaining) - 1) as i64;
            }
        }
    } else {
        // Optional column: branchless pack. The inner loop auto-vectorizes because
        // `(d == max_def_level) as u64` is a conditional-move / compare instruction.
        for w in 0..words {
            let mut bits: u64 = 0;
            let base = w * 64;
            let end = (base + 64).min(num_rows);
            for b in base..end {
                bits |= ((*def_levels.get_unchecked(b) == max_def_level) as u64) << (b - base);
            }
            *out.add(w) = bits as i64;
        }
    }
}

/// Writes dense non-null values into the caller's out-buffer as per-row raw i64
/// bits, using the packed presence bitset to scatter. Null slots hold 0.
///
/// The inner scatter loop is split by nullability: when `null_count == 0` the
/// entire dense buffer can be converted with a tight widening loop that
/// auto-vectorizes (LLVM emits vpmovsxdq / sshll). The nullable path reads the
/// packed bits we just wrote and scatters accordingly.
///
/// # Safety
/// `out` must be valid for `num_rows * 8` bytes. `presence_bits` must contain
/// the packed bitset already written by `pack_presence_from_def_levels`.
#[inline]
unsafe fn expand_to_outbuf<T: Copy>(
    dense: &[T],
    to_bits: impl Fn(T) -> i64,
    null_count: i64,
    num_rows: usize,
    out: *mut u8,
    presence_bits: *const i64,
) {
    let out_i64 = out as *mut i64;
    if null_count == 0 {
        // All rows present — tight conversion loop, no branching, SIMD-friendly.
        for i in 0..num_rows {
            *out_i64.add(i) = to_bits(*dense.get_unchecked(i));
        }
    } else {
        // Scatter using the packed presence bits. Read one word at a time and use
        // trailing_zeros to jump to set bits (pop-and-scatter pattern).
        // First zero-fill so null slots hold 0 without an explicit branch.
        std::ptr::write_bytes(out, 0, num_rows * 8);
        let mut di = 0usize;
        let words = (num_rows + 63) / 64;
        for w in 0..words {
            let mut bits = *presence_bits.add(w) as u64;
            while bits != 0 {
                let b = bits.trailing_zeros() as usize;
                let row = w * 64 + b;
                *out_i64.add(row) = to_bits(*dense.get_unchecked(di));
                di += 1;
                bits &= bits - 1; // clear lowest set bit
            }
        }
    }
}

/// Packs a per-row presence slice into a little-endian `long[]` bitset (bit i set
/// when row i is present), writing into `out` (capacity `out_words`). Returns the
/// number of words required; on capacity shortfall writes nothing and the caller
/// treats the positive required count as an overflow signal.
///
/// The byte path uses this because it already builds a `Vec<bool>` to slice its
/// values; the primitive path instead uses `pack_presence_from_def_levels`, which
/// packs straight from def_levels without materializing a bool vector.
unsafe fn write_presence_bitset(presence: &[bool], out: *mut i64, out_words: i64) -> i64 {
    let words_needed = ((presence.len() + 63) / 64) as i64;
    if words_needed > out_words || out.is_null() {
        return words_needed;
    }
    for w in 0..words_needed as usize {
        let mut bits: u64 = 0;
        let base = w * 64;
        for b in 0..64 {
            let idx = base + b;
            if idx >= presence.len() {
                break;
            }
            if presence[idx] {
                bits |= 1u64 << b;
            }
        }
        *out.add(w) = bits as i64;
    }
    words_needed
}

/// Decode the page containing global row `row` into caller buffers (values +
/// presence bitset).
///
/// On success (`RC_OK`):
///   - `out_first_row` / `out_last_row` = inclusive global row range of the page
///   - primitive columns: per-row raw bits written to `out_value_buf`
///     (interpreted as `long[]`, one slot per row; null rows hold 0);
///     `out_value_actual_len` = `rows * 8`
///   - BYTE_ARRAY columns: concatenated value bytes in `out_value_buf`, per-row
///     CSR offsets (length `rows + 1`) in `out_byte_offsets`;
///     `out_value_actual_len` = total bytes used
///   - `out_presence_bitset` = packed `long[]`, one bit per row
///
/// On `RC_OVERFLOW` (a positive sentinel): `out_first_row`, `out_last_row` and
/// `out_value_actual_len` are populated so the caller can size every buffer
/// (values = `out_value_actual_len` bytes; offsets = `rows + 1`; presence =
/// `ceil(rows / 64)` words) and retry once. Returns a `< 0` error pointer for an
/// unknown handle, an out-of-range row, or a repeated (multi-valued) column.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_decode_page_at_row(
    handle: i64,
    row: i64,
    out_first_row: *mut i64,
    out_last_row: *mut i64,
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_byte_offsets: *mut i32,
    out_byte_offsets_cap: i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> i64 {
    let mut guard = lock_readers()?;
    let state = guard
        .get_mut(&handle)
        .ok_or_else(|| format!("parquet_decode_page_at_row: unknown handle {}", handle))?;

    if row < 0 || row >= state.row_count {
        return Err(format!(
            "parquet_decode_page_at_row: row {} out of range (row count {})",
            row, state.row_count
        ));
    }
    if state.repeated {
        return Err(format!(
            "parquet_decode_page_at_row: column is repeated (multi-valued); use parquet_read_repeated_at_row (handle {})",
            handle
        ));
    }

    let page_idx = state.page_for_row(row)?;
    // Copy out the page's coordinates before borrowing the reader mutably.
    let (rg_idx, local_first, num_rows, first_global) = {
        let e = &state.pages[page_idx];
        (e.rg_idx, e.local_first_row, e.num_rows, e.global_first_row)
    };
    let num_rows_usize = num_rows as usize;
    let max_def_level = state.max_def_level;
    let physical_type = state.physical_type;

    // Always report the page row range so the caller can bound its cache and
    // size buffers even on the overflow path.
    if !out_first_row.is_null() {
        *out_first_row = first_global;
    }
    if !out_last_row.is_null() {
        *out_last_row = first_global + num_rows - 1;
    }

    // For primitive types the value byte length is known before decode (num_rows * 8), so check
    // capacity up front and write straight into the out-buffers. BYTE_ARRAY defers its capacity
    // check to after decode, since its total byte length is data-dependent.
    let is_primitive = physical_type != PhysicalType::BYTE_ARRAY
        && physical_type != PhysicalType::FIXED_LEN_BYTE_ARRAY;

    if is_primitive {
        let value_bytes = (num_rows_usize * 8) as i64;
        if !out_value_actual_len.is_null() {
            *out_value_actual_len = value_bytes;
        }
        let presence_words = ((num_rows_usize + 63) / 64) as i64;
        if value_bytes > out_value_buf_cap
            || out_value_buf.is_null()
            || presence_words > out_presence_bits_cap
            || out_presence_bitset.is_null()
        {
            return Ok(RC_OVERFLOW);
        }
    }

    // Get the null_count from the page entry for the expand path (0 means all-present).
    let page_null_count = state.pages[page_idx].null_count;
    // If null_count is unknown (-1), treat as potentially nullable.
    let effective_null_count = if page_null_count < 0 { 1 } else { page_null_count };

    // Retained-cursor reader acquisition. A typed column reader can only move forward, so it
    // is reusable exactly when the target page is in the same row group at-or-ahead of the
    // cursor's position; then the skip is the remaining forward distance and — crucially —
    // the dictionary page and row-group metadata are NOT re-read. Any backward jump or
    // row-group change falls back to a fresh reader (dictionary re-decoded once).
    // The cursor is take()n up front so a decode error leaves it invalidated; each arm
    // re-installs it only after a successful decode.
    let (col, skip) = match state.cursor.take() {
        Some(c) if c.rg_idx == rg_idx && c.position <= first_global => {
            (c.col_reader, (first_global - c.position) as usize)
        }
        _ => {
            let rg = state.reader.get_row_group(rg_idx).map_err(|e| e.to_string())?;
            let col = rg.get_column_reader(state.leaf_idx).map_err(|e| e.to_string())?;
            (col, local_first as usize)
        }
    };
    // Position of the reader after this page is consumed; stored on the re-installed cursor.
    let next_position = first_global + num_rows;

    // Decode and write directly to the out-buffers. Primitive types decode with reused scratch
    // buffers (no per-call allocation), pack presence branchlessly from def_levels, and scatter
    // values straight to the out-buffer. BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY decode into a temporary
    // value vector first, since their total byte length must be computed before the overflow check.
    match col {
        ColumnReader::Int32ColumnReader(mut r) => {
            let scratch = &mut state.scratch;
            decode_primitive_page(
                &mut r, skip, num_rows_usize, max_def_level, effective_null_count,
                &mut scratch.def_levels, &mut scratch.values_i32, |v| v as i64,
                out_value_buf, out_presence_bitset,
            )?;
            state.cursor = Some(CursorState { rg_idx, col_reader: ColumnReader::Int32ColumnReader(r), position: next_position });
            return Ok(RC_OK);
        }
        ColumnReader::Int64ColumnReader(mut r) => {
            let scratch = &mut state.scratch;
            decode_primitive_page(
                &mut r, skip, num_rows_usize, max_def_level, effective_null_count,
                &mut scratch.def_levels, &mut scratch.values_i64, |v| v,
                out_value_buf, out_presence_bitset,
            )?;
            state.cursor = Some(CursorState { rg_idx, col_reader: ColumnReader::Int64ColumnReader(r), position: next_position });
            return Ok(RC_OK);
        }
        ColumnReader::FloatColumnReader(mut r) => {
            let scratch = &mut state.scratch;
            decode_primitive_page(
                &mut r, skip, num_rows_usize, max_def_level, effective_null_count,
                &mut scratch.def_levels, &mut scratch.values_f32, |v| v.to_bits() as i64,
                out_value_buf, out_presence_bitset,
            )?;
            state.cursor = Some(CursorState { rg_idx, col_reader: ColumnReader::FloatColumnReader(r), position: next_position });
            return Ok(RC_OK);
        }
        ColumnReader::DoubleColumnReader(mut r) => {
            let scratch = &mut state.scratch;
            decode_primitive_page(
                &mut r, skip, num_rows_usize, max_def_level, effective_null_count,
                &mut scratch.def_levels, &mut scratch.values_f64, |v| v.to_bits() as i64,
                out_value_buf, out_presence_bitset,
            )?;
            state.cursor = Some(CursorState { rg_idx, col_reader: ColumnReader::DoubleColumnReader(r), position: next_position });
            return Ok(RC_OK);
        }
        ColumnReader::BoolColumnReader(mut r) => {
            let scratch = &mut state.scratch;
            decode_primitive_page(
                &mut r, skip, num_rows_usize, max_def_level, effective_null_count,
                &mut scratch.def_levels, &mut scratch.values_bool, |v| if v { 1i64 } else { 0i64 },
                out_value_buf, out_presence_bitset,
            )?;
            state.cursor = Some(CursorState { rg_idx, col_reader: ColumnReader::BoolColumnReader(r), position: next_position });
            return Ok(RC_OK);
        }
        ColumnReader::ByteArrayColumnReader(mut r) => {
            let rc = decode_byte_page(
                &mut r, skip, num_rows_usize, max_def_level, &mut state.scratch.def_levels,
                out_value_buf, out_value_buf_cap, out_value_actual_len,
                out_byte_offsets, out_byte_offsets_cap, out_presence_bitset, out_presence_bits_cap,
            )?;
            // Install the cursor only on a successful decode, matching the primitive path. On
            // RC_OVERFLOW the caller retries the same row, which reopens a fresh reader anyway.
            if rc == RC_OK {
                state.cursor = Some(CursorState { rg_idx, col_reader: ColumnReader::ByteArrayColumnReader(r), position: next_position });
            }
            return Ok(rc);
        }
        ColumnReader::FixedLenByteArrayColumnReader(mut r) => {
            let rc = decode_byte_page(
                &mut r, skip, num_rows_usize, max_def_level, &mut state.scratch.def_levels,
                out_value_buf, out_value_buf_cap, out_value_actual_len,
                out_byte_offsets, out_byte_offsets_cap, out_presence_bitset, out_presence_bits_cap,
            )?;
            // See the ByteArray arm: install the cursor only on a successful decode.
            if rc == RC_OK {
                state.cursor = Some(CursorState { rg_idx, col_reader: ColumnReader::FixedLenByteArrayColumnReader(r), position: next_position });
            }
            return Ok(rc);
        }
        ColumnReader::Int96ColumnReader(_) => {
            let _ = physical_type; // silence unused in this arm
            Err("parquet_decode_page_at_row: INT96 columns are not supported".to_string())
        }
    }
}

/// Expands dense non-null byte values into a per-row slice vector, null rows mapping to an empty
/// slice. Works for both `ByteArray` and `FixedLenByteArray` via the `AsBytes` trait they share.
fn expand_byte_values<'a, T: parquet::data_type::AsBytes>(presence: &[bool], dense: &'a [T]) -> Vec<&'a [u8]> {
    let mut out: Vec<&[u8]> = Vec::with_capacity(presence.len());
    let mut di = 0usize;
    for &present in presence {
        if present {
            out.push(dense[di].as_bytes());
            di += 1;
        } else {
            out.push(&[]);
        }
    }
    out
}

/// Decodes one BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY page and writes it to the caller buffers
/// (concatenated value bytes + per-row CSR offsets + presence bitset), or returns `RC_OVERFLOW`
/// after recording the required sizes. Shared by both byte column types via the `AsBytes` trait.
/// Unlike the primitive path, the values decode into a temporary vector first because the total
/// byte length is data-dependent and must be known before the capacity check.
#[allow(clippy::too_many_arguments)]
unsafe fn decode_byte_page<T: ParquetDataType>(
    r: &mut ColumnReaderImpl<T>,
    skip: usize,
    num_rows: usize,
    max_def_level: i16,
    def_scratch: &mut Vec<i16>,
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_byte_offsets: *mut i32,
    out_byte_offsets_cap: i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> Result<i64, String>
where
    T::T: parquet::data_type::AsBytes,
{
    let mut values: Vec<T::T> = Vec::new();
    decode_page_records(r, skip, num_rows, def_scratch, &mut values)?;
    let presence: Vec<bool> = if max_def_level == 0 {
        vec![true; num_rows]
    } else {
        def_scratch.iter().take(num_rows).map(|d| *d == max_def_level).collect()
    };
    let slices = expand_byte_values(&presence, &values);
    write_bytes_page(
        &slices, &presence, out_value_buf, out_value_buf_cap, out_value_actual_len,
        out_byte_offsets, out_byte_offsets_cap, out_presence_bitset, out_presence_bits_cap,
    )
}

/// Writes a decoded BYTE_ARRAY page (concatenated bytes + CSR offsets + presence
/// bitset) to the caller buffers, or returns `RC_OVERFLOW` after recording the
/// required total byte length.
unsafe fn write_bytes_page(
    slices: &[&[u8]],
    presence: &[bool],
    out_value_buf: *mut u8,
    out_value_buf_cap: i64,
    out_value_actual_len: *mut i64,
    out_byte_offsets: *mut i32,
    out_byte_offsets_cap: i64,
    out_presence_bitset: *mut i64,
    out_presence_bits_cap: i64,
) -> Result<i64, String> {
    let total_bytes: usize = slices.iter().map(|s| s.len()).sum();
    if !out_value_actual_len.is_null() {
        *out_value_actual_len = total_bytes as i64;
    }

    let offsets_needed = (slices.len() + 1) as i64;
    let presence_words = ((presence.len() + 63) / 64) as i64;
    if (total_bytes as i64) > out_value_buf_cap
        || (total_bytes > 0 && out_value_buf.is_null())
        || offsets_needed > out_byte_offsets_cap
        || out_byte_offsets.is_null()
        || presence_words > out_presence_bits_cap
        || out_presence_bitset.is_null()
    {
        return Ok(RC_OVERFLOW);
    }

    // Offsets are exchanged as i32 (a page is small by design, ~20k rows). Accumulate in i64 and
    // reject a page whose byte total would exceed i32::MAX rather than let the offset wrap, which
    // would corrupt the offsets and drive an out-of-bounds write into out_value_buf.
    let mut acc: i64 = 0;
    for (i, s) in slices.iter().enumerate() {
        if acc > i32::MAX as i64 {
            return Err(format!("byte page offset {} exceeds i32::MAX", acc));
        }
        *out_byte_offsets.add(i) = acc as i32;
        if !s.is_empty() {
            std::ptr::copy_nonoverlapping(s.as_ptr(), out_value_buf.add(acc as usize), s.len());
        }
        acc += s.len() as i64;
    }
    if acc > i32::MAX as i64 {
        return Err(format!("byte page total {} exceeds i32::MAX", acc));
    }
    *out_byte_offsets.add(slices.len()) = acc as i32;

    write_presence_bitset(presence, out_presence_bitset, out_presence_bits_cap);
    Ok(RC_OK)
}
