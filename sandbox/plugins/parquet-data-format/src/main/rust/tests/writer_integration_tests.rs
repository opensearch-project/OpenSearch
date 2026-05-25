/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use opensearch_parquet_format::test_utils::*;
use opensearch_parquet_format::writer::NativeParquetWriter;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

#[test]
fn test_complete_writer_lifecycle() {
    let (_temp_dir, filename) = get_temp_file_path("complete_workflow.parquet");
    let file_path = std::path::Path::new(&filename);
    let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

    for _i in 0..3 {
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
        cleanup_ffi_data(array_ptr, data_schema_ptr);
    }

    let metadata = close_writer_and_get_metadata(&filename, schema_ptr);
    assert_eq!(metadata.metadata.file_metadata().num_rows(), 9); // 3 batches × 3 rows
    assert!(metadata.metadata.file_metadata().version() > 0);

    assert!(NativeParquetWriter::sync_to_disk(filename.clone()).is_ok());
    assert!(file_path.exists());
    assert!(file_path.metadata().unwrap().len() > 0);

    let read_metadata = NativeParquetWriter::get_file_metadata(filename.clone()).unwrap();
    assert_eq!(read_metadata.num_rows(), metadata.metadata.file_metadata().num_rows());
    assert_eq!(read_metadata.version(), metadata.metadata.file_metadata().version());
}

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
fn test_concurrent_complete_writer_lifecycle() {
    let temp_dir = tempdir().unwrap();
    let thread_count = 6;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..thread_count {
        let temp_dir_path = temp_dir.path().to_path_buf();
        let success_count = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            let file_path = temp_dir_path.join(format!("lifecycle_{}.parquet", i));
            let filename = file_path.to_string_lossy().to_string();
            let (_schema, schema_ptr) = create_test_ffi_schema();

            if NativeParquetWriter::create_writer(filename.clone(), "test-index".to_string(), schema_ptr, vec![], vec![], vec![], 0).is_ok() {
                let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
                let write_ok = NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr).is_ok();
                cleanup_ffi_data(array_ptr, data_schema_ptr);

                if write_ok {
                    if let Ok(Some(metadata)) = NativeParquetWriter::finalize_writer(filename.clone()) {
                        if metadata.metadata.file_metadata().num_rows() == 3
                            && NativeParquetWriter::sync_to_disk(filename.clone()).is_ok()
                            && file_path.exists()
                        {
                            success_count.fetch_add(1, Ordering::SeqCst);
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

// ===== Arrow IPC staging integration tests =====

#[test]
fn test_ipc_staging_sorted_writer_integration() {
    let (_temp_dir, filename) = get_temp_file_path("ipc_integ_sorted.parquet");
    let (_schema, schema_ptr) = create_test_ffi_schema();

    NativeParquetWriter::create_writer(
        filename.clone(), "test-index".to_string(), schema_ptr,
        vec!["id".to_string()], vec![false], vec![false], 0
    ).unwrap();

    // Write multiple batches with out-of-order data
    for batch_ids in [vec![50, 30, 10], vec![40, 20, 60]] {
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

    let ids = read_parquet_file_sorted_ids(&filename);
    assert_eq!(ids, vec![10, 20, 30, 40, 50, 60]);

    let read_metadata = NativeParquetWriter::get_file_metadata(filename).unwrap();
    assert_eq!(read_metadata.num_rows(), 6);

    cleanup_ffi_schema(schema_ptr);
}

#[test]
fn test_ipc_staging_concurrent_sorted_lifecycle() {
    let temp_dir = tempdir().unwrap();
    let thread_count = 6;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..thread_count {
        let temp_dir_path = temp_dir.path().to_path_buf();
        let success_count = Arc::clone(&success_count);
        let handle = thread::spawn(move || {
            let file_path = temp_dir_path.join(format!("ipc_lifecycle_{}.parquet", i));
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
                        if metadata.metadata.file_metadata().num_rows() == 3
                            && NativeParquetWriter::sync_to_disk(filename.clone()).is_ok()
                            && file_path.exists()
                        {
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
fn test_ipc_and_parquet_mixed_concurrent_lifecycle() {
    let temp_dir = tempdir().unwrap();
    let thread_count = 8;
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for i in 0..thread_count {
        let temp_dir_path = temp_dir.path().to_path_buf();
        let success_count = Arc::clone(&success_count);
        let use_sort = i % 2 == 0; // Even threads use IPC (sorted), odd use Parquet (unsorted)

        let handle = thread::spawn(move || {
            let file_path = temp_dir_path.join(format!("mixed_{}.parquet", i));
            let filename = file_path.to_string_lossy().to_string();
            let (_schema, schema_ptr) = create_test_ffi_schema();

            let sort_cols = if use_sort { vec!["id".to_string()] } else { vec![] };
            let reverse = if use_sort { vec![false] } else { vec![] };
            let nulls = if use_sort { vec![false] } else { vec![] };

            if NativeParquetWriter::create_writer(
                filename.clone(), "test-index".to_string(), schema_ptr,
                sort_cols, reverse, nulls, 0
            ).is_ok() {
                let (ap, sp) = create_test_ffi_data_with_ids(
                    vec![30, 10, 20], vec![Some("C"), Some("A"), Some("B")]
                ).unwrap();
                let write_ok = NativeParquetWriter::write_data(filename.clone(), ap, sp).is_ok();
                cleanup_ffi_data(ap, sp);

                if write_ok {
                    if let Ok(Some(metadata)) = NativeParquetWriter::finalize_writer(filename.clone()) {
                        if metadata.metadata.file_metadata().num_rows() == 3 && file_path.exists() {
                            let ids = read_parquet_file_sorted_ids(&filename);
                            let expected = if use_sort { vec![10, 20, 30] } else { vec![30, 10, 20] };
                            if ids == expected {
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

// ===== Two-tier encoding/compression settings tests =====

use opensearch_parquet_format::native_settings::NativeSettings;
use opensearch_parquet_format::field_config::FieldConfig;
use opensearch_parquet_format::SETTINGS_STORE;
use parquet::basic::{Compression, Encoding};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::collections::HashMap;
use std::fs::File;

fn read_column_compression(path: &str, col_name: &str) -> Compression {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    let meta = reader.metadata();
    let schema = meta.file_metadata().schema_descr();
    let rg = meta.row_group(0);
    for i in 0..rg.num_columns() {
        let col = rg.column(i);
        if col.column_descr().name() == col_name {
            return col.compression();
        }
    }
    panic!("column '{}' not found in {}", col_name, path);
}

fn read_column_encoding(path: &str, col_name: &str) -> Vec<Encoding> {
    let reader = SerializedFileReader::new(File::open(path).unwrap()).unwrap();
    let meta = reader.metadata();
    let rg = meta.row_group(0);
    for i in 0..rg.num_columns() {
        let col = rg.column(i);
        if col.column_descr().name() == col_name {
            return col.encodings().collect();
        }
    }
    panic!("column '{}' not found in {}", col_name, path);
}

/// Index-level field compression is applied to the named column.
#[test]
fn test_index_level_field_compression_applied() {
    let index = "test_index_field_compression";
    let mut field_configs = HashMap::new();
    field_configs.insert("id".to_string(), FieldConfig {
        compression_type: Some("SNAPPY".to_string()),
        ..Default::default()
    });
    SETTINGS_STORE.insert(index.to_string(), NativeSettings {
        index_name: Some(index.to_string()),
        field_configs: Some(field_configs),
        ..Default::default()
    });

    let (_tmp, path) = get_temp_file_path("idx_field_comp.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success_for_index(&path, index);
    let (ap, sp) = create_test_ffi_data().unwrap();
    NativeParquetWriter::write_data(path.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);
    NativeParquetWriter::finalize_writer(path.clone()).unwrap();
    cleanup_ffi_schema(schema_ptr);

    assert!(matches!(read_column_compression(&path, "id"), Compression::SNAPPY));
    SETTINGS_STORE.remove(index);
}

/// Cluster-level type compression is applied when no index-level field config exists.
#[test]
fn test_cluster_level_type_compression_fallback() {
    let index = "test_cluster_type_compression";
    let mut type_compression = HashMap::new();
    type_compression.insert("int32".to_string(), "SNAPPY".to_string());
    SETTINGS_STORE.insert(index.to_string(), NativeSettings {
        index_name: Some(index.to_string()),
        type_compression_configs: Some(type_compression),
        ..Default::default()
    });

    let (_tmp, path) = get_temp_file_path("cluster_type_comp.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success_for_index(&path, index);
    let (ap, sp) = create_test_ffi_data().unwrap();
    NativeParquetWriter::write_data(path.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);
    NativeParquetWriter::finalize_writer(path.clone()).unwrap();
    cleanup_ffi_schema(schema_ptr);

    // "id" is Int32 → should get SNAPPY from cluster type config
    assert!(matches!(read_column_compression(&path, "id"), Compression::SNAPPY));
    SETTINGS_STORE.remove(index);
}

/// Index-level field config takes priority over cluster-level type config.
#[test]
fn test_index_level_overrides_cluster_level_compression() {
    let index = "test_index_overrides_cluster_compression";
    let mut field_configs = HashMap::new();
    field_configs.insert("id".to_string(), FieldConfig {
        compression_type: Some("UNCOMPRESSED".to_string()),
        ..Default::default()
    });
    let mut type_compression = HashMap::new();
    type_compression.insert("int32".to_string(), "SNAPPY".to_string());
    SETTINGS_STORE.insert(index.to_string(), NativeSettings {
        index_name: Some(index.to_string()),
        field_configs: Some(field_configs),
        type_compression_configs: Some(type_compression),
        ..Default::default()
    });

    let (_tmp, path) = get_temp_file_path("idx_overrides_cluster_comp.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success_for_index(&path, index);
    let (ap, sp) = create_test_ffi_data().unwrap();
    NativeParquetWriter::write_data(path.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);
    NativeParquetWriter::finalize_writer(path.clone()).unwrap();
    cleanup_ffi_schema(schema_ptr);

    // Index says UNCOMPRESSED for "id", cluster says SNAPPY for int32 — index wins
    assert!(matches!(read_column_compression(&path, "id"), Compression::UNCOMPRESSED));
    SETTINGS_STORE.remove(index);
}

/// Cluster-level type encoding is applied when no index-level field config exists.
#[test]
fn test_cluster_level_type_encoding_fallback() {
    let index = "test_cluster_type_encoding";
    let mut type_encoding = HashMap::new();
    type_encoding.insert("int32".to_string(), "DELTA_BINARY_PACKED".to_string());
    SETTINGS_STORE.insert(index.to_string(), NativeSettings {
        index_name: Some(index.to_string()),
        type_encoding_configs: Some(type_encoding),
        ..Default::default()
    });

    let (_tmp, path) = get_temp_file_path("cluster_type_enc.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success_for_index(&path, index);
    let (ap, sp) = create_test_ffi_data().unwrap();
    NativeParquetWriter::write_data(path.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);
    NativeParquetWriter::finalize_writer(path.clone()).unwrap();
    cleanup_ffi_schema(schema_ptr);

    // "id" is Int32 → should get DELTA_BINARY_PACKED from cluster type config
    let encodings = read_column_encoding(&path, "id");
    assert!(encodings.contains(&Encoding::DELTA_BINARY_PACKED),
        "expected DELTA_BINARY_PACKED in {:?}", encodings);
    SETTINGS_STORE.remove(index);
}

/// Index-level field encoding takes priority over cluster-level type encoding.
#[test]
fn test_index_level_overrides_cluster_level_encoding() {
    let index = "test_index_overrides_cluster_encoding";
    let mut field_configs = HashMap::new();
    field_configs.insert("id".to_string(), FieldConfig {
        encoding_type: Some("PLAIN".to_string()),
        ..Default::default()
    });
    let mut type_encoding = HashMap::new();
    type_encoding.insert("int32".to_string(), "DELTA_BINARY_PACKED".to_string());
    SETTINGS_STORE.insert(index.to_string(), NativeSettings {
        index_name: Some(index.to_string()),
        field_configs: Some(field_configs),
        type_encoding_configs: Some(type_encoding),
        ..Default::default()
    });

    let (_tmp, path) = get_temp_file_path("idx_overrides_cluster_enc.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success_for_index(&path, index);
    let (ap, sp) = create_test_ffi_data().unwrap();
    NativeParquetWriter::write_data(path.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);
    NativeParquetWriter::finalize_writer(path.clone()).unwrap();
    cleanup_ffi_schema(schema_ptr);

    // Index says PLAIN for "id", cluster says DELTA_BINARY_PACKED for int32 — index wins
    let encodings = read_column_encoding(&path, "id");
    assert!(!encodings.contains(&Encoding::DELTA_BINARY_PACKED),
        "DELTA_BINARY_PACKED should not be present when index-level PLAIN is set, got {:?}", encodings);
    SETTINGS_STORE.remove(index);
}

/// Different columns get different configs: one from index level, one from cluster level.
#[test]
fn test_mixed_index_and_cluster_level_configs() {
    let index = "test_mixed_index_cluster";
    let mut field_configs = HashMap::new();
    // "id" (Int32) has explicit index-level compression
    field_configs.insert("id".to_string(), FieldConfig {
        compression_type: Some("UNCOMPRESSED".to_string()),
        ..Default::default()
    });
    let mut type_compression = HashMap::new();
    // "utf8" type → "name" column (Utf8) gets SNAPPY from cluster level
    type_compression.insert("utf8".to_string(), "SNAPPY".to_string());
    SETTINGS_STORE.insert(index.to_string(), NativeSettings {
        index_name: Some(index.to_string()),
        field_configs: Some(field_configs),
        type_compression_configs: Some(type_compression),
        ..Default::default()
    });

    let (_tmp, path) = get_temp_file_path("mixed_idx_cluster.parquet");
    let (_schema, schema_ptr) = create_writer_and_assert_success_for_index(&path, index);
    let (ap, sp) = create_test_ffi_data().unwrap();
    NativeParquetWriter::write_data(path.clone(), ap, sp).unwrap();
    cleanup_ffi_data(ap, sp);
    NativeParquetWriter::finalize_writer(path.clone()).unwrap();
    cleanup_ffi_schema(schema_ptr);

    assert!(matches!(read_column_compression(&path, "id"), Compression::UNCOMPRESSED));
    assert!(matches!(read_column_compression(&path, "name"), Compression::SNAPPY));
    SETTINGS_STORE.remove(index);
}

/// Helper: create a writer for a specific index name (not the default "test-index").
fn create_writer_and_assert_success_for_index(filename: &str, index: &str) -> (std::sync::Arc<arrow::datatypes::Schema>, i64) {
    let (schema, schema_ptr) = create_test_ffi_schema();
    NativeParquetWriter::create_writer(
        filename.to_string(), index.to_string(), schema_ptr, vec![], vec![], vec![], 0
    ).expect("create_writer failed");
    (schema, schema_ptr)
}

/// Helper: create a writer with a 3-column schema: id (Int32), name (Utf8), score (Float64).
fn create_three_col_writer(filename: &str, index: &str) -> (std::sync::Arc<arrow::datatypes::Schema>, i64) {
    use arrow::datatypes::{Field, Schema, DataType};
    use arrow::ffi::FFI_ArrowSchema;
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id",    DataType::Int32,   false),
        Field::new("name",  DataType::Utf8,    true),
        Field::new("score", DataType::Float64, true),
    ]));
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    NativeParquetWriter::create_writer(
        filename.to_string(), index.to_string(), schema_ptr, vec![], vec![], vec![], 0
    ).expect("create_writer failed");
    (schema, schema_ptr)
}

/// Helper: write a 3-column batch (id: Int32, name: Utf8, score: Float64).
fn write_three_col_data(filename: &str) {
    use arrow::array::{Array, Int32Array, StringArray, Float64Array, StructArray};
    use arrow::datatypes::{Field, Schema, DataType};
    use arrow::record_batch::RecordBatch;
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("id",    DataType::Int32,   false),
        Field::new("name",  DataType::Utf8,    true),
        Field::new("score", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![
        std::sync::Arc::new(Int32Array::from(vec![1, 2, 3])),
        std::sync::Arc::new(StringArray::from(vec!["a", "b", "c"])),
        std::sync::Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
    ]).unwrap();
    let struct_array = StructArray::from(batch);
    let array_data = struct_array.into_data();
    let ffi_array = FFI_ArrowArray::new(&array_data);
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    NativeParquetWriter::write_data(filename.to_string(), array_ptr, schema_ptr).unwrap();
    unsafe {
        let _ = Box::from_raw(array_ptr as *mut FFI_ArrowArray);
        let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
    }
}

/// Full 3-tier fallback test:
/// - global: LZ4_RAW for all columns
/// - cluster: SNAPPY for utf8 type → applies to "name"
/// - index:   UNCOMPRESSED for field "id" → overrides global for "id"
/// - "score" (Float64): no index or cluster config → gets global LZ4_RAW
#[test]
fn test_three_tier_compression_fallback() {
    let index = "test_three_tier_compression";
    let mut field_configs = HashMap::new();
    field_configs.insert("id".to_string(), FieldConfig {
        compression_type: Some("UNCOMPRESSED".to_string()),
        ..Default::default()
    });
    let mut type_compression = HashMap::new();
    type_compression.insert("utf8".to_string(), "SNAPPY".to_string());
    SETTINGS_STORE.insert(index.to_string(), NativeSettings {
        index_name: Some(index.to_string()),
        compression_type: Some("LZ4_RAW".to_string()),
        field_configs: Some(field_configs),
        type_compression_configs: Some(type_compression),
        ..Default::default()
    });

    let (_tmp, path) = get_temp_file_path("three_tier_comp.parquet");
    let (_schema, schema_ptr) = create_three_col_writer(&path, index);
    write_three_col_data(&path);
    NativeParquetWriter::finalize_writer(path.clone()).unwrap();
    cleanup_ffi_schema(schema_ptr);

    // index level wins for "id"
    assert!(matches!(read_column_compression(&path, "id"), Compression::UNCOMPRESSED));
    // cluster level wins for "name" (utf8)
    assert!(matches!(read_column_compression(&path, "name"), Compression::SNAPPY));
    // global fallback for "score" (float64, no index or cluster config)
    assert!(matches!(read_column_compression(&path, "score"), Compression::LZ4_RAW));

    SETTINGS_STORE.remove(index);
}
