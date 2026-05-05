/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::array::{Int32Array, StringArray, StructArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::array::Array;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::sync::Arc;
use tempfile::tempdir;

use crate::writer::NativeParquetWriter;

pub fn create_test_ffi_schema() -> (Arc<Schema>, i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    (schema, schema_ptr)
}

pub fn cleanup_ffi_schema(schema_ptr: i64) {
    unsafe {
        let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
    }
}

pub fn create_test_ffi_data() -> Result<(i64, i64), Box<dyn std::error::Error>> {
    create_test_ffi_data_with_ids(vec![1, 2, 3], vec![Some("Alice"), Some("Bob"), None])
}

pub fn create_test_ffi_data_with_ids(ids: Vec<i32>, names: Vec<Option<&str>>) -> Result<(i64, i64), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id_array = Arc::new(Int32Array::from(ids));
    let name_array = Arc::new(StringArray::from(names));
    let record_batch = RecordBatch::try_new(schema.clone(), vec![id_array, name_array])?;
    let struct_array = StructArray::from(record_batch);
    let array_data = struct_array.into_data();
    let ffi_array = FFI_ArrowArray::new(&array_data);
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())?;
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    Ok((array_ptr, schema_ptr))
}

pub fn cleanup_ffi_data(array_ptr: i64, schema_ptr: i64) {
    unsafe {
        let _ = Box::from_raw(array_ptr as *mut FFI_ArrowArray);
        let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
    }
}

pub fn get_temp_file_path(name: &str) -> (tempfile::TempDir, String) {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join(name);
    let filename = file_path.to_string_lossy().to_string();
    (temp_dir, filename)
}

pub fn create_writer_and_assert_success(filename: &str) -> (Arc<Schema>, i64) {
    let (schema, schema_ptr) = create_test_ffi_schema();
    let result = NativeParquetWriter::create_writer(filename.to_string(), "test-index".to_string(), schema_ptr, vec![], vec![], vec![]);
    assert!(result.is_ok());
    (schema, schema_ptr)
}

pub fn create_sorted_writer_and_assert_success(filename: &str, sort_column: &str, reverse: bool) -> (Arc<Schema>, i64) {
    let (schema, schema_ptr) = create_test_ffi_schema();
    let result = NativeParquetWriter::create_writer(
        filename.to_string(), "test-index".to_string(), schema_ptr, vec![sort_column.to_string()], vec![reverse], vec![false]
    );
    assert!(result.is_ok());
    (schema, schema_ptr)
}

pub fn close_writer_and_cleanup_schema(filename: &str, schema_ptr: i64) {
    let _ = NativeParquetWriter::finalize_writer(filename.to_string());
    cleanup_ffi_schema(schema_ptr);
}

pub fn write_ffi_data_to_writer(filename: &str) -> (i64, i64) {
    let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
    let result = NativeParquetWriter::write_data(filename.to_string(), array_ptr, data_schema_ptr);
    assert!(result.is_ok());
    (array_ptr, data_schema_ptr)
}

pub fn create_mismatched_ffi_data() -> Result<(i64, i64), Box<dyn std::error::Error>> {
    use arrow::array::{BooleanArray, Int64Array};
    let schema = Arc::new(Schema::new(vec![
        Field::new("count", DataType::Int64, false),
        Field::new("active", DataType::Boolean, true),
    ]));
    let count_array = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let active_array = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
    let record_batch = RecordBatch::try_new(schema.clone(), vec![count_array, active_array])?;
    let struct_array = StructArray::from(record_batch);
    let array_data = struct_array.into_data();
    let ffi_array = FFI_ArrowArray::new(&array_data);
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())?;
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;
    Ok((array_ptr, schema_ptr))
}

pub fn close_writer_and_get_metadata(filename: &str, schema_ptr: i64) -> crate::writer::FinalizeResult {
    let result = NativeParquetWriter::finalize_writer(filename.to_string());
    cleanup_ffi_schema(schema_ptr);
    result.unwrap().unwrap()
}

pub fn read_parquet_file(filename: &str) -> Vec<RecordBatch> {
    let file = File::open(filename).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let reader = builder.build().unwrap();
    reader.collect::<Result<Vec<_>, _>>().unwrap()
}

pub fn read_parquet_file_sorted_ids(filename: &str) -> Vec<i32> {
    let batches = read_parquet_file(filename);
    let combined = concat_batches(&batches[0].schema(), &batches).unwrap();
    let id_col = combined.column(0)
        .as_any().downcast_ref::<Int32Array>().unwrap();
    (0..id_col.len()).map(|i| id_col.value(i)).collect()
}
