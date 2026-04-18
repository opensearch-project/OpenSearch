/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::array::{Int32Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow_array::Array;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use std::sync::Arc;
use tempfile::tempdir;

use crate::object_store_handle;
use crate::writer;

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
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let name_array = Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None]));
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

/// Create a local FS ObjectStore rooted at a temp dir and return (temp_dir, store_handle).
pub fn create_temp_store() -> (tempfile::TempDir, i64) {
    let temp_dir = tempdir().unwrap();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap());
    let handle = object_store_handle::arc_into_raw(store);
    (temp_dir, handle)
}

/// Create a writer on the given store and return the writer handle.
pub fn create_writer_on_store(store_handle: i64, object_path: &str) -> i64 {
    let (_schema, schema_ptr) = create_test_ffi_schema();
    let writer_handle = writer::create_writer(store_handle, object_path, schema_ptr)
        .expect("Failed to create writer");
    writer_handle
}

/// Write test FFI data to a writer handle.
pub fn write_ffi_data_to_handle(writer_handle: i64) -> (i64, i64) {
    let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
    writer::write_data(writer_handle, array_ptr, data_schema_ptr).expect("Failed to write data");
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
