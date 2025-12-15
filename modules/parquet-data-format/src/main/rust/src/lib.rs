use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use jni::objects::{JClass, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use lazy_static::lazy_static;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::sync::{Arc, Mutex};

pub mod logger;
pub mod parquet_merge;
pub use parquet_merge::*;

// Re-export macros from the shared crate at crate root level
pub use opensearch_vectorized_spi::{rust_log_info, rust_log_warn, rust_log_error, rust_log_debug, rust_log_trace};

lazy_static! {
    static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<File>>>> = DashMap::new();
    static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
}

struct NativeParquetWriter;

impl NativeParquetWriter {

    fn create_writer(filename: String, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        logger::log_info(&format!("[RUST] create_writer called for file: {}, schema_address: {}", filename, schema_address));

        if (schema_address as *mut u8).is_null() {
            logger::log_error(&format!("[RUST] ERROR: Invalid schema address (null pointer) for file: {}, schema_address: {}", filename, schema_address));
            return Err("Invalid schema address".into());
        }

        if WRITER_MANAGER.contains_key(&filename) {
            logger::log_error(&format!("[RUST] ERROR: Writer already exists for file: {}", filename));
            return Err("Writer already exists for this file".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);

        logger::log_info(&format!("[RUST] Schema created with {} fields", schema.fields().len()));

        for (i, field) in schema.fields().iter().enumerate() {
            logger::log_debug(&format!("[RUST] Field {}: {} ({})", i, field.name(), field.data_type()));
        }

        let file = File::create(&filename)?;
        let file_clone = file.try_clone()?;
        FILE_MANAGER.insert(filename.clone(), file_clone);
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        WRITER_MANAGER.insert(filename, Arc::new(Mutex::new(writer)));
        Ok(())
    }

    fn write_data(filename: String, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        logger::log_info(&format!("[RUST] write_data called for file: {}, array_address: {}, schema_address: {}", filename, array_address, schema_address));

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            logger::log_error(&format!("[RUST] ERROR: Invalid FFI addresses for file: {}, array_address: {}, schema_address: {}", filename, array_address, schema_address));
            return Err("Invalid FFI addresses (null pointers)".into());
        }

        unsafe {
            let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
            let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);

            match arrow::ffi::from_ffi(arrow_array, &arrow_schema) {
                Ok(array_data) => {
                    logger::log_debug(&format!("[RUST] Successfully imported array_data, length: {}", array_data.len()));

                    let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);
                    logger::log_debug(&format!("[RUST] Array type: {:?}, length: {}", array.data_type(), array.len()));

                    if let Some(struct_array) = array.as_any().downcast_ref::<arrow::array::StructArray>() {
                        logger::log_debug(&format!("[RUST] Successfully cast to StructArray with {} columns", struct_array.num_columns()));

                        let schema = Arc::new(arrow::datatypes::Schema::new(
                            struct_array.fields().clone()
                        ));

                        let record_batch = RecordBatch::try_new(
                            schema.clone(),
                            struct_array.columns().to_vec(),
                        )?;

                        logger::log_info(&format!("[RUST] Created RecordBatch with {} rows and {} columns", record_batch.num_rows(), record_batch.num_columns()));

                        if let Some(writer_arc) = WRITER_MANAGER.get(&filename) {
                            logger::log_debug("[RUST] Writing RecordBatch to file");
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                            logger::log_info("[RUST] Successfully wrote RecordBatch");
                            Ok(())
                        } else {
                            logger::log_error(&format!("[RUST] ERROR: No writer found for file: {}", filename));
                            Err("Writer not found".into())
                        }
                    } else {
                        logger::log_error(&format!("[RUST] ERROR: Array is not a StructArray, type: {:?}", array.data_type()));
                        Err("Expected struct array from VectorSchemaRoot".into())
                    }
                }
                Err(e) => {
                    logger::log_error(&format!("[RUST] ERROR: Failed to import from FFI: {:?}", e));
                    Err(e.into())
                }
            }
        }
    }

    fn close_writer(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        logger::log_info(&format!("[RUST] close_writer called for file: {}", filename));

        if let Some((_, writer_arc)) = WRITER_MANAGER.remove(&filename) {
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let writer = mutex.into_inner().unwrap();
                    match writer.close() {
                        Ok(_) => {
                            logger::log_info(&format!("[RUST] Successfully closed writer for file: {}", filename));
                            Ok(())
                        }
                        Err(e) => {
                            logger::log_error(&format!("[RUST] ERROR: Failed to close writer for file: {}", filename));
                            Err(e.into())
                        }
                    }
                }
                Err(_) => {
                    logger::log_error(&format!("[RUST] ERROR: Writer still in use for file: {}", filename));
                    Err("Writer still in use".into())
                }
            }
        } else {
            logger::log_error(&format!("[RUST] ERROR: Writer not found for file: {}\n", filename));
            Err("Writer not found".into())
        }
    }

    fn flush_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        logger::log_info(&format!("[RUST] fsync_file called for file: {}", filename));

        if let Some(file) = FILE_MANAGER.get_mut(&filename) {
            match file.sync_all() {
                Ok(_) => {
                    logger::log_info(&format!("[RUST] Successfully fsynced file: {}", filename));
                    drop(file);
                    FILE_MANAGER.remove(&filename);
                    Ok(())
                }
                Err(e) => {
                    logger::log_error(&format!("[RUST] ERROR: Failed to fsync file: {}", filename));
                    Err(e.into())
                }
            }
        } else {
            logger::log_error(&format!("[RUST] ERROR: File not found for fsync: {}", filename));
            Err("File not found".into())
        }
    }

    fn get_filtered_writer_memory_usage(path_prefix: String) -> Result<usize, Box<dyn std::error::Error>> {
        logger::log_debug(&format!("[RUST] get_filtered_writer_memory_usage called with prefix: {}", path_prefix));

        let mut total_memory = 0;
        let mut writer_count = 0;

        for entry in WRITER_MANAGER.iter() {
            let filename = entry.key();
            let writer_arc = entry.value();

            // Filter writers by path prefix
            if filename.starts_with(&path_prefix) {
                if let Ok(writer) = writer_arc.lock() {
                    let memory_usage = writer.memory_size();
                    total_memory += memory_usage;
                    writer_count += 1;

                    logger::log_debug(&format!("[RUST] Filtered Writer {}: {} bytes", filename, memory_usage));
                }
            }
        }

        logger::log_debug(&format!("[RUST] Total memory usage across {} filtered ArrowWriters (prefix: {}): {} bytes", writer_count, path_prefix, total_memory));

        Ok(total_memory)
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    if let Ok(jvm) = env.get_java_vm() {
        logger::init_logger(jvm);
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_createWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    schema_address: jlong
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::create_writer(filename, schema_address as i64) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_write(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    array_address: jlong,
    schema_address: jlong
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::write_data(filename, array_address as i64, schema_address as i64) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_closeWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::close_writer(filename) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_flushToDisk(
    mut env: JNIEnv,
    _class: JClass,
    file: JString
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::flush_to_disk(filename) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_getFilteredNativeBytesUsed(
    mut env: JNIEnv,
    _class: JClass,
    path_prefix: JString
) -> jlong {
    let prefix: String = env.get_string(&path_prefix).expect("Couldn't get java string!").into();
    match NativeParquetWriter::get_filtered_writer_memory_usage(prefix) {
        Ok(memory_usage) => memory_usage as jlong,
        Err(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StructArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi::FFI_ArrowArray;
    use arrow::ffi::FFI_ArrowSchema;
    use arrow_array::Array;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use tempfile::tempdir;

    fn create_test_ffi_schema() -> (Arc<Schema>, i64) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;

        (schema, schema_ptr)
    }

    fn cleanup_ffi_schema(schema_ptr: i64) {
        unsafe {
            let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
        }
    }

    fn create_test_ffi_data() -> Result<(i64, i64), Box<dyn std::error::Error>> {
        use arrow::array::{Int32Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None]));

        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![id_array, name_array],
        )?;

        // Convert to struct array (what Java VectorSchemaRoot exports)
        let struct_array = StructArray::from(record_batch);
        let array_data = struct_array.into_data();

        // Create FFI representations
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;

        Ok((array_ptr, schema_ptr))
    }

    fn cleanup_ffi_data(array_ptr: i64, schema_ptr: i64) {
        unsafe {
            let _ = Box::from_raw(array_ptr as *mut FFI_ArrowArray);
            let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
        }
    }

    fn get_temp_file_path(name: &str) -> (tempfile::TempDir, String) {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join(name);
        let filename = file_path.to_string_lossy().to_string();
        (temp_dir, filename)
    }

    fn create_writer_and_assert_success(filename: &str) -> (Arc<Schema>, i64) {
        let (schema, schema_ptr) = create_test_ffi_schema();
        let result = NativeParquetWriter::create_writer(filename.to_string(), schema_ptr);
        assert!(result.is_ok());
        (schema, schema_ptr)
    }

    fn close_writer_and_cleanup_schema(filename: &str, schema_ptr: i64) {
        let _ = NativeParquetWriter::close_writer(filename.to_string());
        cleanup_ffi_schema(schema_ptr);
    }

    fn write_ffi_data_to_writer(filename: &str) -> (i64, i64) {
        let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
        let result = NativeParquetWriter::write_data(filename.to_string(), array_ptr, data_schema_ptr);
        assert!(result.is_ok());
        (array_ptr, data_schema_ptr)
    }

    #[test]
    fn test_create_writer_success() {
        let (_temp_dir, filename) = get_temp_file_path("test.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        assert!(WRITER_MANAGER.contains_key(&filename));
        assert!(FILE_MANAGER.contains_key(&filename));

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_create_writer_invalid_path() {
        let invalid_path = "/invalid/path/that/does/not/exist/test.parquet";
        let (_schema, schema_ptr) = create_test_ffi_schema();

        let result = NativeParquetWriter::create_writer(invalid_path.to_string(), schema_ptr);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No such file or directory"));
        
        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_create_writer_invalid_schema_pointer() {
        let (_temp_dir, filename) = get_temp_file_path("invalid_schema.parquet");

        // Test with null schema pointer
        let result = NativeParquetWriter::create_writer(filename, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid schema address"));
    }

    #[test]
    fn test_create_writer_multiple_times_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("duplicate.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Second writer creation for same file should fail
        let result2 = NativeParquetWriter::create_writer(filename.clone(), schema_ptr);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("Writer already exists"));

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_write_data_success() {
        let (_temp_dir, filename) = get_temp_file_path("write_ffi_test.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write data using complete FFI flow
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);

        // Cleanup FFI data
        cleanup_ffi_data(array_ptr, data_schema_ptr);
        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_write_data_no_writer() {
        let (array_ptr, schema_ptr) = create_test_ffi_data().unwrap();

        let result = NativeParquetWriter::write_data("nonexistent.parquet".to_string(), array_ptr, schema_ptr);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Writer not found"));

        cleanup_ffi_data(array_ptr, schema_ptr);
    }

    #[test]
    fn test_write_data_multiple_batches() {
        let (_temp_dir, filename) = get_temp_file_path("multi_write_ffi.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write multiple batches using FFI
        for _ in 0..3 {
            let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
            cleanup_ffi_data(array_ptr, data_schema_ptr);
        }

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_write_data_invalid_pointers() {
        let (_temp_dir, filename) = get_temp_file_path("invalid_ffi.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Test with schema and array pointers both null
        let result = NativeParquetWriter::write_data(filename.clone(), 0, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid FFI addresses"));

        // Test with one null pointer
        let result = NativeParquetWriter::write_data(filename.clone(), 0, schema_ptr);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid FFI addresses"));

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_close_writer_success() {
        let (_temp_dir, filename) = get_temp_file_path("test_close.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let result = NativeParquetWriter::close_writer(filename.clone());

        assert!(result.is_ok());
        assert!(!WRITER_MANAGER.contains_key(&filename));

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_close_nonexistent_writer() {
        let result = NativeParquetWriter::close_writer("nonexistent.parquet".to_string());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Writer not found"));
    }

    #[test]
    fn test_close_multiple_times_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("test.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let result1 = NativeParquetWriter::close_writer(filename.clone());
        assert!(result1.is_ok());
        let result2 = NativeParquetWriter::close_writer(filename);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("Writer not found"));
        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_flush_to_disk_success() {
        let (_temp_dir, filename) = get_temp_file_path("test_flush.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let result = NativeParquetWriter::flush_to_disk(filename.clone());
        assert!(result.is_ok());

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_flush_nonexistent_file() {
        let result = NativeParquetWriter::flush_to_disk("nonexistent.parquet".to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "File not found");
    }

    #[test]
    fn test_get_filtered_writer_memory_usage_with_writers() {
        let (_temp_dir, filename1) = get_temp_file_path("test1.parquet");
        let (_temp_dir, filename2) = get_temp_file_path("test2.parquet");
        let prefix = _temp_dir.path().to_string_lossy().to_string();
        let (_schema1, schema_ptr1) = create_writer_and_assert_success(&filename1);
        let (_schema2, schema_ptr2) = create_writer_and_assert_success(&filename2);

        let result = NativeParquetWriter::get_filtered_writer_memory_usage(prefix);
        assert!(result.is_ok());
        let _memory_usage = result.unwrap();
        assert!(_memory_usage >= 0);

        close_writer_and_cleanup_schema(&filename1, schema_ptr1);
        close_writer_and_cleanup_schema(&filename2, schema_ptr2);
    }

    #[test]
    fn test_complete_writer_lifecycle() {
        let (_temp_dir, filename) = get_temp_file_path("complete_workflow.parquet");
        let file_path = std::path::Path::new(&filename);

        // Step 1: Create schema and writer
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Step 2: Write multiple batches
        for _i in 0..3 {
            let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
            cleanup_ffi_data(array_ptr, data_schema_ptr);
        }

        // Step 3: Close writer
        assert!(NativeParquetWriter::close_writer(filename.clone()).is_ok());

        // Step 4: Flush to disk
        assert!(NativeParquetWriter::flush_to_disk(filename.clone()).is_ok());

        // Step 5: Verify file exists and has content
        assert!(file_path.exists());
        assert!(file_path.metadata().unwrap().len() > 0);

        cleanup_ffi_schema(schema_ptr);
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

                if NativeParquetWriter::create_writer(filename.clone(), schema_ptr).is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                    let _ = NativeParquetWriter::close_writer(filename);
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

        // Multiple threads trying to close the same writer
        for _ in 0..3 {
            let filename = filename.clone();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                if NativeParquetWriter::close_writer(filename).is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Only one thread should succeed in closing
        assert_eq!(success_count.load(Ordering::SeqCst), 1);
        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_concurrent_writes_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("concurrent_write_ffi.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Multiple threads writing to same file using FFI
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

        // Create writers for all files first
        for i in 0..file_count {
            let file_path = temp_dir.path().join(format!("concurrent_write_{}.parquet", i));
            let filename = file_path.to_string_lossy().to_string();
            let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
            filenames.push(filename);
            schema_ptrs.push(schema_ptr);
        }

        // Concurrent write operations to different files
        for i in 0..file_count {
            let filename = filenames[i].clone();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                // Write multiple batches to this file
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

        // All write operations should succeed (file_count * 2 batches per file)
        assert_eq!(success_count.load(Ordering::SeqCst), file_count * 2);

        // Cleanup
        for (i, filename) in filenames.iter().enumerate() {
            close_writer_and_cleanup_schema(filename, schema_ptrs[i]);
        }
    }
}
