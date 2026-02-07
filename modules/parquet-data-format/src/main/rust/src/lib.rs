use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::compute::{sort_to_indices, take};
use dashmap::DashMap;
use jni::objects::{JClass, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use lazy_static::lazy_static;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::schema::types::ColumnPath;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
pub mod profiler;


pub mod logger;
// pub mod parquet_merge;
// pub mod parquet_merge_polars;
pub mod parquet_merge_hybrid;
pub mod rate_limited_writer;
pub mod parquet_merge_stream;

// pub use parquet_merge::*;
// pub use parquet_merge_polars::*;
pub use parquet_merge_hybrid::*;
pub use parquet_merge_stream::*;

// Re-export macros from the shared crate for logging
pub use vectorized_exec_spi::{log_info, log_error, log_debug};

lazy_static! {
    static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<File>>>> = DashMap::new();
    static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
    static ref PROPS_MANAGER: DashMap<String, WriterProperties> = DashMap::new();
}

struct NativeParquetWriter;

impl NativeParquetWriter {

    fn create_writer(filename: String, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] create_writer called for file: {}, schema_address: {}", filename, schema_address);

        if (schema_address as *mut u8).is_null() {
            log_error!("[RUST] ERROR: Invalid schema address (null pointer) for file: {}, schema_address: {}", filename, schema_address);
            return Err("Invalid schema address".into());
        }

        // Create temporary filename with "temp" prefix
        let path = Path::new(&filename);
        let temp_filename = path.parent()
            .unwrap_or_else(|| Path::new(""))
            .join(format!("temp-{}", path.file_name().unwrap().to_str().unwrap()))
            .to_string_lossy()
            .to_string();

        if WRITER_MANAGER.contains_key(&temp_filename) {
            log_error!("[RUST] ERROR: Writer already exists for file: {}", temp_filename);
            return Err("Writer already exists for this file".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);

        log_info!("[RUST] Schema created with {} fields", schema.fields().len());

        for (i, field) in schema.fields().iter().enumerate() {
            log_debug!("[RUST] Field {}: {} ({})", i, field.name(), field.data_type());
        }

        let file = File::create(&temp_filename)?;
        let file_clone = file.try_clone()?;
        FILE_MANAGER.insert(temp_filename.clone(), file_clone);
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();

        // Store properties and original filename for later use in sorting
        PROPS_MANAGER.insert(temp_filename.clone(), props.clone());
        PROPS_MANAGER.insert(format!("{}_original", temp_filename), WriterProperties::builder().build()); // Store original filename as a marker

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        WRITER_MANAGER.insert(temp_filename, Arc::new(Mutex::new(writer)));
        Ok(())
    }

    fn write_data(filename: String, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        // Convert original filename to temp filename
        let path = Path::new(&filename);
        let temp_filename = path.parent()
            .unwrap_or_else(|| Path::new(""))
            .join(format!("temp-{}", path.file_name().unwrap().to_str().unwrap()))
            .to_string_lossy()
            .to_string();

        log_info!("[RUST] write_data called for file: {} (temp: {}), array_address: {}, schema_address: {}", filename, temp_filename, array_address, schema_address);

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!("[RUST] ERROR: Invalid FFI addresses for file: {}, array_address: {}, schema_address: {}", temp_filename, array_address, schema_address);
            return Err("Invalid FFI addresses (null pointers)".into());
        }

        unsafe {
            let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
            let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);

            match arrow::ffi::from_ffi(arrow_array, &arrow_schema) {
                Ok(array_data) => {
                    log_debug!("[RUST] Successfully imported array_data, length: {}", array_data.len());

                    let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);
                    log_debug!("[RUST] Array type: {:?}, length: {}", array.data_type(), array.len());

                    if let Some(struct_array) = array.as_any().downcast_ref::<arrow::array::StructArray>() {
                        log_debug!("[RUST] Successfully cast to StructArray with {} columns", struct_array.num_columns());

                        let schema = Arc::new(arrow::datatypes::Schema::new(
                            struct_array.fields().clone()
                        ));

                        let record_batch = RecordBatch::try_new(
                            schema.clone(),
                            struct_array.columns().to_vec(),
                        )?;

                        log_info!("[RUST] Created RecordBatch with {} rows and {} columns", record_batch.num_rows(), record_batch.num_columns());

                        if let Some(writer_arc) = WRITER_MANAGER.get(&temp_filename) {
                            log_debug!("[RUST] Writing RecordBatch to temp file");
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                            log_info!("[RUST] Successfully wrote RecordBatch");
                            Ok(())
                        } else {
                            log_error!("[RUST] ERROR: No writer found for temp file: {}", temp_filename);
                            Err("Writer not found".into())
                        }
                    } else {
                        log_error!("[RUST] ERROR: Array is not a StructArray, type: {:?}", array.data_type());
                        Err("Expected struct array from VectorSchemaRoot".into())
                    }
                }
                Err(e) => {
                    log_error!("[RUST] ERROR: Failed to import from FFI: {:?}", e);
                    Err(e.into())
                }
            }
        }
    }

    fn close_writer(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        // Convert original filename to temp filename
        let path = Path::new(&filename);
        let temp_filename = path.parent()
            .unwrap_or_else(|| Path::new(""))
            .join(format!("temp-{}", path.file_name().unwrap().to_str().unwrap()))
            .to_string_lossy()
            .to_string();

        log_info!("[RUST] close_writer called for file: {} (temp: {})", filename, temp_filename);

        if let Some((_, writer_arc)) = WRITER_MANAGER.remove(&temp_filename) {
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let writer = mutex.into_inner().unwrap();
                    match writer.close() {
                        Ok(_) => {
                            log_info!("[RUST] Successfully closed writer for temp file: {}", temp_filename);

                            // Sort and rewrite the file
                            Self::sort_and_rewrite_parquet(&temp_filename)?;

                            Ok(())
                        }
                        Err(e) => {
                            log_error!("[RUST] ERROR: Failed to close writer for temp file: {}", temp_filename);
                            Err(e.into())
                        }
                    }
                }
                Err(_) => {
                    log_error!("[RUST] ERROR: Writer still in use for temp file: {}", temp_filename);
                    Err("Writer still in use".into())
                }
            }
        } else {
            log_error!("[RUST] ERROR: Writer not found for temp file: {}\n", temp_filename);
            Err("Writer not found".into())
        }
    }

    fn sort_and_rewrite_parquet(filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] Sorting parquet file by timestamp: {}", filename);

        // Use stored properties from create_writer or fallback to reading from file
        let props = if let Some((_, stored_props)) = PROPS_MANAGER.remove(filename) {
            log_debug!("[RUST] Using stored writer properties");
            stored_props
        } else {
            log_debug!("[RUST] Reading properties from file metadata");
            let file = File::open(filename)?;
            let reader = SerializedFileReader::new(file)?;
            let metadata = reader.metadata();
            let mut props_builder = WriterProperties::builder();

            if let Some(row_group) = metadata.row_groups().get(0) {
                for (col_idx, column_chunk) in row_group.columns().iter().enumerate() {
                    let compression = column_chunk.compression();
                    let encoding = column_chunk.encodings().get(0).copied().unwrap_or(parquet::basic::Encoding::PLAIN);

                    props_builder = props_builder
                        .set_column_compression(ColumnPath::new(vec![format!("column_{}", col_idx)]), compression)
                        .set_column_encoding(ColumnPath::new(vec![format!("column_{}", col_idx)]), encoding);
                }
            }
            props_builder.build()
        };

        // Check file size to decide sorting strategy
        let file_size = std::fs::metadata(filename)?.len();
        const MAX_MEMORY_SIZE: u64 = 32 * 1024 * 1024; // 32MB threshold

        if file_size <= MAX_MEMORY_SIZE {
            Self::sort_small_file(filename, props)
        } else {
            Self::sort_large_file(filename, props)
        }
    }

    fn sort_small_file(filename: &str, props: WriterProperties) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] Using in-memory sort for small file: {}", filename);

        let file = File::open(filename)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut arrow_reader = builder.with_batch_size(2048).build()?;

        let mut batches = Vec::new();
        for batch_result in arrow_reader {
            batches.push(batch_result?);
        }

        if batches.is_empty() {
            log_info!("[RUST] No data to sort in file: {}", filename);
            return Ok(());
        }

        let schema = batches[0].schema();
        let combined_batch = arrow::compute::concat_batches(&schema, &batches)?;

        let timestamp_col_idx = schema.fields().iter().position(|f| f.name() == "EventDate")
            .ok_or("timestamp column not found")?;

        let timestamp_array = combined_batch.column(timestamp_col_idx);
        let sort_indices = sort_to_indices(timestamp_array, None, None)?;

        let sorted_columns: Result<Vec<_>, _> = combined_batch.columns().iter()
            .map(|col| take(col, &sort_indices, None))
            .collect();
        let sorted_columns = sorted_columns?;

        let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_columns)?;

        Self::write_sorted_file(filename, &sorted_batch, schema, props)?;
        log_info!("[RUST] Sorted file written: {}", filename);
        std::fs::remove_file(filename)?;

        Ok(())
    }

    fn sort_large_file(filename: &str, props: WriterProperties) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] Using streaming sort for large file: {}", filename);

        let file = File::open(filename)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let mut arrow_reader = builder.with_batch_size(8192).build()?;

        let mut temp_files = Vec::new();
        let mut batch_count = 0;
        let temp_dir = std::env::temp_dir();

        // Sort each batch individually and write to temp files
        for batch_result in arrow_reader {
            let batch = batch_result?;
            let schema = batch.schema();

            let timestamp_col_idx = schema.fields().iter().position(|f| f.name() == "EventDate")
                .ok_or("timestamp column not found")?;

            let timestamp_array = batch.column(timestamp_col_idx);
            let sort_indices = sort_to_indices(timestamp_array, None, None)?;

            let sorted_columns: Result<Vec<_>, _> = batch.columns().iter()
                .map(|col| take(col, &sort_indices, None))
                .collect();
            let sorted_columns = sorted_columns?;

            let sorted_batch = RecordBatch::try_new(schema.clone(), sorted_columns)?;

            // Write sorted batch to temporary file
            let temp_filename = temp_dir.join(format!("sort_temp_{}_{}.parquet", batch_count, std::process::id()));
            let temp_file = File::create(&temp_filename)?;
            let mut temp_writer = ArrowWriter::try_new(temp_file, schema.clone(), Some(props.clone()))?;
            temp_writer.write(&sorted_batch)?;
            temp_writer.close()?;

            temp_files.push(temp_filename);
            batch_count += 1;
        }

        if temp_files.is_empty() {
            log_info!("[RUST] No data to sort in file: {}", filename);
            return Ok(());
        }

        log_info!("[RUST] Created {} temp files, now merging", batch_count);

        // Merge temp files using streaming merge
        let merged_batch = Self::merge_temp_files(temp_files)?;

        Self::write_sorted_file(filename, &merged_batch, merged_batch.schema(), props)?;
        std::fs::remove_file(filename)?;

        Ok(())
    }

    fn merge_temp_files(temp_files: Vec<std::path::PathBuf>) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        if temp_files.len() == 1 {
            let file = File::open(&temp_files[0])?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let mut reader = builder.build()?;
            let batch = reader.next().unwrap()?;
            std::fs::remove_file(&temp_files[0])?;
            return Ok(batch);
        }

        // For simplicity, read all temp files and merge (still better than keeping all in memory during processing)
        let mut all_batches = Vec::new();
        for temp_file in &temp_files {
            let file = File::open(temp_file)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let mut reader = builder.build()?;
            for batch_result in reader {
                all_batches.push(batch_result?);
            }
        }

        // Clean up temp files
        for temp_file in temp_files {
            let _ = std::fs::remove_file(temp_file);
        }

        if all_batches.is_empty() {
            return Err("No batches to merge".into());
        }

        let schema = all_batches[0].schema();
        let combined = arrow::compute::concat_batches(&schema, &all_batches)?;

        let timestamp_col_idx = schema.fields().iter().position(|f| f.name() == "EventDate")
            .ok_or("timestamp column not found")?;

        let timestamp_array = combined.column(timestamp_col_idx);
        let sort_indices = sort_to_indices(timestamp_array, None, None)?;

        let sorted_columns: Result<Vec<_>, _> = combined.columns().iter()
            .map(|col| take(col, &sort_indices, None))
            .collect();
        let sorted_columns = sorted_columns?;

        RecordBatch::try_new(schema, sorted_columns).map_err(|e| e.into())
    }

    fn write_sorted_file(temp_filename: &str, batch: &RecordBatch, schema: Arc<arrow::datatypes::Schema>, props: WriterProperties) -> Result<(), Box<dyn std::error::Error>> {
        use arrow::array::Int64Array;

        // Extract original filename from temp filename
        let temp_path = Path::new(temp_filename);
        let original_filename = if let Some(temp_name) = temp_path.file_name().and_then(|n| n.to_str()) {
            if temp_name.starts_with("temp-") {
                temp_path.parent()
                    .unwrap_or_else(|| Path::new(""))
                    .join(&temp_name[5..]) // Remove "temp-" prefix
                    .to_string_lossy()
                    .to_string()
            } else {
                temp_filename.to_string()
            }
        } else {
            temp_filename.to_string()
        };

        // Check if ___row_id column exists and rewrite it with sequential values
        let final_batch = if let Some(row_id_idx) = schema.fields().iter().position(|f| f.name() == "___row_id") {
            log_info!("[RUST] Rewriting ___row_id column with sequential values starting from 0");

            let row_count = batch.num_rows();
            let sequential_ids = Int64Array::from_iter_values((0..row_count as u64).map(|x| x as i64));

            let mut new_columns = batch.columns().to_vec();
            new_columns[row_id_idx] = Arc::new(sequential_ids);

            match RecordBatch::try_new(schema.clone(), new_columns) {
                Ok(batch) => {
                    log_info!("[RUST] Successfully rewrote row id, now moving to write sorted file: {}", original_filename);
                    batch
                }
                Err(e) => {
                    log_error!("[RUST] Failed to create batch with rewritten row_id: {}", e);
                    return Err(e.into());
                }
            }
        } else {
            log_info!("[RUST] No ___row_id column found, proceeding with original batch to write sorted file: {}", original_filename);
            batch.clone()
        };

        let sorted_file = File::create(&original_filename)?;
        let mut sorted_writer = ArrowWriter::try_new(sorted_file, schema, Some(props))?;

        sorted_writer.write(&final_batch)?;
        sorted_writer.close()?;

        log_info!("[RUST] Successfully created sorted file: {}", original_filename);
        Ok(())
    }

    fn flush_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        // Convert original filename to temp filename
        let path = Path::new(&filename);
        let temp_filename = path.parent()
            .unwrap_or_else(|| Path::new(""))
            .join(format!("temp-{}", path.file_name().unwrap().to_str().unwrap()))
            .to_string_lossy()
            .to_string();

        log_info!("[RUST] fsync_file called for file: {} (temp: {})", filename, temp_filename);

        if let Some(file) = FILE_MANAGER.get_mut(&temp_filename) {
            match file.sync_all() {
                Ok(_) => {
                    log_info!("[RUST] Successfully fsynced temp file: {}", temp_filename);
                    drop(file);
                    FILE_MANAGER.remove(&temp_filename);
                    Ok(())
                }
                Err(e) => {
                    log_error!("[RUST] ERROR: Failed to fsync temp file: {}", temp_filename);
                    Err(e.into())
                }
            }
        } else {
            log_error!("[RUST] ERROR: Temp file not found for fsync: {}", temp_filename);
            Err("File not found".into())
        }
    }

    fn get_filtered_writer_memory_usage(path_prefix: String) -> Result<usize, Box<dyn std::error::Error>> {
        log_debug!("[RUST] get_filtered_writer_memory_usage called with prefix: {}", path_prefix);

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

                    log_debug!("[RUST] Filtered Writer {}: {} bytes", filename, memory_usage);
                }
            }
        }

        log_debug!("[RUST] Total memory usage across {} filtered ArrowWriters (prefix: {}): {} bytes", writer_count, path_prefix, total_memory);

        Ok(total_memory)
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    // Initialize the logger using the shared crate
    vectorized_exec_spi::logger::init_logger_from_env(&env);
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
