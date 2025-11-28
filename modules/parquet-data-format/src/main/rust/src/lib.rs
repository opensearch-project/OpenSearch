use jni::objects::{JClass, JString};
use jni::sys::{jint, jlong};
use jni::{JNIEnv, JavaVM};
use dashmap::DashMap;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use arrow::ffi::{FFI_ArrowSchema, FFI_ArrowArray};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

pub mod logger;
pub mod parquet_merge;
pub use parquet_merge::*;

lazy_static! {
    static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<File>>>> = DashMap::new();
    static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
}

struct NativeParquetWriter;

impl NativeParquetWriter {

    fn create_writer(filename: String, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        logger::log_info(&format!("[RUST] create_writer called for file: {}, schema_address: {}", filename, schema_address));

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
                        } else {
                            logger::log_error(&format!("[RUST] ERROR: No writer found for file: {}", filename));
                        }
                        Ok(())
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
                    let mut writer = mutex.into_inner().unwrap();
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
            Ok(())
        }
    }

    fn flush_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        logger::log_info(&format!("[RUST] fsync_file called for file: {}", filename));

        if let Some(file) = FILE_MANAGER.get_mut(&filename) {
            match file.sync_all() {
                Ok(_) => {
                    logger::log_info(&format!("[RUST] Successfully fsynced file: {}", filename));
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
