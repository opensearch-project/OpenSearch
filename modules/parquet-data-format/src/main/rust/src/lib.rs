use jni::objects::{JClass, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use dashmap::DashMap;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::{Arc, Mutex};
use lazy_static::lazy_static;
use arrow::ffi::{FFI_ArrowSchema, FFI_ArrowArray};
use std::fs::OpenOptions;
use std::io::Write;
use chrono::Utc;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

pub mod parquet_merge;
pub use parquet_merge::*;

lazy_static! {
    static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<File>>>> = DashMap::new();
    static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
}

struct NativeParquetWriter;

impl NativeParquetWriter {

    fn create_writer(filename: String, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        let log_msg = format!("[RUST] create_writer called for file: {}, schema_address: {}\n", filename, schema_address);
        println!("{}", log_msg.trim());
        Self::log_to_file(&log_msg);

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);

        let schema_msg = format!("[RUST] Schema created with {} fields\n", schema.fields().len());
        println!("{}", schema_msg.trim());
        Self::log_to_file(&schema_msg);

        for (i, field) in schema.fields().iter().enumerate() {
            let field_msg = format!("[RUST] Field {}: {} ({})\n", i, field.name(), field.data_type());
            println!("{}", field_msg.trim());
            Self::log_to_file(&field_msg);
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
        let log_msg = format!("[RUST] write_data called for file: {}, array_address: {}, schema_address: {}\n", filename, array_address, schema_address);
        println!("{}", log_msg.trim());
        Self::log_to_file(&log_msg);

        unsafe {
            let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
            let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);

            match arrow::ffi::from_ffi(arrow_array, &arrow_schema) {
                Ok(array_data) => {
                    let data_msg = format!("[RUST] Successfully imported array_data, length: {}\n", array_data.len());
                    println!("{}", data_msg.trim());
                    Self::log_to_file(&data_msg);

                    let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);
                    let array_msg = format!("[RUST] Array type: {:?}, length: {}\n", array.data_type(), array.len());
                    println!("{}", array_msg.trim());
                    Self::log_to_file(&array_msg);

                    if let Some(struct_array) = array.as_any().downcast_ref::<arrow::array::StructArray>() {
                        let struct_msg = format!("[RUST] Successfully cast to StructArray with {} columns\n", struct_array.num_columns());
                        println!("{}", struct_msg.trim());
                        Self::log_to_file(&struct_msg);

                        let schema = Arc::new(arrow::datatypes::Schema::new(
                            struct_array.fields().clone()
                        ));

                        let record_batch = RecordBatch::try_new(
                            schema.clone(),
                            struct_array.columns().to_vec(),
                        )?;

                        let batch_msg = format!("[RUST] Created RecordBatch with {} rows and {} columns\n", record_batch.num_rows(), record_batch.num_columns());
                        println!("{}", batch_msg.trim());
                        Self::log_to_file(&batch_msg);

                        if let Some(writer_arc) = WRITER_MANAGER.get(&filename) {
                            let write_msg = "[RUST] Writing RecordBatch to file\n";
                            println!("{}", write_msg.trim());
                            Self::log_to_file(write_msg);
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                            let success_msg = "[RUST] Successfully wrote RecordBatch\n";
                            println!("{}", success_msg.trim());
                            Self::log_to_file(success_msg);
                        } else {
                            let error_msg = format!("[RUST] ERROR: No writer found for file: {}\n", filename);
                            println!("{}", error_msg.trim());
                            Self::log_to_file(&error_msg);
                        }
                        Ok(())
                    } else {
                        let error_msg = format!("[RUST] ERROR: Array is not a StructArray, type: {:?}\n", array.data_type());
                        println!("{}", error_msg.trim());
                        Self::log_to_file(&error_msg);
                        Err("Expected struct array from VectorSchemaRoot".into())
                    }
                }
                Err(e) => {
                    let error_msg = format!("[RUST] ERROR: Failed to import from FFI: {:?}\n", e);
                    println!("{}", error_msg.trim());
                    Self::log_to_file(&error_msg);
                    Err(e.into())
                }
            }
        }
    }

    fn close_writer(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        let log_msg = format!("[RUST] close_writer called for file: {}\n", filename);
        println!("{}", log_msg.trim());
        Self::log_to_file(&log_msg);

        if let Some((_, writer_arc)) = WRITER_MANAGER.remove(&filename) {
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let mut writer = mutex.into_inner().unwrap();
                    match writer.close() {
                        Ok(_) => {
                            let success_msg = format!("[RUST] Successfully closed writer for file: {}\n", filename);
                            println!("{}", success_msg.trim());
                            Self::log_to_file(&success_msg);
                            Ok(())
                        }
                        Err(e) => {
                            let error_msg = format!("[RUST] ERROR: Failed to close writer for file: {}\n", filename);
                            println!("{}", error_msg.trim());
                            Self::log_to_file(&error_msg);
                            Err(e.into())
                        }
                    }
                }
                Err(_) => {
                    let error_msg = format!("[RUST] ERROR: Writer still in use for file: {}\n", filename);
                    println!("{}", error_msg.trim());
                    Self::log_to_file(&error_msg);
                    Err("Writer still in use".into())
                }
            }
        } else {
            Ok(())
        }
    }

    fn flush_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        let log_msg = format!("[RUST] fsync_file called for file: {}\n", filename);
        println!("{}", log_msg.trim());
        Self::log_to_file(&log_msg);

        if let Some(mut file) = FILE_MANAGER.get_mut(&filename) {
            match file.sync_all() {
                Ok(_) => {
                    let success_msg = format!("[RUST] Successfully fsynced file: {}\n", filename);
                    println!("{}", success_msg.trim());
                    Self::log_to_file(&success_msg);
                    Ok(())
                }
                Err(e) => {
                    let error_msg = format!("[RUST] ERROR: Failed to fsync file: {}\n", filename);
                    println!("{}", error_msg.trim());
                    Self::log_to_file(&error_msg);
                    Err(e.into())
                }
            }
        } else {
            let error_msg = format!("[RUST] ERROR: File not found for fsync: {}\n", filename);
            println!("{}", error_msg.trim());
            Self::log_to_file(&error_msg);
            Err("File not found".into())
        }
    }

    fn get_writer_memory_usage(filename: String) -> Result<usize, Box<dyn std::error::Error>> {
        let log_msg = format!("[RUST] get_writer_memory_usage called for file: {}\n", filename);
        println!("{}", log_msg.trim());
        Self::log_to_file(&log_msg);

        if let Some(writer_arc) = WRITER_MANAGER.get(&filename) {
            let writer = writer_arc.lock().unwrap();
            let memory_usage = writer.memory_size();
            let usage_msg = format!("[RUST] Memory usage for {}: {} bytes\n", filename, memory_usage);
            println!("{}", usage_msg.trim());
            Self::log_to_file(&usage_msg);
            Ok(memory_usage)
        } else {
            let not_found_msg = format!("[RUST] No writer found for file: {}\n", filename);
            println!("{}", not_found_msg.trim());
            Self::log_to_file(&not_found_msg);
            Ok(0)
        }
    }

    fn log_to_file(message: &str) {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("/tmp/rust_parquet_debug.log") {
            let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f UTC");
            let timestamped_message = format!("[{}] {}", timestamp, message);
            let _ = file.write_all(timestamped_message.as_bytes());
        }
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
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_getNativeBytesUsed(
    mut env: JNIEnv,
    _class: JClass,
    file: JString
) -> jlong {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::get_writer_memory_usage(filename) {
        Ok(memory_usage) => memory_usage as jlong,
        Err(_) => 0,
    }
}
