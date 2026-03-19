/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong, jobject};
use jni::JNIEnv;
use lazy_static::lazy_static;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::format::FileMetaData as FormatFileMetaData;
use std::fs::File;
use std::sync::{Arc, Mutex};


#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(test)]
mod tests;

pub use native_bridge_spi::{log_info, log_error, log_debug};

lazy_static! {
    pub static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<File>>>> = DashMap::new();
    pub static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
}

pub struct NativeParquetWriter;

impl NativeParquetWriter {
    pub fn create_writer(filename: String, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] create_writer called for file: {}, schema_address: {}", filename, schema_address);

        if (schema_address as *mut u8).is_null() {
            log_error!("[RUST] ERROR: Invalid schema address (null pointer) for file: {}", filename);
            return Err("Invalid schema address".into());
        }
        if WRITER_MANAGER.contains_key(&filename) {
            log_error!("[RUST] ERROR: Writer already exists for file: {}", filename);
            return Err("Writer already exists for this file".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);
        log_info!("[RUST] Schema created with {} fields", schema.fields().len());

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

    pub fn write_data(filename: String, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] write_data called for file: {}", filename);

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!("[RUST] ERROR: Invalid FFI addresses for file: {}", filename);
            return Err("Invalid FFI addresses (null pointers)".into());
        }

        unsafe {
            let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
            let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);
            let array_data = arrow::ffi::from_ffi(arrow_array, &arrow_schema)?;
            let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);

            if let Some(struct_array) = array.as_any().downcast_ref::<arrow::array::StructArray>() {
                let schema = Arc::new(arrow::datatypes::Schema::new(struct_array.fields().clone()));
                let record_batch = RecordBatch::try_new(schema, struct_array.columns().to_vec())?;
                log_info!("[RUST] Created RecordBatch with {} rows and {} columns", record_batch.num_rows(), record_batch.num_columns());

                if let Some(writer_arc) = WRITER_MANAGER.get(&filename) {
                    let mut writer = writer_arc.lock().unwrap();
                    writer.write(&record_batch)?;
                    Ok(())
                } else {
                    log_error!("[RUST] ERROR: No writer found for file: {}", filename);
                    Err("Writer not found".into())
                }
            } else {
                log_error!("[RUST] ERROR: Array is not a StructArray, type: {:?}", array.data_type());
                Err("Expected struct array from VectorSchemaRoot".into())
            }
        }
    }

    pub fn close_writer(filename: String) -> Result<Option<FormatFileMetaData>, Box<dyn std::error::Error>> {
        log_info!("[RUST] close_writer called for file: {}", filename);

        if let Some((_, writer_arc)) = WRITER_MANAGER.remove(&filename) {
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let writer = mutex.into_inner().unwrap();
                    let file_metadata = writer.close()?;
                    log_info!("[RUST] Successfully closed writer for file: {}, num_rows={}", filename, file_metadata.num_rows);
                    Ok(Some(file_metadata))
                }
                Err(_) => {
                    log_error!("[RUST] ERROR: Writer still in use for file: {}", filename);
                    Err("Writer still in use".into())
                }
            }
        } else {
            log_error!("[RUST] ERROR: Writer not found for file: {}", filename);
            Err("Writer not found".into())
        }
    }

    pub fn flush_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] fsync_file called for file: {}", filename);

        if let Some(file) = FILE_MANAGER.get_mut(&filename) {
            file.sync_all()?;
            log_info!("[RUST] Successfully fsynced file: {}", filename);
            drop(file);
            FILE_MANAGER.remove(&filename);
            Ok(())
        } else {
            log_error!("[RUST] ERROR: File not found for fsync: {}", filename);
            Err("File not found".into())
        }
    }

    pub fn get_filtered_writer_memory_usage(path_prefix: String) -> Result<usize, Box<dyn std::error::Error>> {
        let mut total_memory = 0;
        for entry in WRITER_MANAGER.iter() {
            if entry.key().starts_with(&path_prefix) {
                if let Ok(writer) = entry.value().lock() {
                    total_memory += writer.memory_size();
                }
            }
        }
        Ok(total_memory)
    }

    pub fn get_file_metadata(filename: String) -> Result<parquet::file::metadata::FileMetaData, Box<dyn std::error::Error>> {
        let file = File::open(&filename)?;
        let reader = SerializedFileReader::new(file)?;
        let file_metadata = reader.metadata().file_metadata().clone();
        log_debug!("[RUST] Metadata for {}: version={}, num_rows={}", filename, file_metadata.version(), file_metadata.num_rows());
        Ok(file_metadata)
    }

    fn create_java_metadata<'local>(env: &mut JNIEnv<'local>, metadata: &FormatFileMetaData) -> Result<JObject<'local>, Box<dyn std::error::Error>> {
        let class = env.find_class("org/opensearch/parquet/bridge/ParquetFileMetadata")?;
        let created_by_jstring = match &metadata.created_by {
            Some(created_by) => env.new_string(created_by)?,
            None => JObject::null().into(),
        };
        let java_metadata = env.new_object(
            &class,
            "(IJLjava/lang/String;)V",
            &[
                (metadata.version).into(),
                (metadata.num_rows).into(),
                (&created_by_jstring).into(),
            ],
        )?;
        Ok(java_metadata)
    }
}

// --- JNI exports ---

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    native_bridge_spi::init_logger_from_env(&env);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_createWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    schema_address: jlong,
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::create_writer(filename, schema_address as i64) {
        Ok(_) => 0,
        Err(e) => {
            log_error!("[RUST] ERROR: Failed to create writer: {:?}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_write(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    array_address: jlong,
    schema_address: jlong,
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::write_data(filename, array_address as i64, schema_address as i64) {
        Ok(_) => 0,
        Err(e) => {
            log_error!("[RUST] ERROR: Failed to write data: {:?}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_closeWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::close_writer(filename) {
        Ok(Some(metadata)) => {
            match NativeParquetWriter::create_java_metadata(&mut env, &metadata) {
                Ok(java_obj) => java_obj.into_raw(),
                Err(e) => {
                    log_error!("[RUST] ERROR: Failed to create Java metadata object: {:?}", e);
                    let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                    JObject::null().into_raw()
                }
            }
        }
        Ok(None) => JObject::null().into_raw(),
        Err(e) => {
            log_error!("[RUST] ERROR: Failed to close writer: {:?}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to close writer: {}", e));
            JObject::null().into_raw()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_flushToDisk(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::flush_to_disk(filename) {
        Ok(_) => 0,
        Err(e) => {
            log_error!("[RUST] ERROR: Failed to flush to disk: {:?}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_getFilteredNativeBytesUsed(
    mut env: JNIEnv,
    _class: JClass,
    path_prefix: JString,
) -> jlong {
    let prefix: String = env.get_string(&path_prefix).expect("Couldn't get java string!").into();
    match NativeParquetWriter::get_filtered_writer_memory_usage(prefix) {
        Ok(memory) => memory as jlong,
        Err(e) => {
            log_error!("[RUST] ERROR: Failed to get filtered native bytes used: {:?}", e);
            0
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_getFileMetadata(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::get_file_metadata(filename) {
        Ok(file_metadata) => {
            // Convert FileMetaData to thrift FormatFileMetaData for create_java_metadata
            let format_metadata = FormatFileMetaData {
                version: file_metadata.version(),
                num_rows: file_metadata.num_rows(),
                created_by: file_metadata.created_by().map(|s| s.to_string()),
                schema: vec![],
                row_groups: vec![],
                key_value_metadata: None,
                encryption_algorithm: None,
                footer_signing_key_metadata: None,
                column_orders: None,
            };
            match NativeParquetWriter::create_java_metadata(&mut env, &format_metadata) {
                Ok(java_obj) => java_obj.into_raw(),
                Err(e) => {
                    log_error!("[RUST] ERROR: Failed to create Java metadata: {:?}", e);
                    let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                    JObject::null().into_raw()
                }
            }
        }
        Err(e) => {
            log_error!("[RUST] ERROR: Failed to get file metadata: {:?}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to get metadata: {}", e));
            JObject::null().into_raw()
        }
    }
}
