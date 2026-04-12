/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use lazy_static::lazy_static;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::metadata::ParquetMetaData;
use parquet::format::FileMetaData as FormatFileMetaData;
use std::fs::File;
use std::sync::{Arc, Mutex};

use crate::{log_info, log_error, log_debug};

lazy_static! {
    pub static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<File>>>> = DashMap::new();
    pub static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
}

pub struct NativeParquetWriter;

impl NativeParquetWriter {
    pub fn create_writer(filename: String, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!("create_writer called for file: {}, schema_address: {}", filename, schema_address);

        if (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid schema address (null pointer) for file: {}", filename);
            return Err("Invalid schema address".into());
        }
        if WRITER_MANAGER.contains_key(&filename) {
            log_error!("ERROR: Writer already exists for file: {}", filename);
            return Err("Writer already exists for this file".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);
        log_debug!("Schema created with {} fields", schema.fields().len());

        let file = File::create(&filename)?;
        let file_clone = file.try_clone()?;
        FILE_MANAGER.insert(filename.clone(), file_clone);

        let props = WriterProperties::builder()
            .set_compression(Compression::LZ4_RAW)
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_fpp(0.1)
            .set_bloom_filter_ndv(100000)
            .build();
        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        WRITER_MANAGER.insert(filename, Arc::new(Mutex::new(writer)));
        Ok(())
    }

    pub fn write_data(filename: String, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!("write_data called for file: {}", filename);

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid FFI addresses for file: {}", filename);
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
                log_debug!("Created RecordBatch with {} rows and {} columns", record_batch.num_rows(), record_batch.num_columns());

                if let Some(writer_arc) = WRITER_MANAGER.get(&filename) {
                    let mut writer = writer_arc.lock().unwrap();
                    writer.write(&record_batch)?;
                    Ok(())
                } else {
                    log_error!("ERROR: No writer found for file: {}", filename);
                    Err("Writer not found".into())
                }
            } else {
                log_error!("ERROR: Array is not a StructArray, type: {:?}", array.data_type());
                Err("Expected struct array from VectorSchemaRoot".into())
            }
        }
    }

    pub fn finalize_writer(filename: String) -> Result<Option<FormatFileMetaData>, Box<dyn std::error::Error>> {
        log_debug!("finalize_writer called for file: {}", filename);

        if let Some((_, writer_arc)) = WRITER_MANAGER.remove(&filename) {
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let writer = mutex.into_inner().unwrap();
                    let parquet_metadata = writer.close()?;
                    let file_metadata = parquet_metadata.file_metadata();
                    log_debug!("Successfully closed writer for file: {}, num_rows={}", filename, file_metadata.num_rows());
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
                    Ok(Some(format_metadata))
                }
                Err(_) => {
                    log_error!("ERROR: Writer still in use for file: {}", filename);
                    Err("Writer still in use".into())
                }
            }
        } else {
            log_error!("ERROR: Writer not found for file: {}", filename);
            Err("Writer not found".into())
        }
    }

    pub fn sync_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!("sync_to_disk called for file: {}", filename);

        if let Some(file) = FILE_MANAGER.get_mut(&filename) {
            file.sync_all()?;
            log_debug!("Successfully fsynced file: {}", filename);
            drop(file);
            FILE_MANAGER.remove(&filename);
            Ok(())
        } else {
            log_error!("ERROR: File not found for fsync: {}", filename);
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
        log_debug!("Metadata for {}: version={}, num_rows={}", filename, file_metadata.version(), file_metadata.num_rows());
        Ok(file_metadata)
    }
}
