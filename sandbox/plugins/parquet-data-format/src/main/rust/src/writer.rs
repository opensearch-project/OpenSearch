/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::format::FileMetaData as FormatFileMetaData;
use std::fs::File;
use std::sync::Arc;

use crate::{log_debug, log_error, RUNTIME};

/// Owns a single async Parquet writer backed by an [`ObjectStore`] via
/// [`ParquetObjectWriter`] from arrow-rs.
///
/// Each instance is heap-allocated and handed to Java as an opaque `jlong`
/// pointer. The static tokio runtime ([`RUNTIME`]) is used to drive async
/// operations from synchronous JNI calls.
pub struct NativeParquetWriter {
    writer: Option<AsyncArrowWriter<ParquetObjectWriter>>,
    path: String,
}

impl std::fmt::Debug for NativeParquetWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeParquetWriter")
            .field("path", &self.path)
            .field("finalized", &self.writer.is_none())
            .finish()
    }
}

impl NativeParquetWriter {
    /// Creates a new async writer targeting `path` in the given `store`.
    pub fn create(
        store: Arc<dyn ObjectStore>,
        path: String,
        schema_address: i64,
    ) -> Result<Box<Self>, Box<dyn std::error::Error>> {
        log_debug!("create called for path: {}, schema_address: {}", path, schema_address);

        if (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid schema address (null pointer) for path: {}", path);
            return Err("Invalid schema address".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);
        log_debug!("Schema created with {} fields", schema.fields().len());

        let object_path = ObjectPath::from(path.as_str());
        let object_writer = ParquetObjectWriter::new(store, object_path);

        let props = WriterProperties::builder()
            .set_compression(Compression::LZ4_RAW)
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_fpp(0.1)
            .set_bloom_filter_ndv(100000)
            .build();

        let writer = AsyncArrowWriter::try_new(object_writer, schema, Some(props))?;

        Ok(Box::new(NativeParquetWriter {
            writer: Some(writer),
            path,
        }))
    }

    /// Writes an Arrow record batch. The encoding is synchronous; only the
    /// row-group flush (if triggered) goes through the async IO path.
    pub fn write_data(&mut self, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!("write_data called for path: {}", self.path);

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid FFI addresses for path: {}", self.path);
            return Err("Invalid FFI addresses (null pointers)".into());
        }

        let writer = self.writer.as_mut().ok_or("Writer already finalized")?;

        unsafe {
            let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
            let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);
            let array_data = arrow::ffi::from_ffi(arrow_array, &arrow_schema)?;
            let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);

            if let Some(struct_array) = array.as_any().downcast_ref::<arrow::array::StructArray>() {
                let schema = Arc::new(arrow::datatypes::Schema::new(struct_array.fields().clone()));
                let record_batch = RecordBatch::try_new(schema, struct_array.columns().to_vec())?;
                log_debug!(
                    "Created RecordBatch with {} rows and {} columns",
                    record_batch.num_rows(),
                    record_batch.num_columns()
                );
                RUNTIME.block_on(writer.write(&record_batch))?;
                Ok(())
            } else {
                log_error!("ERROR: Array is not a StructArray, type: {:?}", array.data_type());
                Err("Expected struct array from VectorSchemaRoot".into())
            }
        }
    }

    /// Finalizes the writer — flushes remaining data and writes the Parquet
    /// footer to the object store. Returns file-level metadata.
    pub fn finalize_writer(&mut self) -> Result<Option<FormatFileMetaData>, Box<dyn std::error::Error>> {
        log_debug!("finalize_writer called for path: {}", self.path);

        match self.writer.take() {
            Some(writer) => {
                let file_metadata = RUNTIME.block_on(writer.close())?;
                log_debug!(
                    "Successfully closed writer for path: {}, num_rows={}",
                    self.path,
                    file_metadata.num_rows
                );
                Ok(Some(file_metadata))
            }
            None => {
                log_error!("ERROR: Writer already finalized for path: {}", self.path);
                Err("Writer already finalized".into())
            }
        }
    }

    /// Returns the in-memory buffer size of the Arrow writer, or 0 if finalized.
    pub fn memory_size(&self) -> usize {
        self.writer.as_ref().map_or(0, |w| w.memory_size())
    }

    /// Returns the path this writer is writing to.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Reads metadata from an existing Parquet file on local disk (static utility).
    pub fn get_file_metadata(
        filename: String,
    ) -> Result<parquet::file::metadata::FileMetaData, Box<dyn std::error::Error>> {
        let file = File::open(&filename)?;
        let reader = SerializedFileReader::new(file)?;
        let file_metadata = reader.metadata().file_metadata().clone();
        log_debug!(
            "Metadata for {}: version={}, num_rows={}",
            filename,
            file_metadata.version(),
            file_metadata.num_rows()
        );
        Ok(file_metadata)
    }
}

/// Converts a raw pointer back into a mutable reference.
/// # Safety
/// The pointer must have been created by `Box::into_raw` on a valid `NativeParquetWriter`.
pub unsafe fn writer_from_handle(handle: i64) -> &'static mut NativeParquetWriter {
    assert!(handle != 0, "Null writer handle");
    unsafe { &mut *(handle as *mut NativeParquetWriter) }
}

/// Converts a `Box<NativeParquetWriter>` into a raw pointer suitable for passing to Java.
pub fn writer_to_handle(writer: Box<NativeParquetWriter>) -> i64 {
    Box::into_raw(writer) as i64
}

/// Reclaims a writer from its raw pointer, dropping it and freeing memory.
/// # Safety
/// The pointer must have been created by `writer_to_handle` and must not be used after this call.
pub unsafe fn drop_writer(handle: i64) {
    if handle != 0 {
        unsafe {
            let _ = Box::from_raw(handle as *mut NativeParquetWriter);
        }
    }
}
