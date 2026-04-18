/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::path::Path as ObjectPath;
use parquet::arrow::async_writer::{AsyncFileWriter, ParquetObjectWriter};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::errors::Result as ParquetResult;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::sync::{Arc, Mutex};

use crate::log_debug;
use crate::object_store_handle;
use crate::runtime::RUNTIME;

/// Shared CRC32 state that survives AsyncArrowWriter::close().
#[derive(Clone)]
struct SharedCrc32(Arc<Mutex<crc32fast::Hasher>>);

impl SharedCrc32 {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(crc32fast::Hasher::new())))
    }
    fn update(&self, data: &[u8]) {
        self.0.lock().unwrap().update(data);
    }
    fn finalize(&self) -> u32 {
        self.0.lock().unwrap().clone().finalize()
    }
}

/// Async file writer wrapper that computes CRC32 as bytes flow through.
pub struct Crc32AsyncFileWriter {
    inner: ParquetObjectWriter,
    crc: SharedCrc32,
}

impl Crc32AsyncFileWriter {
    fn new(inner: ParquetObjectWriter, crc: SharedCrc32) -> Self {
        Self { inner, crc }
    }
}

impl AsyncFileWriter for Crc32AsyncFileWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, ParquetResult<()>> {
        self.crc.update(&bs);
        self.inner.write(bs)
    }

    fn complete(&mut self) -> BoxFuture<'_, ParquetResult<()>> {
        self.inner.complete()
    }
}

/// Result from finalizing a writer.
#[derive(Debug)]
pub struct FinalizeResult {
    pub num_rows: i64,
    pub crc32: u32,
}

/// Opaque writer state held behind a handle pointer.
struct WriterState {
    writer: AsyncArrowWriter<Crc32AsyncFileWriter>,
    crc: SharedCrc32,
}

/// Create a new ObjectStore-backed writer.
/// Returns a handle (leaked Box pointer) to the WriterState.
pub fn create_writer(
    store_handle: i64,
    object_path: &str,
    schema_address: i64,
) -> Result<i64, Box<dyn std::error::Error>> {
    log_debug!(
        "create_writer: store_handle={}, path={}",
        store_handle,
        object_path
    );

    if (schema_address as *mut u8).is_null() {
        return Err("Invalid schema address".into());
    }
    if store_handle <= 0 {
        return Err("Invalid store handle".into());
    }

    let store = unsafe { object_store_handle::arc_clone_from_raw(store_handle) };
    let path = ObjectPath::from(object_path);

    let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
    let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);
    log_debug!("Schema created with {} fields", schema.fields().len());

    let props = WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .set_bloom_filter_enabled(true)
        .set_bloom_filter_fpp(0.1)
        .set_bloom_filter_ndv(100000)
        .build();

    let obj_writer = ParquetObjectWriter::new(store, path);
    let crc = SharedCrc32::new();
    let crc_writer = Crc32AsyncFileWriter::new(obj_writer, crc.clone());
    let async_writer = AsyncArrowWriter::try_new(crc_writer, schema, Some(props))?;

    let state = Box::new(WriterState {
        writer: async_writer,
        crc,
    });
    let handle = Box::into_raw(state) as i64;
    log_debug!("Writer created, handle={}", handle);
    Ok(handle)
}

/// Write an Arrow record batch to the writer.
pub fn write_data(
    writer_handle: i64,
    array_address: i64,
    schema_address: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    if writer_handle <= 0 {
        return Err("Invalid writer handle".into());
    }
    if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
        return Err("Invalid FFI addresses (null pointers)".into());
    }

    let state = unsafe { &mut *(writer_handle as *mut WriterState) };

    let batch = unsafe {
        let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
        let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);
        let array_data = arrow::ffi::from_ffi(arrow_array, &arrow_schema)?;
        let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);

        let struct_array = array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .ok_or("Expected struct array from VectorSchemaRoot")?;
        let schema = Arc::new(arrow::datatypes::Schema::new(struct_array.fields().clone()));
        RecordBatch::try_new(schema, struct_array.columns().to_vec())?
    };

    log_debug!(
        "Writing batch: {} rows, {} cols",
        batch.num_rows(),
        batch.num_columns()
    );
    RUNTIME.block_on(state.writer.write(&batch))?;
    Ok(())
}

/// Finalize the writer: flush footer, upload to ObjectStore, return metadata.
/// Consumes the writer handle (invalid after this call).
pub fn finalize_writer(writer_handle: i64) -> Result<FinalizeResult, Box<dyn std::error::Error>> {
    if writer_handle <= 0 {
        return Err("Invalid writer handle".into());
    }

    let state = unsafe { Box::from_raw(writer_handle as *mut WriterState) };
    let crc = state.crc.clone();
    let metadata = RUNTIME.block_on(state.writer.close())?;

    let num_rows = metadata.file_metadata().num_rows();
    let crc32 = crc.finalize();
    log_debug!(
        "Writer finalized: num_rows={}, crc32={:#010x}",
        num_rows,
        crc32
    );
    Ok(FinalizeResult { num_rows, crc32 })
}

/// Destroy a writer without finalizing (error cleanup path).
pub fn destroy_writer(writer_handle: i64) {
    if writer_handle > 0 {
        unsafe {
            let _ = Box::from_raw(writer_handle as *mut WriterState);
        }
    }
}

/// Read Parquet file metadata from a local file path.
pub fn get_file_metadata(
    filename: String,
) -> Result<parquet::file::metadata::FileMetaData, Box<dyn std::error::Error>> {
    let file = std::fs::File::open(&filename)?;
    let reader = SerializedFileReader::new(file)?;
    Ok(reader.metadata().file_metadata().clone())
}
