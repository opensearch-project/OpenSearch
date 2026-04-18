/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::compute::{concat_batches, lexsort_to_indices, take, SortColumn};
use dashmap::DashMap;
use lazy_static::lazy_static;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::{log_info, log_error, log_debug};
use crate::merge::schema::ROW_ID_COLUMN_NAME;
use crate::native_settings::NativeSettings;
use crate::writer_properties_builder::WriterPropertiesBuilder;

/// Result from finalizing a writer: Parquet metadata + whole-file CRC32.
#[derive(Debug)]
pub struct FinalizeResult {
    pub metadata: parquet::file::metadata::ParquetMetaData,
    pub crc32: u32,
}

/// Bundles all per-writer resources so a single `DashMap::remove` atomically
/// drops the writer, closes the file handle, and cleans up sort config.
struct WriterState {
    writer: Arc<Mutex<ArrowWriter<File>>>,
    file_handle: File,
    index_name: String,
    sort_columns: Vec<String>,
    reverse_sorts: Vec<bool>,
    nulls_first: Vec<bool>,
}

lazy_static! {
    /// Unified per-writer registry. Keyed by temp filename.
    static ref WRITERS: DashMap<String, WriterState> = DashMap::new();
    pub static ref SETTINGS_STORE: DashMap<String, NativeSettings> = DashMap::new();
}

pub struct NativeParquetWriter;

impl NativeParquetWriter {
    /// Returns true if a writer is currently open for the given filename.
    pub fn has_writer(filename: &str) -> bool {
        let temp_filename = Self::temp_filename(filename);
        WRITERS.contains_key(&temp_filename)
    }
    /// Build the temp filename by prepending "temp-" to the basename.
    fn temp_filename(filename: &str) -> String {
        let path = Path::new(filename);
        path.parent()
            .unwrap_or_else(|| Path::new(""))
            .join(format!("temp-{}", path.file_name().unwrap().to_str().unwrap()))
            .to_string_lossy()
            .to_string()
    }

    pub fn create_writer(
        filename: String,
        index_name: String,
        schema_address: i64,
        sort_columns: Vec<String>,
        reverse_sorts: Vec<bool>,
        nulls_first: Vec<bool>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!(
            "create_writer called for file: {}, index: {}, schema_address: {}, sort_columns: {:?}, reverse_sorts: {:?}, nulls_first: {:?}",
            filename, index_name, schema_address, sort_columns, reverse_sorts, nulls_first
        );

        if (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid schema address (null pointer) for file: {}", filename);
            return Err("Invalid schema address".into());
        }

        let temp_filename = Self::temp_filename(&filename);

        if WRITERS.contains_key(&temp_filename) {
            log_error!("ERROR: Writer already exists for file: {}", temp_filename);
            return Err("Writer already exists for this file".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);
        log_debug!("Schema created with {} fields", schema.fields().len());

        let file = File::create(&temp_filename)?;
        let file_clone = file.try_clone()?;

        let config: NativeSettings = SETTINGS_STORE
            .get(&index_name)
            .map(|r| r.clone())
            .unwrap_or_default();
        let props = WriterPropertiesBuilder::build(&config);

        SETTINGS_STORE.entry(index_name.clone()).and_modify(|s| {
            s.sort_columns = sort_columns.clone();
            s.reverse_sorts = reverse_sorts.clone();
            s.nulls_first = nulls_first.clone();
        }).or_insert_with(|| {
            let mut s = NativeSettings::default();
            s.sort_columns = sort_columns.clone();
            s.reverse_sorts = reverse_sorts.clone();
            s.nulls_first = nulls_first.clone();
            s
        });

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;

        WRITERS.insert(temp_filename, WriterState {
            writer: Arc::new(Mutex::new(writer)),
            file_handle: file_clone,
            index_name,
            sort_columns,
            reverse_sorts,
            nulls_first,
        });

        Ok(())
    }

    pub fn write_data(filename: String, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        let temp_filename = Self::temp_filename(&filename);
        log_debug!("write_data called for file: {} (temp: {})", filename, temp_filename);

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid FFI addresses for file: {}", temp_filename);
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

                if let Some(state) = WRITERS.get(&temp_filename) {
                    let mut writer = state.writer.lock().unwrap();
                    writer.write(&record_batch)?;
                    Ok(())
                } else {
                    log_error!("ERROR: No writer found for temp file: {}", temp_filename);
                    Err("Writer not found".into())
                }
            } else {
                log_error!("ERROR: Array is not a StructArray, type: {:?}", array.data_type());
                Err("Expected struct array from VectorSchemaRoot".into())
            }
        }
    }

    pub fn finalize_writer(filename: String) -> Result<Option<FinalizeResult>, Box<dyn std::error::Error>> {
        let temp_filename = Self::temp_filename(&filename);
        log_info!("finalize_writer called for file: {} (temp: {})", filename, temp_filename);

        if let Some((_, state)) = WRITERS.remove(&temp_filename) {
            let WriterState { writer: writer_arc, file_handle: _file, index_name, sort_columns, reverse_sorts, nulls_first } = state;
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let writer = mutex.into_inner().unwrap();
                    match writer.close() {
                        Ok(_) => {
                            log_info!("Successfully closed temp writer for: {}", temp_filename);
                            // _file is dropped here, closing the file handle

                            Self::sort_and_rewrite_parquet(&temp_filename, &filename, &index_name, &sort_columns, &reverse_sorts, &nulls_first)?;

                            let _ = std::fs::remove_file(&temp_filename);

                            // Compute CRC32 by reading the final sorted file
                            let crc32 = Self::compute_file_crc32(&filename)?;
                            log_debug!("CRC32 for file {}: {:#010x}", filename, crc32);

                            // Read full ParquetMetaData from the final file
                            let file = File::open(&filename)?;
                            let reader = SerializedFileReader::new(file)?;
                            let parquet_metadata = reader.metadata().clone();

                            Ok(Some(FinalizeResult { metadata: parquet_metadata, crc32 }))
                        }
                        Err(e) => {
                            log_error!("ERROR: Failed to close writer for temp file: {}", temp_filename);
                            Err(e.into())
                        }
                    }
                }
                Err(_) => {
                    log_error!("ERROR: Writer still in use for temp file: {}", temp_filename);
                    Err("Writer still in use".into())
                }
            }
        } else {
            log_error!("ERROR: Writer not found for temp file: {}", temp_filename);
            Err("Writer not found".into())
        }
    }

    fn compute_file_crc32(path: &str) -> Result<u32, Box<dyn std::error::Error>> {
        let mut file = File::open(path)?;
        let mut hasher = crc32fast::Hasher::new();
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 { break; }
            hasher.update(&buf[..n]);
        }
        Ok(hasher.finalize())
    }

    fn sort_and_rewrite_parquet(
        temp_filename: &str,
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!(
            "sort_and_rewrite_parquet: temp={}, output={}, sort_columns={:?}, reverse_sorts={:?}, nulls_first={:?}",
            temp_filename, output_filename, sort_columns, reverse_sorts, nulls_first
        );

        if sort_columns.is_empty() {
            log_info!("No sort columns specified, renaming temp file to final");
            std::fs::rename(temp_filename, output_filename)?;
            return Ok(());
        }

        let file_size = std::fs::metadata(temp_filename)?.len();
        const MAX_MEMORY_SIZE: u64 = 32 * 1024 * 1024;

        if file_size <= MAX_MEMORY_SIZE {
            Self::sort_small_file(temp_filename, output_filename, index_name, sort_columns, reverse_sorts, nulls_first)
        } else {
            Self::sort_large_file(temp_filename, output_filename, index_name, sort_columns, reverse_sorts, nulls_first)
        }
    }

    /// In-memory sort for small files: read all batches, concat, sort, rewrite row IDs, write.
    fn sort_small_file(
        temp_filename: &str,
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("Using in-memory sort for small file: {}", temp_filename);

        let file = File::open(temp_filename)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let arrow_reader = builder.with_batch_size(2048).build()?;

        let mut batches = Vec::new();
        for batch_result in arrow_reader {
            batches.push(batch_result?);
        }

        if batches.is_empty() {
            log_info!("No data to sort in file: {}", temp_filename);
            std::fs::rename(temp_filename, output_filename)?;
            return Ok(());
        }

        let schema = batches[0].schema();
        let combined_batch = concat_batches(&schema, &batches)?;
        let sorted_batch = Self::sort_batch(&combined_batch, sort_columns, reverse_sorts, nulls_first)?;
        let final_batch = Self::rewrite_row_ids(&sorted_batch, &schema)?;

        Self::write_final_file(output_filename, index_name, &final_batch, schema)?;
        Ok(())
    }

    /// For large files: read in batches, sort each batch individually, write each
    /// as a separate sorted chunk file, then use the streaming k-way merge to
    /// produce the final globally-sorted output.
    fn sort_large_file(
        temp_filename: &str,
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("Using streaming merge sort for large file: {}", temp_filename);

        let file = File::open(temp_filename)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let arrow_reader = builder.with_batch_size(8192).build()?;

        let mut chunk_paths: Vec<String> = Vec::new();
        let mut batch_count = 0;
        let temp_dir = std::env::temp_dir();

        for batch_result in arrow_reader {
            let batch = batch_result?;
            let schema = batch.schema();
            let sorted_batch = Self::sort_batch(&batch, sort_columns, reverse_sorts, nulls_first)?;

            let chunk_filename = temp_dir
                .join(format!("sort_chunk_{}_{}.parquet", batch_count, std::process::id()))
                .to_string_lossy()
                .to_string();
            Self::write_final_file(&chunk_filename, index_name, &sorted_batch, schema)?;

            chunk_paths.push(chunk_filename);
            batch_count += 1;
        }

        if chunk_paths.is_empty() {
            log_info!("No data to sort in file: {}", temp_filename);
            std::fs::rename(temp_filename, output_filename)?;
            return Ok(());
        }

        log_info!("Created {} sorted chunks, merging via streaming k-way merge", batch_count);

        // Use the streaming merge to produce the final sorted file
        crate::merge::merge_sorted(
            &chunk_paths,
            output_filename,
            index_name,
            sort_columns,
            reverse_sorts,
            nulls_first,
        ).map_err(|e| -> Box<dyn std::error::Error> { format!("Streaming merge failed: {}", e).into() })?;

        // Clean up temp chunk files
        for path in &chunk_paths {
            let _ = std::fs::remove_file(path);
        }

        Ok(())
    }

    fn sort_batch(
        batch: &RecordBatch,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let columns: Vec<SortColumn> = sort_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| {
                let reverse = reverse_sorts.get(i).copied().unwrap_or(false);
                let nf = nulls_first.get(i).copied().unwrap_or(false);
                let options = arrow::compute::SortOptions {
                    descending: reverse,
                    nulls_first: nf,
                };
                let col_index = batch.schema().index_of(col_name)
                    .map_err(|_| format!("Sort column '{}' not found in schema", col_name))?;
                Ok(SortColumn {
                    values: batch.column(col_index).clone(),
                    options: Some(options),
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        let indices = lexsort_to_indices(&columns, None)?;
        let sorted_columns: Result<Vec<_>, _> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect();

        Ok(RecordBatch::try_new(batch.schema(), sorted_columns?)?)
    }

    /// If a ___row_id column exists, rewrite it with sequential values 0..N.
    fn rewrite_row_ids(
        batch: &RecordBatch,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        use arrow::array::Int64Array;

        if let Some(row_id_idx) = schema.fields().iter().position(|f| f.name() == ROW_ID_COLUMN_NAME) {
            log_info!("Rewriting ___row_id column with sequential values 0..{}", batch.num_rows());
            let sequential_ids = Int64Array::from_iter_values(
                (0..batch.num_rows() as u64).map(|x| x as i64)
            );
            let mut new_columns = batch.columns().to_vec();
            new_columns[row_id_idx] = Arc::new(sequential_ids);
            Ok(RecordBatch::try_new(schema.clone(), new_columns)?)
        } else {
            Ok(batch.clone())
        }
    }

    fn write_final_file(
        output_filename: &str,
        index_name: &str,
        batch: &RecordBatch,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let config = SETTINGS_STORE
            .get(index_name)
            .map(|r| r.clone())
            .unwrap_or_default();
        let props = WriterPropertiesBuilder::build(&config);
        let file = File::create(output_filename)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        log_info!("Successfully wrote final file: {}", output_filename);
        Ok(())
    }

    pub fn sync_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!("sync_to_disk called for file: {}", filename);

        let file = match File::open(&filename) {
            Ok(f) => f,
            Err(e) => {
                log_error!("ERROR: Failed to open file for fsync: {}", filename);
                return Err(e.into());
            }
        };

        match file.sync_all() {
            Ok(_) => {
                log_debug!("Successfully fsynced file: {}", filename);
                Ok(())
            }
            Err(e) => {
                log_error!("ERROR: Failed to fsync file: {}", filename);
                Err(e.into())
            }
        }
    }

    pub fn get_filtered_writer_memory_usage(path_prefix: String) -> Result<usize, Box<dyn std::error::Error>> {
        let mut total_memory = 0;
        for entry in WRITERS.iter() {
            if entry.key().starts_with(&path_prefix) {
                if let Ok(writer) = entry.value().writer.lock() {
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
