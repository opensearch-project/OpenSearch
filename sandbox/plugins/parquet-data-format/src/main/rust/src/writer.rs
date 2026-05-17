/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::compute::{concat_batches, take};
use arrow::row::{RowConverter, SortField};
use arrow_ipc::writer::FileWriter as IpcFileWriter;
use arrow_ipc::reader::FileReader as IpcFileReader;
use dashmap::DashMap;
use lazy_static::lazy_static;
use parquet::arrow::ArrowWriter;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::{log_error, log_debug, log_info};
use crate::crc_writer::CrcWriter;
use crate::merge::{merge_sorted, schema::ROW_ID_COLUMN_NAME};
use crate::native_settings::NativeSettings;
use crate::writer_properties_builder::WriterPropertiesBuilder;

/// Result from finalizing a writer: Parquet metadata + whole-file CRC32.
#[derive(Debug)]
pub struct FinalizeResult {
    pub metadata: parquet::file::metadata::ParquetMetaData,
    pub crc32: u32,
}

/// The underlying writer — either direct Parquet or Arrow IPC staging.
/// When sort columns are configured, the IPC variant is used so that
/// batches can be cheaply read back for sorting — Arrow IPC is a raw
/// dump of in-memory Arrow buffers with minimal framing overhead.
enum WriterVariant {
    /// Direct Parquet writer — used when no sort columns are configured.
    Parquet(Arc<Mutex<ArrowWriter<CrcWriter<File>>>>),
    /// Arrow IPC staging writer — used when sort columns are configured.
    /// Batches are written as raw Arrow IPC; on close they are read back,
    /// sorted, and written as a final Parquet file.
    Ipc(Arc<Mutex<IpcFileWriter<File>>>),
}

/// Bundles all per-writer resources so a single `DashMap::remove` atomically
/// drops the writer, closes the file handle, and cleans up sort config.
struct WriterState {
    variant: WriterVariant,
    settings: NativeSettings,
    crc_handle: Option<crate::crc_writer::CrcHandle>,
    writer_generation: i64,
}

/// Path suffix for the intermediate Arrow IPC file used during sort-on-close.
const IPC_STAGING_SUFFIX: &str = ".arrow_ipc_staging";

lazy_static! {
    /// Unified per-writer registry. Keyed by temp filename.
    /// Holds both Parquet and IPC writers via the `WriterVariant` enum.
    static ref WRITERS: DashMap<String, WriterState> = DashMap::new();
    pub static ref SETTINGS_STORE: DashMap<String, NativeSettings> = DashMap::new();
    /// Holds file handles for finalized files pending fsync. Removed after sync.
    static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
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
        writer_generation: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!(
            "create_writer called for file: {}, index: {}, schema_address: {}, sort_columns: {:?}, reverse_sorts: {:?}, nulls_first: {:?}, writer_generation: {}",
            filename, index_name, schema_address, sort_columns, reverse_sorts, nulls_first, writer_generation
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

        let mut settings: NativeSettings = SETTINGS_STORE
            .get(&index_name)
            .map(|r| r.clone())
            .unwrap_or_default();
        settings.index_name = Some(index_name.clone());
        settings.sort_columns = sort_columns;
        settings.reverse_sorts = reverse_sorts;
        settings.nulls_first = nulls_first;

        SETTINGS_STORE.insert(index_name, settings.clone());

        // If sort columns are configured, use Arrow IPC staging path so
        // batches can be cheaply read back for sorting before writing Parquet.
        let (variant, crc_handle) = if !settings.sort_columns.is_empty() {
            let ipc_path = format!("{}{}", temp_filename, IPC_STAGING_SUFFIX);
            let file = File::create(&ipc_path)?;
            let ipc_writer = IpcFileWriter::try_new(file, &schema)?;
            (WriterVariant::Ipc(Arc::new(Mutex::new(ipc_writer))), None)
        } else {
            let file = File::create(&temp_filename)?;
            let (crc_file, crc_handle) = CrcWriter::new(file);
            let props = WriterPropertiesBuilder::build_with_generation(&settings, Some(writer_generation), &schema)
                .map_err(|e| format!("Invalid encoding/compression config: {}", e))?;
            let writer = ArrowWriter::try_new(crc_file, schema, Some(props))?;
            (WriterVariant::Parquet(Arc::new(Mutex::new(writer))), Some(crc_handle))
        };

        WRITERS.insert(temp_filename, WriterState {
            variant,
            settings,
            crc_handle,
            writer_generation,
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
                    match &state.variant {
                        WriterVariant::Ipc(writer_arc) => {
                            log_debug!("Writing RecordBatch to IPC staging file");
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                        }
                        WriterVariant::Parquet(writer_arc) => {
                            log_debug!("Writing RecordBatch to Parquet file");
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                        }
                    }
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
        log_debug!("finalize_writer called for file: {} (temp: {})", filename, temp_filename);

        if let Some((_, state)) = WRITERS.remove(&temp_filename) {
            let WriterState { variant, settings, crc_handle, writer_generation } = state;
            let index_name = settings.index_name.as_deref().unwrap_or("");

            match variant {
                WriterVariant::Ipc(writer_arc) => {
                    match Arc::try_unwrap(writer_arc) {
                        Ok(mutex) => {
                            let mut writer = mutex.into_inner().unwrap();
                            writer.finish()?;
                            log_info!("Successfully closed IPC staging writer for: {}", temp_filename);

                            let ipc_path = format!("{}{}", temp_filename, IPC_STAGING_SUFFIX);
                            let crc32 = Self::sort_and_rewrite_parquet(&ipc_path, &filename, index_name, &settings.sort_columns, &settings.reverse_sorts, &settings.nulls_first, writer_generation)?;
                            let _ = std::fs::remove_file(&ipc_path);

                            log_debug!("CRC32 for file {}: {:#010x}", filename, crc32);

                            let file_for_sync = File::open(&filename)?;
                            FILE_MANAGER.insert(filename.clone(), file_for_sync);

                            let file = File::open(&filename)?;
                            let reader = SerializedFileReader::new(file)?;
                            let parquet_metadata = reader.metadata().clone();

                            Ok(Some(FinalizeResult { metadata: parquet_metadata, crc32 }))
                        }
                        Err(_) => {
                            log_error!("ERROR: IPC Writer still in use for temp file: {}", temp_filename);
                            Err("IPC Writer still in use".into())
                        }
                    }
                }
                WriterVariant::Parquet(writer_arc) => {
                    match Arc::try_unwrap(writer_arc) {
                        Ok(mutex) => {
                            let writer = mutex.into_inner().unwrap();
                            match writer.close() {
                                Ok(_) => {
                                    let crc32 = crc_handle.map(|h| h.crc32()).unwrap_or(0);
                                    log_info!("Successfully closed temp writer for: {}", temp_filename);

                                    // Parquet variant is used for non-sorted data; just rename.
                                    std::fs::rename(&temp_filename, &filename)?;

                                    log_debug!("CRC32 for file {}: {:#010x}", filename, crc32);

                                    let file_for_sync = File::open(&filename)?;
                                    FILE_MANAGER.insert(filename.clone(), file_for_sync);

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
                }
            }
        } else {
            log_error!("ERROR: Writer not found for temp file: {}", temp_filename);
            Err("Writer not found".into())
        }
    }

    fn sort_and_rewrite_parquet(
        temp_filename: &str,
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
        writer_generation: i64,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        log_debug!(
            "sort_and_rewrite_parquet: temp={}, output={}, sort_columns={:?}, reverse_sorts={:?}, nulls_first={:?}",
            temp_filename, output_filename, sort_columns, reverse_sorts, nulls_first
        );

        let config = SETTINGS_STORE
            .get(index_name)
            .map(|r| r.clone())
            .unwrap_or_default();

        let file_size = std::fs::metadata(temp_filename)?.len();

        if file_size <= config.get_sort_in_memory_threshold_bytes() {
            Self::sort_small_file(temp_filename, output_filename, index_name, sort_columns, reverse_sorts, nulls_first, writer_generation)
        } else {
            Self::sort_large_file(temp_filename, output_filename, index_name, sort_columns, reverse_sorts, nulls_first, config.get_sort_batch_size())
        }
    }

    fn sort_small_file(
        temp_filename: &str,
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
        writer_generation: i64,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        log_debug!("Using in-memory sort for small file: {}", temp_filename);

        let file = File::open(temp_filename)?;
        let reader = IpcFileReader::try_new(file, None)?;
        let schema = reader.schema();

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for batch_result in reader {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }

        if all_batches.is_empty() {
            log_info!("No data in temp file: {}", temp_filename);
            let props = WriterPropertiesBuilder::build_with_generation(
                &SETTINGS_STORE.get(index_name).map(|r| r.clone()).unwrap_or_default(),
                Some(writer_generation),
                &schema,
            ).map_err(|e| format!("Invalid encoding/compression config: {}", e))?;
            let file = File::create(output_filename)?;
            let writer = ArrowWriter::try_new(file, schema, Some(props))?;
            writer.close()?;
            return Ok(0);
        }

        let combined_batch = concat_batches(&schema, &all_batches)?;
        let sorted_batch = Self::sort_batch(&combined_batch, sort_columns, reverse_sorts, nulls_first)?;
        let final_batch = Self::rewrite_row_ids(&sorted_batch, &schema)?;

        let crc32 = Self::write_final_file(output_filename, index_name, &final_batch, schema, Some(writer_generation))?;

        log_info!(
            "sort_small_file: sorted {} rows, wrote Parquet to {}",
            final_batch.num_rows(),
            output_filename
        );
        Ok(crc32)
    }

    fn sort_large_file(
        temp_filename: &str,
        output_filename: &str,
        index_name: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
        batch_size: usize,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        log_debug!("Using streaming merge sort for large file: {}", temp_filename);

        let file = File::open(temp_filename)?;
        let reader = IpcFileReader::try_new(file, None)?;
        let schema = reader.schema();

        let mut chunk_paths: Vec<String> = Vec::new();
        let mut batch_count = 0;
        let chunk_dir = Path::new(output_filename).parent().unwrap_or_else(|| Path::new("."));

        for batch_result in reader {
            let batch = batch_result?;
            if batch.num_rows() == 0 {
                continue;
            }

            // IpcFileReader returns batches at whatever size they were written.
            // Slice into batch_size chunks to bound memory during sort.
            let mut offset = 0;
            while offset < batch.num_rows() {
                let len = std::cmp::min(batch_size, batch.num_rows() - offset);
                let slice = batch.slice(offset, len);
                offset += len;

                let sorted_batch = Self::sort_batch(&slice, sort_columns, reverse_sorts, nulls_first)?;

                let chunk_filename = chunk_dir
                    .join(format!("temp_sort_chunk_{}_{}.parquet", batch_count, std::process::id()))
                    .to_string_lossy()
                    .to_string();
                // CRC for temp chunks is not needed, discard it
                Self::write_final_file(&chunk_filename, index_name, &sorted_batch, schema.clone(), None)?;

                chunk_paths.push(chunk_filename);
                batch_count += 1;
            }
        }

        if chunk_paths.is_empty() {
            log_debug!("No data to sort in file: {}", temp_filename);
            return Ok(0);
        }

        log_debug!(
            "Created {} sorted Parquet chunks, merging via streaming k-way merge",
            batch_count
        );

        let _merge_output = merge_sorted(
            &chunk_paths,
            output_filename,
            index_name,
            sort_columns,
            reverse_sorts,
            nulls_first,
        )
        .map_err(|e| -> Box<dyn std::error::Error> {
            format!("Streaming merge failed: {}", e).into()
        })?;

        // Clean up temp chunk files
        for path in &chunk_paths {
            let _ = std::fs::remove_file(path);
        }

        log_info!(
            "sort_large_file: merged {} chunks, wrote Parquet to {}",
            batch_count,
            output_filename
        );
        Ok(0)
    }

    /// Sort a batch using RowConverter: converts sort columns into compact
    /// byte-comparable rows, sorts indices by comparing those rows, then
    /// reorders all columns via take.
    fn sort_batch(
        batch: &RecordBatch,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let sort_fields: Vec<SortField> = sort_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| {
                let col_index = batch.schema().index_of(col_name)
                    .map_err(|_| format!("Sort column '{}' not found in schema", col_name))?;
                let data_type = batch.schema().field(col_index).data_type().clone();
                let options = arrow::compute::SortOptions {
                    descending: reverse_sorts.get(i).copied().unwrap_or(false),
                    nulls_first: nulls_first.get(i).copied().unwrap_or(false),
                };
                Ok(SortField::new_with_options(data_type, options))
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;

        let converter = RowConverter::new(sort_fields)?;

        let sort_arrays: Vec<Arc<dyn arrow::array::Array>> = sort_columns
            .iter()
            .map(|col_name| {
                let col_index = batch.schema().index_of(col_name).unwrap();
                batch.column(col_index).clone()
            })
            .collect();

        let rows = converter.convert_columns(&sort_arrays)?;
        let mut sort_indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
        sort_indices.sort_unstable_by(|&a, &b| rows.row(a as usize).cmp(&rows.row(b as usize)));

        let indices = arrow::array::UInt32Array::from(sort_indices);
        let sorted_columns: Result<Vec<_>, _> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect();

        Ok(RecordBatch::try_new(batch.schema(), sorted_columns?)?)
    }

    /// If a __row_id__ column exists, rewrite it with sequential values 0..N.
    fn rewrite_row_ids(
        batch: &RecordBatch,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        use arrow::array::Int64Array;

        if let Some(row_id_idx) = schema.fields().iter().position(|f| f.name() == ROW_ID_COLUMN_NAME) {
            log_debug!("Rewriting __row_id__ column with sequential values 0..{}", batch.num_rows());
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
        writer_generation: Option<i64>,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let config = SETTINGS_STORE
            .get(index_name)
            .map(|r| r.clone())
            .unwrap_or_default();
        let props = WriterPropertiesBuilder::build_with_generation(&config, writer_generation, &schema)
            .map_err(|e| format!("Invalid encoding/compression config: {}", e))?;
        let file = File::create(output_filename)?;
        let (crc_file, crc_handle) = CrcWriter::new(file);
        let mut writer = ArrowWriter::try_new(crc_file, schema, Some(props))?;
        writer.write(batch)?;
        writer.close()?;
        let crc32 = crc_handle.crc32();
        log_debug!("Successfully wrote final file: {} (crc32={:#010x})", output_filename, crc32);
        Ok(crc32)
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
        for entry in WRITERS.iter() {
            if entry.key().starts_with(&path_prefix) {
                if let WriterVariant::Parquet(writer_arc) = &entry.value().variant {
                    if let Ok(writer) = writer_arc.lock() {
                        total_memory += writer.memory_size();
                    }
                }
                // IPC writers don't expose memory_size()
            }
        }
        Ok(total_memory)
    }

    pub fn get_file_metadata(filename: String) -> Result<parquet::file::metadata::ParquetMetaData, Box<dyn std::error::Error>> {
        let file = File::open(&filename)?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata().clone();
        log_debug!("Metadata for {}: version={}, num_rows={}, num_row_groups={}",
            filename, metadata.file_metadata().version(), metadata.file_metadata().num_rows(), metadata.num_row_groups());
        Ok(metadata)
    }
}
