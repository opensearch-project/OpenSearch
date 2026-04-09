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
use arrow_ipc::writer::FileWriter as IpcFileWriter;
use arrow_ipc::reader::FileReader as IpcFileReader;
use dashmap::DashMap;
use lazy_static::lazy_static;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::{log_info, log_error, log_debug};
use crate::native_settings::NativeSettings;
use crate::writer_properties_builder::WriterPropertiesBuilder;

/// The underlying writer — either direct Parquet or Arrow IPC staging.
/// When sort columns are configured, the IPC variant is used to avoid the
/// redundant Parquet encode→decode cycle before sorting. Arrow IPC is
/// essentially a memory dump of Arrow batches — near-zero serialization cost.
enum WriterVariant {
    /// Direct Parquet writer — used when no sort columns are configured.
    Parquet(Arc<Mutex<ArrowWriter<File>>>),
    /// Arrow IPC staging writer — used when sort columns are configured.
    /// Batches are written as raw Arrow IPC; on close they are read back,
    /// sorted, and written as a single Parquet file.
    Ipc(Arc<Mutex<IpcFileWriter<File>>>),
}

/// Bundles all per-writer resources so a single `DashMap::remove` atomically
/// drops the writer, closes the file handle, and cleans up sort config.
struct WriterState {
    variant: WriterVariant,
    file_handle: File,
    sort_columns: Vec<String>,
    reverse_sorts: Vec<bool>,
    nulls_first: Vec<bool>,
}

/// Path suffix for the intermediate Arrow IPC file used during sort-on-close.
const IPC_STAGING_SUFFIX: &str = ".arrow_ipc_staging";

lazy_static! {
    /// Unified per-writer registry. Keyed by temp filename.
    /// Holds both Parquet and IPC writers via the `WriterVariant` enum.
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

        let config: NativeSettings = SETTINGS_STORE
            .get(&index_name)
            .map(|r| r.clone())
            .unwrap_or_default();

        // Store sort columns in SETTINGS_STORE so merge can look them up
        SETTINGS_STORE.entry(index_name).and_modify(|s| {
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

        // If sort columns are configured, use Arrow IPC staging path to avoid
        // the redundant Parquet encode→decode cycle before sorting.
        let (variant, file_clone) = if !sort_columns.is_empty() {
            let ipc_path = format!("{}{}", temp_filename, IPC_STAGING_SUFFIX);
            let file = File::create(&ipc_path)?;
            let file_clone = file.try_clone()?;
            let ipc_writer = IpcFileWriter::try_new(file, &schema)?;
            (WriterVariant::Ipc(Arc::new(Mutex::new(ipc_writer))), file_clone)
        } else {
            let file = File::create(&temp_filename)?;
            let file_clone = file.try_clone()?;
            let props = WriterPropertiesBuilder::build(&config);
            let writer = ArrowWriter::try_new(file, schema, Some(props))?;
            (WriterVariant::Parquet(Arc::new(Mutex::new(writer))), file_clone)
        };

        WRITERS.insert(temp_filename, WriterState {
            variant,
            file_handle: file_clone,
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

                // Write to the appropriate writer variant
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

    pub fn finalize_writer(filename: String) -> Result<Option<parquet::file::metadata::FileMetaData>, Box<dyn std::error::Error>> {
        let temp_filename = Self::temp_filename(&filename);
        log_info!("finalize_writer called for file: {} (temp: {})", filename, temp_filename);

        if let Some((_, state)) = WRITERS.remove(&temp_filename) {
            let WriterState { variant, file_handle: _file, sort_columns, reverse_sorts, nulls_first } = state;

            match variant {
                WriterVariant::Ipc(writer_arc) => {
                    match Arc::try_unwrap(writer_arc) {
                        Ok(mutex) => {
                            let mut writer = mutex.into_inner().unwrap();
                            writer.finish()?;
                            log_info!("Successfully closed IPC staging writer for: {}", temp_filename);

                            let ipc_path = format!("{}{}", temp_filename, IPC_STAGING_SUFFIX);
                            Self::sort_ipc_and_write_parquet(&ipc_path, &filename, &sort_columns, &reverse_sorts, &nulls_first)?;
                            let _ = std::fs::remove_file(&ipc_path);

                            let metadata = Self::get_file_metadata(filename)?;
                            Ok(Some(metadata))
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
                                    log_info!("Successfully closed temp writer for: {}", temp_filename);

                                    Self::sort_and_rewrite_parquet(&temp_filename, &filename, &sort_columns, &reverse_sorts, &nulls_first)?;
                                    let _ = std::fs::remove_file(&temp_filename);

                                    let metadata = Self::get_file_metadata(filename)?;
                                    Ok(Some(metadata))
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

    /// Read the temp parquet file, sort by sort_columns (if any), rewrite ___row_id,
    /// and write the final sorted file to `output_filename`.
    fn sort_and_rewrite_parquet(
        temp_filename: &str,
        output_filename: &str,
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

        Self::sort_and_write(temp_filename, output_filename, sort_columns, reverse_sorts, nulls_first)
    }

    /// Sort-on-close for the IPC staging path: reads Arrow IPC batches from the
    /// staging file, sorts them, writes the final Parquet file.
    /// Arrow IPC is essentially a memory dump of Arrow batches — near-zero
    /// serialization cost — so this avoids the redundant Parquet encode→decode cycle.
    fn sort_ipc_and_write_parquet(
        ipc_path: &str,
        output_filename: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!(
            "sort_ipc_and_write_parquet: ipc={}, output={}, sort_columns={:?}",
            ipc_path, output_filename, sort_columns
        );

        if sort_columns.is_empty() {
            // Should not happen on IPC path, but handle gracefully
            log_info!("No sort columns on IPC path, this is unexpected");
            return Err("IPC staging path requires sort columns".into());
        }

        // Read all batches from the Arrow IPC staging file
        let ipc_file = File::open(ipc_path)?;
        let ipc_reader = IpcFileReader::try_new(ipc_file, None)?;
        let schema = ipc_reader.schema();

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        for batch_result in ipc_reader {
            let batch = batch_result?;
            if batch.num_rows() > 0 {
                all_batches.push(batch);
            }
        }

        if all_batches.is_empty() {
            log_info!("No data in IPC staging file: {}", ipc_path);
            // Write an empty Parquet file
            let config = NativeSettings::default();
            let props = WriterPropertiesBuilder::build(&config);
            let file = File::create(output_filename)?;
            let writer = ArrowWriter::try_new(file, schema, Some(props))?;
            writer.close()?;
            return Ok(());
        }

        // Concatenate all batches into one
        let combined_batch = concat_batches(&schema, &all_batches)?;

        // Sort using the same multi-column sort as the Parquet path
        let sorted_batch = Self::sort_batch(&combined_batch, sort_columns, reverse_sorts, nulls_first)?;

        // Rewrite ___row_id with sequential values
        let final_batch = Self::rewrite_row_ids(&sorted_batch, &schema)?;

        // Write the final sorted Parquet file
        Self::write_final_file(output_filename, &final_batch, schema)?;

        log_info!("sort_ipc_and_write_parquet: sorted {} rows, wrote Parquet to {}", final_batch.num_rows(), output_filename);
        Ok(())
    }

    fn sort_and_write(
        temp_filename: &str,
        output_filename: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
        nulls_first: &[bool],
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("Using in-memory sort for file: {}", temp_filename);

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

        Self::write_final_file(output_filename, &final_batch, schema)?;
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

        if let Some(row_id_idx) = schema.fields().iter().position(|f| f.name() == "___row_id") {
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
        batch: &RecordBatch,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let config = NativeSettings::default();
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
                if let WriterVariant::Parquet(writer_arc) = &entry.value().variant {
                    if let Ok(writer) = writer_arc.lock() {
                        total_memory += writer.memory_size();
                    }
                }
                // IPC writers don't expose memory_size(), but we count them as present
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
