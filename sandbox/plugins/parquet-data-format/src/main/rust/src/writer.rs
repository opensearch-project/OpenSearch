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
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::{log_info, log_error, log_debug};
use crate::native_settings::NativeSettings;
use crate::writer_properties_builder::WriterPropertiesBuilder;

/// Per-writer sort configuration stored at create time, consumed at close time.
struct SortConfig {
    sort_columns: Vec<String>,
    reverse_sorts: Vec<bool>,
}

lazy_static! {
    pub static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<File>>>> = DashMap::new();
    pub static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
    pub static ref SETTINGS_STORE: DashMap<String, NativeSettings> = DashMap::new();
    /// Maps temp_filename -> SortConfig so finalize_writer knows how to sort and where to write.
    static ref SORT_CONFIG: DashMap<String, SortConfig> = DashMap::new();
}

pub struct NativeParquetWriter;

impl NativeParquetWriter {
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
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!(
            "create_writer called for file: {}, index: {}, schema_address: {}, sort_columns: {:?}, reverse_sorts: {:?}",
            filename, index_name, schema_address, sort_columns, reverse_sorts
        );

        if (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid schema address (null pointer) for file: {}", filename);
            return Err("Invalid schema address".into());
        }

        let temp_filename = Self::temp_filename(&filename);

        if WRITER_MANAGER.contains_key(&temp_filename) {
            log_error!("ERROR: Writer already exists for file: {}", temp_filename);
            return Err("Writer already exists for this file".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);
        log_debug!("Schema created with {} fields", schema.fields().len());

        let file = File::create(&temp_filename)?;
        let file_clone = file.try_clone()?;
        FILE_MANAGER.insert(temp_filename.clone(), file_clone);

        let config: NativeSettings = SETTINGS_STORE
            .get(&index_name)
            .map(|r| r.clone())
            .unwrap_or_default();
        let props = WriterPropertiesBuilder::build(&config);

        // Store sort columns in SETTINGS_STORE so merge can look them up
        SETTINGS_STORE.entry(index_name).and_modify(|s| {
            s.sort_columns = sort_columns.clone();
            s.reverse_sorts = reverse_sorts.clone();
        }).or_insert_with(|| {
            let mut s = NativeSettings::default();
            s.sort_columns = sort_columns.clone();
            s.reverse_sorts = reverse_sorts.clone();
            s
        });

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        WRITER_MANAGER.insert(temp_filename.clone(), Arc::new(Mutex::new(writer)));

        SORT_CONFIG.insert(temp_filename, SortConfig {
            sort_columns,
            reverse_sorts,
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

                if let Some(writer_arc) = WRITER_MANAGER.get(&temp_filename) {
                    let mut writer = writer_arc.lock().unwrap();
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

    pub fn finalize_writer(filename: String) -> Result<Option<parquet::file::metadata::FileMetaData>, Box<dyn std::error::Error>> {
        let temp_filename = Self::temp_filename(&filename);
        log_info!("finalize_writer called for file: {} (temp: {})", filename, temp_filename);

        if let Some((_, writer_arc)) = WRITER_MANAGER.remove(&temp_filename) {
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let writer = mutex.into_inner().unwrap();
                    match writer.close() {
                        Ok(_) => {
                            log_info!("Successfully closed temp writer for: {}", temp_filename);

                            let sort_config = SORT_CONFIG.remove(&temp_filename).map(|(_, v)| v);
                            let (sort_columns, reverse_sorts) = match &sort_config {
                                Some(cfg) => (cfg.sort_columns.clone(), cfg.reverse_sorts.clone()),
                                None => (vec![], vec![]),
                            };

                            Self::sort_and_rewrite_parquet(&temp_filename, &filename, &sort_columns, &reverse_sorts)?;

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
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!(
            "sort_and_rewrite_parquet: temp={}, output={}, sort_columns={:?}, reverse_sorts={:?}",
            temp_filename, output_filename, sort_columns, reverse_sorts
        );

        if sort_columns.is_empty() {
            log_info!("No sort columns specified, renaming temp file to final");
            std::fs::rename(temp_filename, output_filename)?;
            return Ok(());
        }

        let file_size = std::fs::metadata(temp_filename)?.len();
        const MAX_MEMORY_SIZE: u64 = 32 * 1024 * 1024;

        if file_size <= MAX_MEMORY_SIZE {
            Self::sort_small_file(temp_filename, output_filename, sort_columns, reverse_sorts)
        } else {
            Self::sort_large_file(temp_filename, output_filename, sort_columns, reverse_sorts)
        }
    }

    fn sort_small_file(
        temp_filename: &str,
        output_filename: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
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
        let sorted_batch = Self::sort_batch(&combined_batch, sort_columns, reverse_sorts)?;
        let final_batch = Self::rewrite_row_ids(&sorted_batch, &schema)?;

        Self::write_final_file(output_filename, &final_batch, schema)?;
        Ok(())
    }

    fn sort_large_file(
        temp_filename: &str,
        output_filename: &str,
        sort_columns: &[String],
        reverse_sorts: &[bool],
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("Using streaming sort for large file: {}", temp_filename);

        let file = File::open(temp_filename)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let arrow_reader = builder.with_batch_size(8192).build()?;

        let mut temp_files = Vec::new();
        let mut batch_count = 0;
        let temp_dir = std::env::temp_dir();

        for batch_result in arrow_reader {
            let batch = batch_result?;
            let schema = batch.schema();
            let sorted_batch = Self::sort_batch(&batch, sort_columns, reverse_sorts)?;

            let chunk_filename = temp_dir.join(format!(
                "sort_chunk_{}_{}.parquet", batch_count, std::process::id()
            ));
            let chunk_file = File::create(&chunk_filename)?;
            let config = NativeSettings::default();
            let props = WriterPropertiesBuilder::build(&config);
            let mut chunk_writer = ArrowWriter::try_new(chunk_file, schema.clone(), Some(props))?;
            chunk_writer.write(&sorted_batch)?;
            chunk_writer.close()?;

            temp_files.push(chunk_filename);
            batch_count += 1;
        }

        if temp_files.is_empty() {
            log_info!("No data to sort in file: {}", temp_filename);
            std::fs::rename(temp_filename, output_filename)?;
            return Ok(());
        }

        log_info!("Created {} sorted chunks, now merging", batch_count);

        let merged_batch = Self::merge_sorted_chunks(&temp_files, sort_columns, reverse_sorts)?;

        for chunk_file in &temp_files {
            let _ = std::fs::remove_file(chunk_file);
        }

        let schema = merged_batch.schema();
        let final_batch = Self::rewrite_row_ids(&merged_batch, &schema)?;

        Self::write_final_file(output_filename, &final_batch, schema)?;
        Ok(())
    }

    fn sort_batch(
        batch: &RecordBatch,
        sort_columns: &[String],
        reverse_sorts: &[bool],
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let columns: Vec<SortColumn> = sort_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| {
                let reverse = reverse_sorts.get(i).copied().unwrap_or(false);
                let options = arrow::compute::SortOptions {
                    descending: reverse,
                    nulls_first: !reverse,
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

    fn merge_sorted_chunks(
        chunk_files: &[std::path::PathBuf],
        sort_columns: &[String],
        reverse_sorts: &[bool],
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        if chunk_files.len() == 1 {
            let file = File::open(&chunk_files[0])?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let mut reader = builder.build()?;
            return Ok(reader.next().unwrap()?);
        }

        let mut all_batches = Vec::new();
        for chunk_file in chunk_files {
            let file = File::open(chunk_file)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;
            for batch_result in reader {
                all_batches.push(batch_result?);
            }
        }

        if all_batches.is_empty() {
            return Err("No batches to merge".into());
        }

        let schema = all_batches[0].schema();
        let combined = concat_batches(&schema, &all_batches)?;
        Self::sort_batch(&combined, sort_columns, reverse_sorts)
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
                let temp_filename = Self::temp_filename(&filename);
                FILE_MANAGER.remove(&temp_filename);
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
