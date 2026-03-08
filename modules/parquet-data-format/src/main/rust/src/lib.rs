use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use arrow::record_batch::RecordBatch;
use arrow::compute::{concat_batches, sort_to_indices, take};
use dashmap::DashMap;
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jint, jlong, jobject, jboolean};
use jni::JNIEnv;
use lazy_static::lazy_static;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use parquet::file::metadata::FileMetaData as FileFileMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};

pub mod logger;
pub mod rate_limited_writer;
pub mod native_settings;
pub mod field_config;
pub mod writer_properties_builder;
pub mod parquet_merge_stream;
pub use parquet_merge_stream::*;
pub use native_settings::NativeSettings;
pub use field_config::FieldConfig;
pub use writer_properties_builder::WriterPropertiesBuilder;
// Re-export macros from the shared crate for logging
pub use vectorized_exec_spi::{log_info, log_error, log_debug};

/// Per-writer sort configuration stored at create time, consumed at close time.
struct SortConfig {
    sort_column: Option<String>,
    reverse_sort: bool,
    original_filename: String,
}

lazy_static! {
    static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<File>>>> = DashMap::new();
    static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
    static ref SETTINGS_STORE: DashMap<String, NativeSettings> = DashMap::new();
    /// Maps temp_filename -> SortConfig so close_writer knows how to sort and where to write.
    static ref SORT_CONFIG: DashMap<String, SortConfig> = DashMap::new();
}

struct NativeParquetWriter;

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

    fn create_writer(
        filename: String,
        schema_address: i64,
        sort_column: Option<String>,
        reverse_sort: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!(
            "[RUST] create_writer called for file: {}, schema_address: {}, sort_column: {:?}, reverse_sort: {}",
            filename, schema_address, sort_column, reverse_sort
        );

        if (schema_address as *mut u8).is_null() {
            log_error!("[RUST] ERROR: Invalid schema address (null pointer) for file: {}, schema_address: {}", filename, schema_address);
            return Err("Invalid schema address".into());
        }

        let temp_filename = Self::temp_filename(&filename);

        if WRITER_MANAGER.contains_key(&temp_filename) {
            log_error!("[RUST] ERROR: Writer already exists for file: {}", temp_filename);
            return Err("Writer already exists for this file".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);

        log_info!("[RUST] Schema created with {} fields", schema.fields().len());

        for (i, field) in schema.fields().iter().enumerate() {
            log_debug!("[RUST] Field {}: {} ({})", i, field.name(), field.data_type());
        }

        // Write to temp file; on close we sort and rename to the original filename
        let file = File::create(&temp_filename)?;
        let file_clone = file.try_clone()?;
        FILE_MANAGER.insert(temp_filename.clone(), file_clone);

        // Fall back to defaults for writer properties
        let config = NativeSettings::default();
        let props = WriterPropertiesBuilder::build(&config);

        let writer = ArrowWriter::try_new(file, schema, Some(props))?;
        WRITER_MANAGER.insert(temp_filename.clone(), Arc::new(Mutex::new(writer)));

        // Store sort config so close_writer can use it
        SORT_CONFIG.insert(temp_filename, SortConfig {
            sort_column,
            reverse_sort,
            original_filename: filename,
        });

        Ok(())
    }

    fn write_data(filename: String, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        let temp_filename = Self::temp_filename(&filename);
        log_info!(
            "[RUST] write_data called for file: {} (temp: {}), array_address: {}, schema_address: {}",
            filename, temp_filename, array_address, schema_address
        );

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!("[RUST] ERROR: Invalid FFI addresses for file: {}, array_address: {}, schema_address: {}", temp_filename, array_address, schema_address);
            return Err("Invalid FFI addresses (null pointers)".into());
        }

        unsafe {
            let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
            let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);

            match arrow::ffi::from_ffi(arrow_array, &arrow_schema) {
                Ok(array_data) => {
                    log_debug!("[RUST] Successfully imported array_data, length: {}", array_data.len());

                    let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);
                    log_debug!("[RUST] Array type: {:?}, length: {}", array.data_type(), array.len());

                    if let Some(struct_array) = array.as_any().downcast_ref::<arrow::array::StructArray>() {
                        log_debug!("[RUST] Successfully cast to StructArray with {} columns", struct_array.num_columns());

                        let schema = Arc::new(arrow::datatypes::Schema::new(
                            struct_array.fields().clone()
                        ));

                        let record_batch = RecordBatch::try_new(
                            schema.clone(),
                            struct_array.columns().to_vec(),
                        )?;

                        log_info!("[RUST] Created RecordBatch with {} rows and {} columns", record_batch.num_rows(), record_batch.num_columns());

                        if let Some(writer_arc) = WRITER_MANAGER.get(&temp_filename) {
                            log_debug!("[RUST] Writing RecordBatch to temp file");
                            let mut writer = writer_arc.lock().unwrap();
                            writer.write(&record_batch)?;
                            log_info!("[RUST] Successfully wrote RecordBatch");
                            Ok(())
                        } else {
                            log_error!("[RUST] ERROR: No writer found for temp file: {}", temp_filename);
                            Err("Writer not found".into())
                        }
                    } else {
                        log_error!("[RUST] ERROR: Array is not a StructArray, type: {:?}", array.data_type());
                        Err("Expected struct array from VectorSchemaRoot".into())
                    }
                }
                Err(e) => {
                    log_error!("[RUST] ERROR: Failed to import from FFI: {:?}", e);
                    Err(e.into())
                }
            }
        }
    }

    fn close_writer(filename: String) -> Result<Option<FileFileMetaData>, Box<dyn std::error::Error>> {
        let temp_filename = Self::temp_filename(&filename);
        log_info!("[RUST] close_writer called for file: {} (temp: {})", filename, temp_filename);

        if let Some((_, writer_arc)) = WRITER_MANAGER.remove(&temp_filename) {
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let writer = mutex.into_inner().unwrap();
                    match writer.close() {
                        Ok(_) => {
                            log_info!("[RUST] Successfully closed temp writer for: {}", temp_filename);

                            // Retrieve and consume sort config
                            let sort_config = SORT_CONFIG.remove(&temp_filename).map(|(_, v)| v);

                            let (sort_column, reverse_sort) = match &sort_config {
                                Some(cfg) => (cfg.sort_column.clone(), cfg.reverse_sort),
                                None => (None, false),
                            };

                            // Sort the temp file and write to the original filename
                            Self::sort_and_rewrite_parquet(&temp_filename, &filename, sort_column.as_deref(), reverse_sort)?;

                            // Clean up temp file
                            let _ = std::fs::remove_file(&temp_filename);

                            // Read metadata from the final sorted file
                            let metadata = Self::get_file_metadata(filename)?;
                            Ok(Some(metadata))
                        }
                        Err(e) => {
                            log_error!("[RUST] ERROR: Failed to close writer for temp file: {}", temp_filename);
                            Err(e.into())
                        }
                    }
                }
                Err(_) => {
                    log_error!("[RUST] ERROR: Writer still in use for temp file: {}", temp_filename);
                    Err("Writer still in use".into())
                }
            }
        } else {
            log_error!("[RUST] ERROR: Writer not found for temp file: {}\n", temp_filename);
            Err("Writer not found".into())
        }
    }

    /// Read the temp parquet file, sort by sort_column (if any), rewrite ___row_id,
    /// and write the final sorted file to `output_filename`.
    fn sort_and_rewrite_parquet(
        temp_filename: &str,
        output_filename: &str,
        sort_column: Option<&str>,
        reverse_sort: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!(
            "[RUST] sort_and_rewrite_parquet: temp={}, output={}, sort_column={:?}, reverse={}",
            temp_filename, output_filename, sort_column, reverse_sort
        );

        // If no sort column, just rename the temp file to the final name
        if sort_column.is_none() {
            log_info!("[RUST] No sort column specified, renaming temp file to final");
            std::fs::rename(temp_filename, output_filename)?;
            return Ok(());
        }

        let sort_col = sort_column.unwrap();

        // Check file size to decide sorting strategy
        let file_size = std::fs::metadata(temp_filename)?.len();
        const MAX_MEMORY_SIZE: u64 = 32 * 1024 * 1024; // 32MB threshold

        if file_size <= MAX_MEMORY_SIZE {
            Self::sort_small_file(temp_filename, output_filename, sort_col, reverse_sort)
        } else {
            Self::sort_large_file(temp_filename, output_filename, sort_col, reverse_sort)
        }
    }

    /// In-memory sort for files under the memory threshold.
    fn sort_small_file(
        temp_filename: &str,
        output_filename: &str,
        sort_column: &str,
        reverse_sort: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] Using in-memory sort for small file: {}", temp_filename);

        let file = File::open(temp_filename)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let arrow_reader = builder.with_batch_size(2048).build()?;

        let mut batches = Vec::new();
        for batch_result in arrow_reader {
            batches.push(batch_result?);
        }

        if batches.is_empty() {
            log_info!("[RUST] No data to sort in file: {}", temp_filename);
            std::fs::rename(temp_filename, output_filename)?;
            return Ok(());
        }

        let schema = batches[0].schema();
        let combined_batch = concat_batches(&schema, &batches)?;

        let sorted_batch = Self::sort_batch(&combined_batch, sort_column, reverse_sort)?;
        let final_batch = Self::rewrite_row_ids(&sorted_batch, &schema)?;

        Self::write_final_file(output_filename, &final_batch, schema)?;
        log_info!("[RUST] Sorted file written: {}", output_filename);

        Ok(())
    }

    /// Streaming sort for files above the memory threshold.
    fn sort_large_file(
        temp_filename: &str,
        output_filename: &str,
        sort_column: &str,
        reverse_sort: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] Using streaming sort for large file: {}", temp_filename);

        let file = File::open(temp_filename)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let arrow_reader = builder.with_batch_size(8192).build()?;

        let mut temp_files = Vec::new();
        let mut batch_count = 0;
        let temp_dir = std::env::temp_dir();

        // Sort each batch individually and write to temp files
        for batch_result in arrow_reader {
            let batch = batch_result?;
            let schema = batch.schema();

            let sorted_batch = Self::sort_batch(&batch, sort_column, reverse_sort)?;

            // Write sorted batch to temporary file
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
            log_info!("[RUST] No data to sort in file: {}", temp_filename);
            std::fs::rename(temp_filename, output_filename)?;
            return Ok(());
        }

        log_info!("[RUST] Created {} sorted chunks, now merging", batch_count);

        // Merge all sorted chunks
        let merged_batch = Self::merge_sorted_chunks(&temp_files, sort_column, reverse_sort)?;

        // Clean up chunk files
        for chunk_file in &temp_files {
            let _ = std::fs::remove_file(chunk_file);
        }

        let schema = merged_batch.schema();
        let final_batch = Self::rewrite_row_ids(&merged_batch, &schema)?;

        Self::write_final_file(output_filename, &final_batch, schema)?;
        log_info!("[RUST] Sorted file written: {}", output_filename);

        Ok(())
    }

    /// Sort a single RecordBatch by the given column.
    fn sort_batch(
        batch: &RecordBatch,
        sort_column: &str,
        reverse: bool,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let col_index = batch.schema().index_of(sort_column)
            .map_err(|_| format!("Sort column '{}' not found in schema", sort_column))?;

        let sort_col = batch.column(col_index);

        let options = arrow::compute::SortOptions {
            descending: reverse,
            nulls_first: !reverse,
        };

        let indices = sort_to_indices(sort_col, Some(options), None)?;

        let sorted_columns: Result<Vec<_>, _> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect();
        let sorted_columns = sorted_columns?;

        let sorted_batch = RecordBatch::try_new(batch.schema(), sorted_columns)?;

        log_debug!(
            "[RUST] Sorted {} rows by column '{}' (reverse={})",
            batch.num_rows(), sort_column, reverse
        );

        Ok(sorted_batch)
    }

    /// Merge multiple sorted chunk files into one sorted RecordBatch.
    fn merge_sorted_chunks(
        chunk_files: &[std::path::PathBuf],
        sort_column: &str,
        reverse: bool,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        if chunk_files.len() == 1 {
            let file = File::open(&chunk_files[0])?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let mut reader = builder.build()?;
            let batch = reader.next().unwrap()?;
            return Ok(batch);
        }

        // Read all chunks into memory and merge-sort
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

        // Final sort across all chunks
        Self::sort_batch(&combined, sort_column, reverse)
    }

    /// If a ___row_id column exists, rewrite it with sequential values 0..N.
    fn rewrite_row_ids(
        batch: &RecordBatch,
        schema: &Arc<arrow::datatypes::Schema>,
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        use arrow::array::Int64Array;

        if let Some(row_id_idx) = schema.fields().iter().position(|f| f.name() == "___row_id") {
            log_info!("[RUST] Rewriting ___row_id column with sequential values 0..{}", batch.num_rows());

            let sequential_ids = Int64Array::from_iter_values(
                (0..batch.num_rows() as u64).map(|x| x as i64)
            );

            let mut new_columns = batch.columns().to_vec();
            new_columns[row_id_idx] = Arc::new(sequential_ids);

            Ok(RecordBatch::try_new(schema.clone(), new_columns)?)
        } else {
            log_debug!("[RUST] No ___row_id column found, keeping batch as-is");
            Ok(batch.clone())
        }
    }

    /// Write the final sorted batch to the output file with default properties.
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

        log_info!("[RUST] Successfully wrote final file: {}", output_filename);
        Ok(())
    }

    fn flush_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        log_info!("[RUST] fsync_file called for file: {}", filename);

        // After close_writer the final file is at `filename`; open it for fsync
        let file = match File::open(&filename) {
            Ok(f) => f,
            Err(e) => {
                log_error!("[RUST] ERROR: Failed to open file for fsync: {}", filename);
                return Err(e.into());
            }
        };

        match file.sync_all() {
            Ok(_) => {
                log_info!("[RUST] Successfully fsynced file: {}", filename);
                // Also clean up any leftover FILE_MANAGER entry for the temp file
                let temp_filename = Self::temp_filename(&filename);
                FILE_MANAGER.remove(&temp_filename);
                Ok(())
            }
            Err(e) => {
                log_error!("[RUST] ERROR: Failed to fsync file: {}", filename);
                Err(e.into())
            }
        }
    }

    fn get_filtered_writer_memory_usage(path_prefix: String) -> Result<usize, Box<dyn std::error::Error>> {
        log_debug!("[RUST] get_filtered_writer_memory_usage called with prefix: {}", path_prefix);

        let mut total_memory = 0;
        let mut writer_count = 0;

        for entry in WRITER_MANAGER.iter() {
            let filename = entry.key();
            let writer_arc = entry.value();

            // Filter writers by path prefix (temp filenames live in the same directory)
            if filename.starts_with(&path_prefix) {
                if let Ok(writer) = writer_arc.lock() {
                    let memory_usage = writer.memory_size();
                    total_memory += memory_usage;
                    writer_count += 1;

                    log_debug!("[RUST] Filtered Writer {}: {} bytes", filename, memory_usage);
                }
            }
        }

        log_debug!("[RUST] Total memory usage across {} filtered ArrowWriters (prefix: {}): {} bytes", writer_count, path_prefix, total_memory);

        Ok(total_memory)
    }

    fn get_file_metadata(filename: String) -> Result<FileFileMetaData, Box<dyn std::error::Error>> {
        log_debug!("[RUST] get_file_metadata called for file: {}\n", filename);

        // Open the Parquet file
        let file = match File::open(&filename) {
            Ok(f) => f,
            Err(e) => {
                log_error!("[RUST] ERROR: Failed to open file {}: {:?}", filename, e);
                return Err(format!("File not found: {}", filename).into());
            }
        };

        // Create SerializedFileReader
        let reader = match SerializedFileReader::new(file) {
            Ok(r) => r,
            Err(e) => {
                log_error!("[RUST] ERROR: Failed to create Parquet reader for {}: {:?}", filename, e);
                return Err(format!("Invalid Parquet file format: {}", e).into());
            }
        };

        // Get metadata from the reader
        let parquet_metadata = reader.metadata();
        let file_metadata = parquet_metadata.file_metadata().clone();

        log_debug!("[RUST] Successfully read metadata from file: {}, version={}, num_rows={}\n",
                                  filename, file_metadata.version(), file_metadata.num_rows());

        Ok(file_metadata)
    }

    fn create_java_metadata_from_file<'local>(env: &mut JNIEnv<'local>, metadata: &FileFileMetaData) -> Result<JObject<'local>, Box<dyn std::error::Error>> {
        // Find the ParquetFileMetadata class
        let class = env.find_class("com/parquet/parquetdataformat/bridge/ParquetFileMetadata")?;

        // Create Java String for created_by (handle None case)
        let created_by_jstring = match metadata.created_by() {
            Some(created_by) => env.new_string(created_by)?,
            None => JObject::null().into(),
        };

        // Create the Java object using new_object with signature
        let java_metadata = env.new_object(&class, "(IJLjava/lang/String;)V", &[
            (metadata.version()).into(),
            (metadata.num_rows()).into(),
            (&created_by_jstring).into(),
        ])?;

        Ok(java_metadata)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// JNI entry points
// ═══════════════════════════════════════════════════════════════════════════

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    // Initialize the logger using the shared crate
    vectorized_exec_spi::logger::init_logger_from_env(&env);
}

/// JNI entry point for createWriter.
/// Matches Java: createWriter(String file, long schemaAddress, String sortColumn, boolean reverseSort) throws IOException
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_createWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    schema_address: jlong,
    sort_column: JString,
    reverse_sort: jboolean,
) {
    let filename: String = env.get_string(&file).expect("Couldn't get file string!").into();

    // sort_column can be null from Java — handle it safely
    let sort_col: Option<String> = if sort_column.is_null() {
        None
    } else {
        Some(env.get_string(&sort_column).expect("Couldn't get sort_column string!").into())
    };

    let reverse = reverse_sort != 0;

    match NativeParquetWriter::create_writer(filename, schema_address as i64, sort_col, reverse) {
        Ok(_) => {},
        Err(e) => {
            log_error!("[RUST] create_writer failed: {}", e);
            // Throw IOException to Java since the native method declares throws IOException
            let _ = env.throw_new(
                "java/io/IOException",
                &format!("create_writer failed: {}", e),
            );
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_onSettingsUpdate(
    mut env: JNIEnv,
    _class: JClass,
    settings: JObject
) {
    match read_native_settings(&mut env, &settings) {
        Ok(config) => {
            let index_name = match &config.index_name {
                Some(name) => name.clone(),
                None => {
                    log_error!("[RUST] onSettingsUpdate: missing index_name, cannot store settings");
                    let _ = env.throw_new("java/io/IOException", "NativeSettings missing indexName");
                    return;
                }
            };
            log_info!(
                "[RUST] onSettingsUpdate: storing settings for index '{}' - compression_type={:?}, compression_level={:?}, \
                 page_size_bytes={:?}, page_row_limit={:?}, dict_size_bytes={:?}, row_group_size_bytes={:?}",
                index_name,
                config.compression_type,
                config.compression_level,
                config.page_size_bytes,
                config.page_row_limit,
                config.dict_size_bytes,
                config.row_group_size_bytes
            );
            SETTINGS_STORE.insert(index_name, config);
        }
        Err(e) => {
            log_error!("[RUST] onSettingsUpdate: failed to read NativeSettings object: {}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to read NativeSettings: {}", e));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_removeSettings(
    mut env: JNIEnv,
    _class: JClass,
    index_name: JString
) {
    match env.get_string(&index_name) {
        Ok(name) => {
            let name: String = name.into();
            SETTINGS_STORE.remove(&name);
            log_info!("[RUST] removeSettings: removed settings for index '{}'", name);
        }
        Err(e) => {
            log_error!("[RUST] removeSettings: failed to read index name: {}", e);
        }
    }
}

/// Reads a NativeSettings Java object into a Rust NativeSettings struct via JNI getters.
fn read_native_settings(env: &mut JNIEnv, obj: &JObject) -> Result<NativeSettings, Box<dyn std::error::Error>> {
    macro_rules! get_boxed_int {
        ($method:expr) => {{
            let jobj = env.call_method(obj, $method, "()Ljava/lang/Integer;", &[])?.l()?;
            if jobj.is_null() {
                None
            } else {
                Some(env.call_method(&jobj, "intValue", "()I", &[])?.i()?)
            }
        }};
    }

    macro_rules! get_boxed_long_as_usize {
        ($method:expr) => {{
            let jobj = env.call_method(obj, $method, "()Ljava/lang/Long;", &[])?.l()?;
            if jobj.is_null() {
                None
            } else {
                Some(env.call_method(&jobj, "longValue", "()J", &[])?.j()? as usize)
            }
        }};
    }

    macro_rules! get_string {
        ($method:expr) => {{
            let jobj = env.call_method(obj, $method, "()Ljava/lang/String;", &[])?.l()?;
            if jobj.is_null() {
                None
            } else {
                let s: String = env.get_string(&jobj.into())?.into();
                Some(s)
            }
        }};
    }

    Ok(NativeSettings {
        index_name:           get_string!("getIndexName"),
        compression_type:     get_string!("getCompressionType"),
        compression_level:    get_boxed_int!("getCompressionLevel"),
        page_size_bytes:      get_boxed_long_as_usize!("getPageSizeBytes"),
        page_row_limit:       get_boxed_int!("getPageRowLimit").map(|v| v as usize),
        dict_size_bytes:      get_boxed_long_as_usize!("getDictSizeBytes"),
        row_group_size_bytes: get_boxed_long_as_usize!("getRowGroupSizeBytes"),
        field_configs:        None,
        custom_settings:      None,
    })
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
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::close_writer(filename) {
        Ok(maybe_metadata) => {
            match maybe_metadata {
                Some(metadata) => {
                    match NativeParquetWriter::create_java_metadata_from_file(&mut env, &metadata) {
                        Ok(java_obj) => java_obj.into_raw(),
                        Err(e) => {
                            let error_msg = format!("[RUST] ERROR: Failed to create Java metadata object: {:?}\n", e);
                            log_error!("{}", error_msg.trim());
                            // Throw IOException to Java
                            let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                            JObject::null().into_raw()
                        }
                    }
                }
                None => {
                    // No writer was found, but this is not necessarily an error
                    // Return null to indicate no metadata available
                    JObject::null().into_raw()
                }
            }
        }
        Err(e) => {
            log_error!("[RUST] ERROR: Failed to close writer: {:?}\n", e);
            // Throw IOException to Java
            let _ = env.throw_new("java/io/IOException", &format!("Failed to close writer: {}", e));
            JObject::null().into_raw()
        }
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
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_getFileMetadata(
    mut env: JNIEnv,
    _class: JClass,
    file: JString
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::get_file_metadata(filename) {
        Ok(metadata) => {
            match NativeParquetWriter::create_java_metadata_from_file(&mut env, &metadata) {
                Ok(java_obj) => java_obj.into_raw(),
                Err(e) => {
                    let error_msg = format!("[RUST] ERROR: Failed to create Java metadata object: {:?}\n", e);
                    println!("{}", error_msg.trim());
                    log_error!("{}", error_msg);
                    // Throw IOException to Java
                    let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                    JObject::null().into_raw()
                }
            }
        }
        Err(e) => {
            let error_msg = format!("[RUST] ERROR: Failed to read file metadata: {:?}\n", e);
            println!("{}", error_msg.trim());
            log_error!("{}", error_msg);
            // Throw IOException to Java
            let _ = env.throw_new("java/io/IOException", &format!("Failed to read file metadata: {}", e));
            JObject::null().into_raw()
        }
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
        Ok(memory) => memory as jlong,
        Err(_) => 0,
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StructArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi::FFI_ArrowArray;
    use arrow::ffi::FFI_ArrowSchema;
    use arrow_array::Array;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use tempfile::tempdir;

    fn create_test_ffi_schema() -> (Arc<Schema>, i64) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;

        (schema, schema_ptr)
    }

    fn cleanup_ffi_schema(schema_ptr: i64) {
        unsafe {
            let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
        }
    }

    fn create_test_ffi_data() -> Result<(i64, i64), Box<dyn std::error::Error>> {
        use arrow::array::{Int32Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None]));

        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![id_array, name_array],
        )?;

        // Convert to struct array (what Java VectorSchemaRoot exports)
        let struct_array = StructArray::from(record_batch);
        let array_data = struct_array.into_data();

        // Create FFI representations
        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;

        Ok((array_ptr, schema_ptr))
    }

    /// Create test FFI data with specific IDs for sort testing
    fn create_test_ffi_data_with_ids(ids: Vec<i32>, names: Vec<Option<&str>>) -> Result<(i64, i64), Box<dyn std::error::Error>> {
        use arrow::array::{Int32Array, StringArray};

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Arc::new(Int32Array::from(ids));
        let name_array = Arc::new(StringArray::from(names));

        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![id_array, name_array],
        )?;

        let struct_array = StructArray::from(record_batch);
        let array_data = struct_array.into_data();

        let ffi_array = FFI_ArrowArray::new(&array_data);
        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref())?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as i64;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as i64;

        Ok((array_ptr, schema_ptr))
    }

    fn cleanup_ffi_data(array_ptr: i64, schema_ptr: i64) {
        unsafe {
            let _ = Box::from_raw(array_ptr as *mut FFI_ArrowArray);
            let _ = Box::from_raw(schema_ptr as *mut FFI_ArrowSchema);
        }
    }

    fn get_temp_file_path(name: &str) -> (tempfile::TempDir, String) {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join(name);
        let filename = file_path.to_string_lossy().to_string();
        (temp_dir, filename)
    }

    /// Create a writer with no sorting
    fn create_writer_and_assert_success(filename: &str) -> (Arc<Schema>, i64) {
        let (schema, schema_ptr) = create_test_ffi_schema();
        let result = NativeParquetWriter::create_writer(filename.to_string(), schema_ptr, None, false);
        assert!(result.is_ok());
        (schema, schema_ptr)
    }

    /// Create a writer with sorting enabled
    fn create_sorted_writer_and_assert_success(filename: &str, sort_column: &str, reverse: bool) -> (Arc<Schema>, i64) {
        let (schema, schema_ptr) = create_test_ffi_schema();
        let result = NativeParquetWriter::create_writer(
            filename.to_string(), schema_ptr, Some(sort_column.to_string()), reverse
        );
        assert!(result.is_ok());
        (schema, schema_ptr)
    }

    fn close_writer_and_cleanup_schema(filename: &str, schema_ptr: i64) {
        let _ = NativeParquetWriter::close_writer(filename.to_string());
        cleanup_ffi_schema(schema_ptr);
    }

    fn write_ffi_data_to_writer(filename: &str) -> (i64, i64) {
        let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
        let result = NativeParquetWriter::write_data(filename.to_string(), array_ptr, data_schema_ptr);
        assert!(result.is_ok());
        (array_ptr, data_schema_ptr)
    }

    /// Helper to read back a parquet file and return all RecordBatches
    fn read_parquet_file(filename: &str) -> Vec<RecordBatch> {
        let file = File::open(filename).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();
        reader.collect::<Result<Vec<_>, _>>().unwrap()
    }

    #[test]
    fn test_create_writer_success() {
        let (_temp_dir, filename) = get_temp_file_path("test.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let temp = NativeParquetWriter::temp_filename(&filename);
        assert!(WRITER_MANAGER.contains_key(&temp));

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_create_writer_invalid_path() {
        let invalid_path = "/invalid/path/that/does/not/exist/test.parquet";
        let (_schema, schema_ptr) = create_test_ffi_schema();

        let result = NativeParquetWriter::create_writer(invalid_path.to_string(), schema_ptr, None, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No such file or directory"));

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_create_writer_invalid_schema_pointer() {
        let (_temp_dir, filename) = get_temp_file_path("invalid_schema.parquet");

        // Test with null schema pointer
        let result = NativeParquetWriter::create_writer(filename, 0, None, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid schema address"));
    }

    #[test]
    fn test_create_writer_multiple_times_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("duplicate.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Second writer creation for same file should fail
        let (_, schema_ptr2) = create_test_ffi_schema();
        let result2 = NativeParquetWriter::create_writer(filename.clone(), schema_ptr2, None, false);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("Writer already exists"));

        cleanup_ffi_schema(schema_ptr2);
        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_write_data_success() {
        let (_temp_dir, filename) = get_temp_file_path("write_ffi_test.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write data using complete FFI flow
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);

        // Cleanup FFI data
        cleanup_ffi_data(array_ptr, data_schema_ptr);
        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_write_data_no_writer() {
        let (array_ptr, schema_ptr) = create_test_ffi_data().unwrap();

        let result = NativeParquetWriter::write_data("nonexistent.parquet".to_string(), array_ptr, schema_ptr);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Writer not found"));

        cleanup_ffi_data(array_ptr, schema_ptr);
    }

    #[test]
    fn test_write_data_multiple_batches() {
        let (_temp_dir, filename) = get_temp_file_path("multi_write_ffi.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write multiple batches using FFI
        for _ in 0..3 {
            let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
            cleanup_ffi_data(array_ptr, data_schema_ptr);
        }

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_write_data_invalid_pointers() {
        let (_temp_dir, filename) = get_temp_file_path("invalid_ffi.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Test with schema and array pointers both null
        let result = NativeParquetWriter::write_data(filename.clone(), 0, 0);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid FFI addresses"));

        // Test with one null pointer
        let result = NativeParquetWriter::write_data(filename.clone(), 0, schema_ptr);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid FFI addresses"));

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_close_writer_success() {
        let (_temp_dir, filename) = get_temp_file_path("test_close.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write some data first so the file is valid
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
        cleanup_ffi_data(array_ptr, data_schema_ptr);

        let result = NativeParquetWriter::close_writer(filename.clone());

        assert!(result.is_ok());
        let temp = NativeParquetWriter::temp_filename(&filename);
        assert!(!WRITER_MANAGER.contains_key(&temp));
        // Final file should exist
        assert!(Path::new(&filename).exists());

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_close_nonexistent_writer() {
        let result = NativeParquetWriter::close_writer("nonexistent.parquet".to_string());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Writer not found"));
    }

    #[test]
    fn test_close_multiple_times_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("test.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write data so close succeeds
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
        cleanup_ffi_data(array_ptr, data_schema_ptr);

        let result1 = NativeParquetWriter::close_writer(filename.clone());
        assert!(result1.is_ok());
        let result2 = NativeParquetWriter::close_writer(filename);
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("Writer not found"));
        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_flush_to_disk_success() {
        let (_temp_dir, filename) = get_temp_file_path("test_flush.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write data and close first (creates the final file)
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
        cleanup_ffi_data(array_ptr, data_schema_ptr);
        let _ = NativeParquetWriter::close_writer(filename.clone());

        let result = NativeParquetWriter::flush_to_disk(filename.clone());
        assert!(result.is_ok());

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_flush_nonexistent_file() {
        let result = NativeParquetWriter::flush_to_disk("nonexistent.parquet".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_complete_writer_lifecycle() {
        let (_temp_dir, filename) = get_temp_file_path("complete_workflow.parquet");
        let file_path = Path::new(&filename);

        // Step 1: Create schema and writer
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Step 2: Write multiple batches
        for _i in 0..3 {
            let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
            cleanup_ffi_data(array_ptr, data_schema_ptr);
        }

        // Step 3: Close writer (sorts and writes final file)
        let close_result = NativeParquetWriter::close_writer(filename.clone());
        assert!(close_result.is_ok());
        assert!(close_result.unwrap().is_some());

        // Step 4: Flush to disk
        assert!(NativeParquetWriter::flush_to_disk(filename.clone()).is_ok());

        // Step 5: Verify file exists and has content
        assert!(file_path.exists());
        assert!(file_path.metadata().unwrap().len() > 0);

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_sorted_writer_ascending() {
        let (_temp_dir, filename) = get_temp_file_path("sorted_asc.parquet");

        // Create writer with sort on "id" column, ascending
        let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", false);

        // Write unsorted batches
        let (ap1, sp1) = create_test_ffi_data_with_ids(
            vec![30, 10, 50], vec![Some("C"), Some("A"), Some("E")]
        ).unwrap();
        NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
        cleanup_ffi_data(ap1, sp1);

        let (ap2, sp2) = create_test_ffi_data_with_ids(
            vec![20, 40], vec![Some("B"), Some("D")]
        ).unwrap();
        NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
        cleanup_ffi_data(ap2, sp2);

        // Close triggers sort + write to final file
        NativeParquetWriter::close_writer(filename.clone()).unwrap();

        // Read back and verify sorted order
        let batches = read_parquet_file(&filename);
        let combined = concat_batches(&batches[0].schema(), &batches).unwrap();

        let id_col = combined.column(0)
            .as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();

        let ids: Vec<i32> = (0..id_col.len()).map(|i| id_col.value(i)).collect();
        assert_eq!(ids, vec![10, 20, 30, 40, 50], "Data should be sorted ascending by id");

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_sorted_writer_descending() {
        let (_temp_dir, filename) = get_temp_file_path("sorted_desc.parquet");

        // Create writer with sort on "id" column, descending (reverse=true)
        let (_schema, schema_ptr) = create_sorted_writer_and_assert_success(&filename, "id", true);

        // Write unsorted batches
        let (ap1, sp1) = create_test_ffi_data_with_ids(
            vec![30, 10, 50], vec![Some("C"), Some("A"), Some("E")]
        ).unwrap();
        NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
        cleanup_ffi_data(ap1, sp1);

        let (ap2, sp2) = create_test_ffi_data_with_ids(
            vec![20, 40], vec![Some("B"), Some("D")]
        ).unwrap();
        NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
        cleanup_ffi_data(ap2, sp2);

        // Close triggers sort + write
        NativeParquetWriter::close_writer(filename.clone()).unwrap();

        // Read back and verify descending order
        let batches = read_parquet_file(&filename);
        let combined = concat_batches(&batches[0].schema(), &batches).unwrap();

        let id_col = combined.column(0)
            .as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();

        let ids: Vec<i32> = (0..id_col.len()).map(|i| id_col.value(i)).collect();
        assert_eq!(ids, vec![50, 40, 30, 20, 10], "Data should be sorted descending by id");

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_unsorted_writer_preserves_insertion_order() {
        let (_temp_dir, filename) = get_temp_file_path("unsorted.parquet");

        // Create writer with NO sort column
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write batches in specific order
        let (ap1, sp1) = create_test_ffi_data_with_ids(
            vec![30, 10, 50], vec![Some("C"), Some("A"), Some("E")]
        ).unwrap();
        NativeParquetWriter::write_data(filename.clone(), ap1, sp1).unwrap();
        cleanup_ffi_data(ap1, sp1);

        let (ap2, sp2) = create_test_ffi_data_with_ids(
            vec![20, 40], vec![Some("B"), Some("D")]
        ).unwrap();
        NativeParquetWriter::write_data(filename.clone(), ap2, sp2).unwrap();
        cleanup_ffi_data(ap2, sp2);

        // Close writes data in insertion order (no sorting, just rename)
        NativeParquetWriter::close_writer(filename.clone()).unwrap();

        // Read back and verify insertion order preserved
        let batches = read_parquet_file(&filename);
        let combined = concat_batches(&batches[0].schema(), &batches).unwrap();

        let id_col = combined.column(0)
            .as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();

        let ids: Vec<i32> = (0..id_col.len()).map(|i| id_col.value(i)).collect();
        assert_eq!(ids, vec![30, 10, 50, 20, 40], "Data should preserve insertion order");

        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_get_filtered_writer_memory_usage_with_writers() {
        let (_temp_dir, filename1) = get_temp_file_path("test1.parquet");
        let (_temp_dir2, filename2) = get_temp_file_path("test2.parquet");
        // Use the temp prefix since writers are stored under temp filenames
        let prefix = Path::new(&NativeParquetWriter::temp_filename(&filename2))
            .parent().unwrap().to_string_lossy().to_string();
        let (_schema1, schema_ptr1) = create_writer_and_assert_success(&filename1);
        let (_schema2, schema_ptr2) = create_writer_and_assert_success(&filename2);

        let result = NativeParquetWriter::get_filtered_writer_memory_usage(prefix);
        assert!(result.is_ok());
        let _memory_usage = result.unwrap();
        assert!(_memory_usage >= 0);

        close_writer_and_cleanup_schema(&filename1, schema_ptr1);
        close_writer_and_cleanup_schema(&filename2, schema_ptr2);
    }

    #[test]
    fn test_concurrent_writer_creation() {
        let temp_dir = tempdir().unwrap();
        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for i in 0..10 {
            let temp_dir_path = temp_dir.path().to_path_buf();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                let file_path = temp_dir_path.join(format!("concurrent_{}.parquet", i));
                let filename = file_path.to_string_lossy().to_string();
                let (_schema, schema_ptr) = create_test_ffi_schema();

                if NativeParquetWriter::create_writer(filename.clone(), schema_ptr, None, false).is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                    // Write data so close can produce a valid file
                    let (ap, sp) = create_test_ffi_data().unwrap();
                    let _ = NativeParquetWriter::write_data(filename.clone(), ap, sp);
                    cleanup_ffi_data(ap, sp);
                    let _ = NativeParquetWriter::close_writer(filename);
                }
                cleanup_ffi_schema(schema_ptr);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(success_count.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn test_concurrent_close_operations_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("close_race.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        // Write data so at least one close can succeed
        let (array_ptr, data_schema_ptr) = write_ffi_data_to_writer(&filename);
        cleanup_ffi_data(array_ptr, data_schema_ptr);

        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Multiple threads trying to close the same writer
        for _ in 0..3 {
            let filename = filename.clone();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                if NativeParquetWriter::close_writer(filename).is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Only one thread should succeed in closing
        assert_eq!(success_count.load(Ordering::SeqCst), 1);
        cleanup_ffi_schema(schema_ptr);
    }

    #[test]
    fn test_concurrent_writes_same_file() {
        let (_temp_dir, filename) = get_temp_file_path("concurrent_write_ffi.parquet");
        let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);

        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Multiple threads writing to same file using FFI
        for _ in 0..5 {
            let filename = filename.clone();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
                if NativeParquetWriter::write_data(filename, array_ptr, data_schema_ptr).is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                cleanup_ffi_data(array_ptr, data_schema_ptr);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(success_count.load(Ordering::SeqCst), 5);

        close_writer_and_cleanup_schema(&filename, schema_ptr);
    }

    #[test]
    fn test_concurrent_writes_different_files() {
        let temp_dir = tempdir().unwrap();
        let file_count = 8;
        let success_count = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        let mut filenames = vec![];
        let mut schema_ptrs = vec![];

        // Create writers for all files first
        for i in 0..file_count {
            let file_path = temp_dir.path().join(format!("concurrent_write_{}.parquet", i));
            let filename = file_path.to_string_lossy().to_string();
            let (_schema, schema_ptr) = create_writer_and_assert_success(&filename);
            filenames.push(filename);
            schema_ptrs.push(schema_ptr);
        }

        // Concurrent write operations to different files
        for i in 0..file_count {
            let filename = filenames[i].clone();
            let success_count = Arc::clone(&success_count);

            let handle = thread::spawn(move || {
                // Write multiple batches to this file
                for _ in 0..2 {
                    let (array_ptr, data_schema_ptr) = create_test_ffi_data().unwrap();
                    if NativeParquetWriter::write_data(filename.clone(), array_ptr, data_schema_ptr).is_ok() {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }
                    cleanup_ffi_data(array_ptr, data_schema_ptr);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // All write operations should succeed (file_count * 2 batches per file)
        assert_eq!(success_count.load(Ordering::SeqCst), file_count * 2);

        // Cleanup
        for (i, filename) in filenames.iter().enumerate() {
            close_writer_and_cleanup_schema(filename, schema_ptrs[i]);
        }
    }
}
