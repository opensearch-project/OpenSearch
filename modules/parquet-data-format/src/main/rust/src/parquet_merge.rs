use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use std::fs::File;
use std::error::Error;
use std::any::Any;
use std::sync::Arc;
use std::panic::AssertUnwindSafe;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use arrow::array::{Int64Array, ArrayRef};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use crate::rate_limited_writer::RateLimitedWriter;

use crate::{log_info, log_error};

// Constants
const READER_BATCH_SIZE: usize = 8192;
const WRITER_BATCH_SIZE: usize = 8192;
const ROW_ID_COLUMN_NAME: &str = "___row_id";

// Custom error types
#[derive(Debug)]
pub enum ParquetMergeError {
    EmptyInput,
    InvalidFile(String),
    SchemaReadError(String),
    WriterCreationError(String),
    BatchProcessingError(String),
}

impl std::fmt::Display for ParquetMergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParquetMergeError::EmptyInput => write!(f, "No input files provided"),
            ParquetMergeError::InvalidFile(path) => write!(f, "Invalid file: {}", path),
            ParquetMergeError::SchemaReadError(msg) => write!(f, "Schema read error: {}", msg),
            ParquetMergeError::WriterCreationError(msg) => write!(f, "Writer creation error: {}", msg),
            ParquetMergeError::BatchProcessingError(msg) => write!(f, "Batch processing error: {}", msg),
        }
    }
}

impl Error for ParquetMergeError {}

// Statistics tracking
struct ProcessingStats {
    files_processed: usize,
    total_rows: usize,
    total_batches: usize,
}

// JNI Entry Point
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesInRust(
    mut env: JNIEnv,
    _class: JClass,
    input_files: JObject,
    output_file: JString,
) -> jint {
    let result = catch_unwind(|| {
        let input_files_vec = convert_java_list_to_vec(&mut env, input_files)
            .map_err(|e| format!("Failed to convert Java list: {}", e))?;

        let output_path: String = env
            .get_string(&output_file)
            .map_err(|e| format!("Failed to get output file string: {}", e))?
            .into();

        log_info!("Starting merge of {} files to {}", input_files_vec.len(), output_path);

        process_parquet_files(&input_files_vec, &output_path)?;

        log_info!("Merge completed successfully");
        Ok(())
    });

    match result {
        Ok(Ok(_)) => 0,
        Ok(Err(e)) => {
            let error_msg = format!("Error processing Parquet files: {}", e);
            log_error!("{}", error_msg);
            let _ = env.throw_new("java/lang/RuntimeException", &error_msg);
            -1
        }
        Err(e) => {
            let error_msg = format!("Rust panic occurred: {:?}", e);
            log_error!("{}", error_msg);
            let _ = env.throw_new("java/lang/RuntimeException", &error_msg);
            -1
        }
    }
}

// Main processing function
pub fn process_parquet_files(input_files: &[String], output_path: &str) -> Result<(), Box<dyn Error>> {
    // Validate input
    validate_input(input_files)?;

    // Read schema from first file
    let schema = read_schema_from_file(&input_files[0])?;
    log_info!("Schema read successfully: {:?}", schema);

    // Create writer
    let mut writer = create_writer(output_path, schema.clone(), input_files)?;

    // Process files
    let stats = process_files(input_files, &schema, &mut writer)?;

    // Close writer
    writer.close()
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to close writer: {}", e)))?;

    log_info!(
        "Processing complete: {} files, {} rows, {} batches",
        stats.files_processed, stats.total_rows, stats.total_batches
    );

    Ok(())
}

// Validation functions
fn validate_input(input_files: &[String]) -> Result<(), Box<dyn Error>> {
    if input_files.is_empty() {
        return Err(Box::new(ParquetMergeError::EmptyInput));
    }

    for path in input_files {
        if !std::path::Path::new(path).exists() {
            return Err(Box::new(ParquetMergeError::InvalidFile(path.clone())));
        }
    }

    Ok(())
}

// Schema reading
fn read_schema_from_file(file_path: &str) -> Result<SchemaRef, Box<dyn Error>> {
    let file = File::open(file_path)
        .map_err(|e| ParquetMergeError::InvalidFile(format!("{}: {}", file_path, e)))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| ParquetMergeError::SchemaReadError(format!("Failed to read schema: {}", e)))?;

    Ok(builder.schema().clone())
}

// Writer creation
fn create_writer(output_path: &str, schema: SchemaRef, input_files: &[String]) -> Result<ArrowWriter<RateLimitedWriter<File>>, Box<dyn Error>> {
    let props = build_merge_writer_properties(output_path, &schema, input_files)?;

    let out_file = File::create(output_path)
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create output file: {}", e)))?;

    let throttled_writer = RateLimitedWriter::new(out_file, 20.0 * 1024.0 * 1024.0)
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create rate limiter: {}", e)))?;

    ArrowWriter::try_new(throttled_writer, schema, Some(props))
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create writer: {}", e)).into())
}

// Writer properties configuration
fn build_merge_writer_properties(output_path: &str, schema: &SchemaRef, input_files: &[String]) -> Result<WriterProperties, Box<dyn Error>> {
    let mut props_builder = WriterProperties::builder()
        .set_write_batch_size(WRITER_BATCH_SIZE)
        .set_compression(Compression::ZSTD(Default::default()));

    let mut found_configs = false;
    for input_file in input_files {
        if let Some(field_configs) = crate::BLOOM_FILTER_CONFIGS.get(input_file) {
            log_info!("Inheriting {} bloom filter configs from source file: {} for merge output: {}", field_configs.len(), input_file, output_path);
            
            for field in schema.fields().iter() {
                if let Some(config) = field_configs.get(field.name()) {
                    if config.enabled {
                        log_info!("Adding inherited bloom filter for field during merge: {} (from source: {})", field.name(), input_file);
                        props_builder = props_builder
                            .set_column_bloom_filter_enabled(field.name().clone().into(), true)
                            .set_column_bloom_filter_fpp(field.name().clone().into(), config.fpp)
                            .set_column_bloom_filter_ndv(field.name().clone().into(), config.ndv);
                    } else {
                        log_info!("Skipping bloom filter for field during merge: {} (disabled in source: {})", field.name(), input_file);
                    }
                } else {
                    log_info!("No bloom filter config found for field during merge: {} (source: {})", field.name(), input_file);
                }
            }
            found_configs = true;
            break; // Use configs from first file that has them
        }
    }

    if !found_configs {
        if let Some(field_configs) = crate::BLOOM_FILTER_CONFIGS.get(output_path) {
            log_info!("Found {} bloom filter configs for merge output file: {}", field_configs.len(), output_path);
            for field in schema.fields().iter() {
                if let Some(config) = field_configs.get(field.name()) {
                    if config.enabled {
                        log_info!("Adding bloom filter for field during merge: {} (enabled via config)", field.name());
                        props_builder = props_builder
                            .set_column_bloom_filter_enabled(field.name().clone().into(), true)
                            .set_column_bloom_filter_fpp(field.name().clone().into(), config.fpp)
                            .set_column_bloom_filter_ndv(field.name().clone().into(), config.ndv);
                    } else {
                        log_info!("Skipping bloom filter for field during merge: {} (disabled via config)", field.name());
                    }
                } else {
                    log_info!("No bloom filter config found for field during merge: {}", field.name());
                }
            }
        } else {
            log_info!("No bloom filter configurations found for merge output file or source files: {}", output_path);
        }
    }

    Ok(props_builder.build())
}

// File processing
fn process_files(
    input_files: &[String],
    schema: &SchemaRef,
    writer: &mut ArrowWriter<RateLimitedWriter<File>>,
) -> Result<ProcessingStats, Box<dyn Error>> {
    let mut current_row_id: i64 = 0;
    let mut stats = ProcessingStats {
        files_processed: 0,
        total_rows: 0,
        total_batches: 0,
    };

    for path in input_files {
        log_info!("Processing file: {}", path);

        let file = File::open(path)
            .map_err(|e| ParquetMergeError::InvalidFile(format!("{}: {}", path, e)))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to create reader: {}", e)))?
            .with_batch_size(READER_BATCH_SIZE)
            .build()
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to build reader: {}", e)))?;

        let mut file_rows = 0;
        let mut file_batches = 0;

        for batch_result in reader {
            let original_batch = batch_result
                .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to read batch: {}", e)))?;

            let batch_rows = original_batch.num_rows();

            let new_batch = update_row_ids(&original_batch, current_row_id, schema)?;

            writer.write(&new_batch)
                .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to write batch: {}", e)))?;

            current_row_id += batch_rows as i64;
            file_rows += batch_rows;
            file_batches += 1;
        }

        stats.files_processed += 1;
        stats.total_rows += file_rows;
        stats.total_batches += file_batches;

        log_info!("File processed: {} rows, {} batches", file_rows, file_batches);
    }

    Ok(stats)
}

// Row ID update logic
pub fn update_row_ids(
    original_batch: &RecordBatch,
    start_id: i64,
    schema: &SchemaRef,
) -> Result<RecordBatch, Box<dyn Error>> {
    let row_count = original_batch.num_rows();

    // Create new row IDs
    let row_ids: Int64Array = (start_id..start_id + row_count as i64)
        .collect::<Vec<i64>>()
        .into();

    // Build new columns array
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(original_batch.num_columns());

    for (i, column) in original_batch.columns().iter().enumerate() {
        let field_name = schema.field(i).name();
        if field_name == ROW_ID_COLUMN_NAME {
            columns.push(Arc::new(row_ids.clone()));
        } else {
            columns.push(column.clone());
        }
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to create batch: {}", e)).into())
}

// JNI helper functions
fn convert_java_list_to_vec(env: &mut JNIEnv, list: JObject) -> Result<Vec<String>, Box<dyn Error>> {
    let iterator = env.call_method(&list, "iterator", "()Ljava/util/Iterator;", &[])?
        .l()?;

    let mut result = Vec::new();
    while env.call_method(&iterator, "hasNext", "()Z", &[])?.z()? {
        let element = env.call_method(&iterator, "next", "()Ljava/lang/Object;", &[])?
            .l()?;
        let path_string = env.call_method(&element, "toString", "()Ljava/lang/String;", &[])?
            .l()?;
        let jstring = JString::from(path_string);
        let string = env.get_string(&jstring)?;
        result.push(string.to_str()?.to_string());
    }

    Ok(result)
}

fn catch_unwind<F: FnOnce() -> Result<(), Box<dyn Error>>>(
    f: F
) -> Result<Result<(), Box<dyn Error>>, Box<dyn Any + Send>> {
    std::panic::catch_unwind(AssertUnwindSafe(f))
}


// Close function
// #[no_mangle]
// pub extern "system" fn Java_org_opensearch_arrow_bridge_ArrowRustBridge_close(
//     _env: JNIEnv,
//     _class: JClass,
// ) {
//     log_info("Closing ArrowRustBridge");
// }
