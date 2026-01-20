use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString, JMap};
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

use crate::{log_info, log_error, DEFAULT_BLOOM_FILTER_FPP, DEFAULT_BLOOM_FILTER_NDV};

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
    bloom_filter_map: JObject,
) -> jint {
    let result = catch_unwind(|| {
        let input_files_vec = convert_java_list_to_vec(&mut env, input_files)
            .map_err(|e| format!("Failed to convert Java list: {}", e))?;

        let output_path: String = env
            .get_string(&output_file)
            .map_err(|e| format!("Failed to get output file string: {}", e))?
            .into();

        let bloom_fields = convert_bloom_filter_map(&mut env, bloom_filter_map);

        log_info!("Starting merge of {} files to {} with {} bloom filter fields", 
            input_files_vec.len(), output_path, bloom_fields.len());

        process_parquet_files(&input_files_vec, &output_path, &bloom_fields)?;

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
pub fn process_parquet_files(input_files: &[String], output_path: &str, bloom_filter_fields: &[String]) -> Result<(), Box<dyn Error>> {
    // Validate input
    validate_input(input_files)?;

    // Read schema from first file
    let schema = read_schema_from_file(&input_files[0])?;
    log_info!("Schema read successfully: {:?}", schema);

    // Create writer
    let mut writer = create_writer(output_path, schema.clone(), bloom_filter_fields)?;

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
fn create_writer(output_path: &str, schema: SchemaRef, bloom_filter_fields: &[String]) -> Result<ArrowWriter<RateLimitedWriter<File>>, Box<dyn Error>> {
    let props = build_merge_writer_properties(&schema, bloom_filter_fields)?;

    let out_file = File::create(output_path)
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create output file: {}", e)))?;

    let throttled_writer = RateLimitedWriter::new(out_file, 20.0 * 1024.0 * 1024.0)
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create rate limiter: {}", e)))?;

    ArrowWriter::try_new(throttled_writer, schema, Some(props))
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create writer: {}", e)).into())
}

// Writer properties configuration
fn build_merge_writer_properties(schema: &SchemaRef, bloom_filter_fields: &[String]) -> Result<WriterProperties, Box<dyn Error>> {
    let mut props_builder = WriterProperties::builder()
        .set_write_batch_size(WRITER_BATCH_SIZE)
        .set_compression(Compression::ZSTD(Default::default()));

    if !bloom_filter_fields.is_empty() {
        log_info!("Configuring {} bloom filter fields for merge output", bloom_filter_fields.len());
        
        let bloom_set: std::collections::HashSet<&String> = bloom_filter_fields.iter().collect();
        
        for field in schema.fields().iter() {
            if bloom_set.contains(field.name()) {
                log_info!("Adding bloom filter for field during merge: {} (fpp={}, ndv={})", 
                    field.name(), DEFAULT_BLOOM_FILTER_FPP, DEFAULT_BLOOM_FILTER_NDV);
                props_builder = props_builder
                    .set_column_bloom_filter_enabled(field.name().clone().into(), true)
                    .set_column_bloom_filter_fpp(field.name().clone().into(), DEFAULT_BLOOM_FILTER_FPP)
                    .set_column_bloom_filter_ndv(field.name().clone().into(), DEFAULT_BLOOM_FILTER_NDV);
            }
        }
    } else {
        log_info!("No bloom filter configurations provided for merge output");
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

fn convert_bloom_filter_map(env: &mut JNIEnv, bloom_filter_map: JObject) -> Vec<String> {
    if bloom_filter_map.is_null() { return Vec::new(); }
    let mut bloom_fields = Vec::new(); let map = JMap::from_env(env, &bloom_filter_map).expect("Couldn't get Java Map!"); let mut iter = map.iter(env).expect("Couldn't get map iterator!"); while let Some((k, v)) = iter.next(env).expect("Error iterating map") { let field_name: String = env.get_string(&JString::from(k)).expect("Couldn't get field name!").into(); let boolean_class = env.find_class("java/lang/Boolean").expect("Couldn't find Boolean class"); let boolean_value_method = env.get_method_id(&boolean_class, "booleanValue", "()Z").expect("Couldn't find booleanValue method"); let is_enabled = unsafe { env.call_method_unchecked(&v, boolean_value_method, jni::signature::ReturnType::Primitive(jni::signature::Primitive::Boolean), &[]).expect("Couldn't call booleanValue").z().expect("Couldn't get boolean value") }; if is_enabled { bloom_fields.push(field_name); } } bloom_fields
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
