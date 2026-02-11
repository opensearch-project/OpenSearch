use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString, JValue};
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

// Row ID mapping for cross-format merge
struct RowIdMappingData {
    old_file_id: String,
    old_row_id: i64,
    new_row_id: i64,
}

// JNI Entry Point - returns RowIdMapping to Java
#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesInRust<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    input_files: JObject<'local>,
    output_file: JString<'local>,
) -> JObject<'local> {
    let result = catch_unwind(|| {
        let input_files_vec = convert_java_list_to_vec(&mut env, input_files)
            .map_err(|e| format!("Failed to convert Java list: {}", e))?;

        let output_path: String = env
            .get_string(&output_file)
            .map_err(|e| format!("Failed to get output file string: {}", e))?
            .into();

        log_info!("Starting merge of {} files to {}", input_files_vec.len(), output_path);

        let (mappings, output_file_id) = process_parquet_files(&input_files_vec, &output_path)?;

        log_info!("Merge completed successfully");
        Ok((mappings, output_file_id))
    });

    match result {
        Ok(Ok((mappings, output_file_id))) => {
            match create_row_id_mapping_object(&mut env, mappings, &output_file_id) {
                Ok(obj) => obj,
                Err(e) => {
                    let error_msg = format!("Failed to create RowIdMapping: {}", e);
                    log_error!("{}", error_msg);
                    let _ = env.throw_new("java/lang/RuntimeException", &error_msg);
                    JObject::null()
                }
            }
        }
        Ok(Err(e)) => {
            let error_msg = format!("Error processing Parquet files: {}", e);
            log_error!("{}", error_msg);
            let _ = env.throw_new("java/lang/RuntimeException", &error_msg);
            JObject::null()
        }
        Err(e) => {
            let error_msg = format!("Rust panic occurred: {:?}", e);
            log_error!("{}", error_msg);
            let _ = env.throw_new("java/lang/RuntimeException", &error_msg);
            JObject::null()
        }
    }
}

// Main processing function - returns row ID mappings
pub fn process_parquet_files(input_files: &[String], output_path: &str) -> Result<(Vec<RowIdMappingData>, String), Box<dyn Error>> {
    // Validate input
    validate_input(input_files)?;

    // Read schema from first file
    let schema = read_schema_from_file(&input_files[0])?;
    log_info!("Schema read successfully: {:?}", schema);

    // Create writer
    let mut writer = create_writer(output_path, schema.clone())?;

    // Process files and collect mappings
    let (stats, mappings) = process_files(input_files, &schema, &mut writer)?;

    // Close writer
    writer.close()
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to close writer: {}", e)))?;

    log_info!(
        "Processing complete: {} files, {} rows, {} batches, {} mappings",
        stats.files_processed, stats.total_rows, stats.total_batches, mappings.len()
    );

    let output_file_id = std::path::Path::new(output_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(output_path)
        .to_string();

    Ok((mappings, output_file_id))
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
fn create_writer(output_path: &str, schema: SchemaRef) -> Result<ArrowWriter<RateLimitedWriter<File>>, Box<dyn Error>> {
    let props = WriterProperties::builder()
        .set_write_batch_size(WRITER_BATCH_SIZE)
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    let out_file = File::create(output_path)
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create output file: {}", e)))?;

    let throttled_writer = RateLimitedWriter::new(out_file, 20.0 * 1024.0 * 1024.0)
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create rate limiter: {}", e)))?;

    ArrowWriter::try_new(throttled_writer, schema, Some(props))
        .map_err(|e| ParquetMergeError::WriterCreationError(format!("Failed to create writer: {}", e)).into())
}

// File processing - collects row ID mappings
fn process_files(
    input_files: &[String],
    schema: &SchemaRef,
    writer: &mut ArrowWriter<RateLimitedWriter<File>>,
) -> Result<(ProcessingStats, Vec<RowIdMappingData>), Box<dyn Error>> {
    let mut current_row_id: i64 = 0;
    let mut stats = ProcessingStats {
        files_processed: 0,
        total_rows: 0,
        total_batches: 0,
    };
    let mut mappings = Vec::new();

    for path in input_files {
        log_info!("Processing file: {}", path);

        let old_file_id = std::path::Path::new(path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(path)
            .to_string();

        let file = File::open(path)
            .map_err(|e| ParquetMergeError::InvalidFile(format!("{}: {}", path, e)))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to create reader: {}", e)))?
            .with_batch_size(READER_BATCH_SIZE)
            .build()
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to build reader: {}", e)))?;

        let mut file_rows = 0;
        let mut file_batches = 0;
        let file_start_row_id = current_row_id;

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

        // Create mappings for this file
        for old_row_id in 0..file_rows as i64 {
            mappings.push(RowIdMappingData {
                old_file_id: old_file_id.clone(),
                old_row_id,
                new_row_id: file_start_row_id + old_row_id,
            });
        }

        stats.files_processed += 1;
        stats.total_rows += file_rows;
        stats.total_batches += file_batches;

        log_info!("File processed: {} rows, {} batches", file_rows, file_batches);
    }

    Ok((stats, mappings))
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

fn catch_unwind<F: FnOnce() -> Result<(Vec<RowIdMappingData>, String), Box<dyn Error>>>(
    f: F
) -> Result<Result<(Vec<RowIdMappingData>, String), Box<dyn Error>>, Box<dyn Any + Send>> {
    std::panic::catch_unwind(AssertUnwindSafe(f))
}

// Create Java RowIdMapping object
fn create_row_id_mapping_object<'local>(
    env: &mut JNIEnv<'local>,
    mappings: Vec<RowIdMappingData>,
    output_file_id: &str,
) -> Result<JObject<'local>, Box<dyn Error>> {
    // Create HashMap<RowId, Long>
    let hash_map = env.new_object("java/util/HashMap", "()V", &[])?;

    for mapping in mappings {
        // Create RowId object
        let row_id_obj = env.new_object(
            "org/opensearch/index/engine/exec/merge/RowId",
            "(JLjava/lang/String;)V",
            &[
                JValue::Long(mapping.old_row_id),
                JValue::Object(&env.new_string(&mapping.old_file_id)?.into()),
            ],
        )?;

        // Create Long object for new row ID
        let new_row_id_obj = env.new_object(
            "java/lang/Long",
            "(J)V",
            &[JValue::Long(mapping.new_row_id)],
        )?;

        // Put into HashMap
        env.call_method(
            &hash_map,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[JValue::Object(&row_id_obj), JValue::Object(&new_row_id_obj)],
        )?;
    }

    // Create RowIdMapping object
    let row_id_mapping = env.new_object(
        "org/opensearch/index/engine/exec/merge/RowIdMapping",
        "(Ljava/util/Map;Ljava/lang/String;)V",
        &[
            JValue::Object(&hash_map),
            JValue::Object(&env.new_string(output_file_id)?.into()),
        ],
    )?;

    Ok(row_id_mapping)
}


// Close function
// #[no_mangle]
// pub extern "system" fn Java_org_opensearch_arrow_bridge_ArrowRustBridge_close(
//     _env: JNIEnv,
//     _class: JClass,
// ) {
//     log_info("Closing ArrowRustBridge");
// }
