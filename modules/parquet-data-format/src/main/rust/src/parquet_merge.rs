use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use std::fs::File;
use std::error::Error;
use std::any::Any;
use std::sync::Arc;
use std::panic::AssertUnwindSafe;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
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
const READER_BATCH_SIZE: usize = 1024;
const WRITER_BATCH_SIZE: usize = 8192;
const ROW_ID_COLUMN_NAME: &str = "___row_id";


// Priority queue item for sorted merging
#[derive(Debug)]
struct PriorityItem {
    sort_value: i64,
    file_index: usize,
    row_index: usize,
    batch: RecordBatch,
}

impl Eq for PriorityItem {}

impl PartialEq for PriorityItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_value == other.sort_value
    }
}

impl Ord for PriorityItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.sort_value.cmp(&self.sort_value) // Min-heap for ascending order
    }
}

impl PartialOrd for PriorityItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

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
    sort_column: JString,
    reverse_sort: jint,
) -> jint {
    let result = catch_unwind(|| {
        let input_files_vec = convert_java_list_to_vec(&mut env, input_files)
            .map_err(|e| format!("Failed to convert Java list: {}", e))?;

        let output_path: String = env
            .get_string(&output_file)
            .map_err(|e| format!("Failed to get output file string: {}", e))?
            .into();

        let sort_column_name: Option<String> = if sort_column.is_null() {
            None
        } else {
            Some(env
                .get_string(&sort_column)
                .map_err(|e| format!("Failed to get sort column string: {}", e))?
                .into())
        };
        let is_reverse_sort = reverse_sort != 0;

        log_info!("Starting merge of {} files to {} sorted by {:?} (reverse: {})", input_files_vec.len(), output_path, sort_column_name, is_reverse_sort);

        process_parquet_files(&input_files_vec, &output_path, sort_column_name.as_deref(), is_reverse_sort)?;

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
pub fn process_parquet_files(input_files: &[String], output_path: &str, sort_column_name: Option<&str>, reverse_sort: bool) -> Result<(), Box<dyn Error>> {
    // Validate input
    validate_input(input_files)?;

    // Read schema from first file
    let schema = read_schema_from_file(&input_files[0])?;
    log_info!("Schema read successfully: {:?}", schema);

    // Create writer
    let mut writer = create_writer(output_path, schema.clone())?;

    // Process files with or without sorting
    let stats = if let Some(sort_col) = sort_column_name {
        process_files_sorted(input_files, &schema, &mut writer, sort_col, reverse_sort)?
    } else {
        process_files(input_files, &schema, &mut writer)?
    };

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
    log_info!("Processing merge without sorting.");
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


// File processing with sorted merging
fn process_files_sorted(
    input_files: &[String],
    schema: &SchemaRef,
    writer: &mut ArrowWriter<RateLimitedWriter<File>>,
    sort_column_name: &str,
    reverse_sort: bool,
) -> Result<ProcessingStats, Box<dyn Error>> {
    let mut current_row_id: i64 = 0;
    let mut stats = ProcessingStats {
        files_processed: input_files.len(),
        total_rows: 0,
        total_batches: 0,
    };

    log_info!("Processing merge with sorting.");

    // Create readers for all files
    let mut readers = Vec::new();
    for path in input_files {
        let file = File::open(path)
            .map_err(|e| ParquetMergeError::InvalidFile(format!("{}: {}", path, e)))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to create reader: {}", e)))?
            .with_batch_size(READER_BATCH_SIZE)
            .build()
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to build reader: {}", e)))?;
        readers.push(reader);
    }

    // Priority queue for sorted merging
    let mut heap = BinaryHeap::new();
    let mut readers: Vec<_> = readers.into_iter().enumerate().collect();

    // Initialize heap with first record from each file
    for (file_index, reader) in readers.iter_mut() {
        if let Some(batch_result) = reader.next() {
            let batch = batch_result
                .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to read batch: {}", e)))?;

            if batch.num_rows() > 0 {
                let mut sort_value = get_sort_value_from_batch(&batch, 0, schema, sort_column_name)?;
                if reverse_sort {
                    sort_value = -sort_value; // Negate for reverse order
                }
                heap.push(PriorityItem {
                    sort_value,
                    file_index: file_index.clone(),
                    row_index: 0,
                    batch: batch.clone(),
                });

            }
        }
    }

    let mut output_records = Vec::new();

    // Process records in sorted order
    while let Some(item) = heap.pop() {
        // Extract current record
        let record = extract_record_from_batch(&item.batch, item.row_index, schema)?;
        output_records.push(record);

        // Write batch when it reaches the target size
        if output_records.len() >= WRITER_BATCH_SIZE {
            let output_batch = create_batch_from_records(&output_records, schema)?;

            let new_batch = update_row_ids(&output_batch, current_row_id, schema)?;

            writer.write(&new_batch)
                .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to write batch: {}", e)))?;

            current_row_id += new_batch.num_rows() as i64;

            stats.total_rows += output_records.len();
            stats.total_batches += 1;
            output_records.clear();
        }

        // Get next record from the same file
        let next_row_index = item.row_index + 1;
        if next_row_index < item.batch.num_rows() {
            // Next record is in the same batch
            let mut sort_value = get_sort_value_from_batch(&item.batch, next_row_index, schema, sort_column_name)?;
            if reverse_sort {
                sort_value = -sort_value;
            }
            heap.push(PriorityItem {
                sort_value,
                file_index: item.file_index,
                row_index: next_row_index,
                batch: item.batch,
            });
        } else {
            // Need to read next batch from this file
            if let Some(batch_result) = readers.get_mut(item.file_index).and_then(|(_, r)| r.next()) {
                let batch = batch_result
                    .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to read batch: {}", e)))?;
                if batch.num_rows() > 0 {
                    let mut sort_value = get_sort_value_from_batch(&batch, 0, schema, sort_column_name)?;
                    if reverse_sort {
                        sort_value = -sort_value;
                    }
                    heap.push(PriorityItem {
                        sort_value,
                        file_index: item.file_index,
                        row_index: 0,
                        batch: batch.clone(),
                    });

                }
            }
        }
    }

    // Write remaining records
    if !output_records.is_empty() {
        let output_batch = create_batch_from_records(&output_records, schema)?;
        let new_batch = update_row_ids(&output_batch, current_row_id, schema)?;
        writer.write(&new_batch)
            .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to write batch: {}", e)))?;
        stats.total_rows += output_records.len();
        stats.total_batches += 1;
    }

    Ok(stats)
}

// Helper function to get sort value from a batch at specific index
fn get_sort_value_from_batch(batch: &RecordBatch, row_index: usize, schema: &SchemaRef, sort_column_name: &str) -> Result<i64, Box<dyn Error>> {
    for (i, field) in schema.fields().iter().enumerate() {
        if field.name() == sort_column_name {
            let column = batch.column(i);
            let sort_array = column.as_any().downcast_ref::<Int64Array>()
                .ok_or("Sort column is not Int64Array")?;
            return Ok(sort_array.value(row_index));
        }
    }
    return Ok(0);
    // TODO
    // Confirm the behaviour when sort field is not present, today will consider it as 0.
//     Err("Sort column not found".into())
}

// Helper function to extract a single record from batch
fn extract_record_from_batch(batch: &RecordBatch, row_index: usize, _schema: &SchemaRef) -> Result<Vec<ArrayRef>, Box<dyn Error>> {
    let mut record = Vec::new();
    for column in batch.columns() {
        let slice = column.slice(row_index, 1);
        record.push(slice);
    }
    Ok(record)
}

// Helper function to create batch from collected records
fn create_batch_from_records(records: &[Vec<ArrayRef>], schema: &SchemaRef) -> Result<RecordBatch, Box<dyn Error>> {
    if records.is_empty() {
        return Err("No records to create batch".into());
    }

    let mut columns = Vec::new();
    for col_index in 0..schema.fields().len() {
        let mut arrays = Vec::new();
        for record in records {
            arrays.push(record[col_index].clone());
        }
        let array_refs: Vec<&dyn arrow::array::Array> = arrays.iter().map(|a| a.as_ref()).collect();
        let concatenated = arrow::compute::concat(&array_refs)
            .map_err(|e| format!("Failed to concatenate arrays: {}", e))?;
        columns.push(concatenated);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| ParquetMergeError::BatchProcessingError(format!("Failed to create batch: {}", e)).into())
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
