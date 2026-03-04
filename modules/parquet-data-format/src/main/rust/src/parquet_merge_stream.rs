// src/parquet_merge_stream.rs

use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fs::File;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const ROW_ID_COLUMN_NAME: &str = "__row_id";
const BATCH_SIZE: usize = 50_000;
const OUTPUT_FLUSH_ROWS: usize = 100_000;

/// Cursor over one parquet file - TRUE STREAMING
struct FileCursor {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    batch: RecordBatch,
    row_idx: usize,
    file_id: usize,
    sort_col_idx: usize,
    sort_values: Vec<i64>,
}

impl FileCursor {
    fn try_new(path: &str, file_id: usize, batch_size: usize, sort_column: &str) -> Result<Option<Self>, String> {
        let file = File::open(path).map_err(|e| e.to_string())?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| e.to_string())?;

        let schema = builder.schema().clone();
        let sort_col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == sort_column)
            .ok_or_else(|| format!("Sort column '{}' not found", sort_column))?;

        let mut reader = builder
            .with_batch_size(batch_size)
            .build()
            .map_err(|e| e.to_string())?;

        // Read first batch
        let first_batch = match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => return Err(e.to_string()),
            None => return Ok(None),
        };

        let sort_values = extract_sort_values(&first_batch, sort_col_idx)?;

        Ok(Some(FileCursor {
            reader,
            batch: first_batch,
            row_idx: 0,
            file_id,
            sort_col_idx,
            sort_values,
        }))
    }

    fn next_batch(&mut self) -> Result<bool, String> {
        match self.reader.next() {
            Some(Ok(batch)) => {
                self.sort_values = extract_sort_values(&batch, self.sort_col_idx)?;
                self.batch = batch;
                self.row_idx = 0;
                Ok(true)
            }
            Some(Err(e)) => Err(e.to_string()),
            None => Ok(false),
        }
    }

    #[inline]
    fn current_sort_value(&self) -> i64 {
        self.sort_values[self.row_idx]
    }

    fn count_rows_le(&self, threshold: i64) -> usize {
        let mut count = 0;
        let mut idx = self.row_idx;
        while idx < self.sort_values.len() && self.sort_values[idx] <= threshold {
            count += 1;
            idx += 1;
        }
        count
    }

    /// Slice current batch
    fn slice(&self, offset: usize, length: usize) -> RecordBatch {
        self.batch.slice(offset, length)
    }
}

/// Extract sort column as Vec<i64>
fn extract_sort_values(batch: &RecordBatch, col_idx: usize) -> Result<Vec<i64>, String> {
    let array = batch.column(col_idx);

    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.iter().map(|v| v.unwrap_or(i64::MIN)).collect())
        }
        DataType::Timestamp(_, _) => {
            // Handle all timestamp types
            if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                Ok(arr.iter().map(|v| v.unwrap_or(i64::MIN)).collect())
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                Ok(arr.iter().map(|v| v.unwrap_or(i64::MIN)).collect())
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                Ok(arr.iter().map(|v| v.unwrap_or(i64::MIN)).collect())
            } else if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                Ok(arr.iter().map(|v| v.unwrap_or(i64::MIN)).collect())
            } else {
                Err("Unknown timestamp type".to_string())
            }
        }
        dt => Err(format!("Unsupported sort column type: {:?}", dt)),
    }
}

/// Heap item (min-heap via reversed Ord)
#[derive(Debug)]
struct HeapItem {
    sort_value: i64,
    file_id: usize,
}

impl Eq for HeapItem {}
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_value == other.sort_value
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.sort_value.cmp(&self.sort_value)
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesStreaming(
    mut env: JNIEnv,
    _class: JClass,
    input_files: JObject,
    output_file: JString,
    sort_column: JString,
    _is_reverse: jint,
) -> jint {
    let input_files_vec = match convert_java_list_to_vec(&mut env, input_files) {
        Ok(v) => v,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };

    let output_path: String = env.get_string(&output_file).unwrap().into();
    let sort_column: String = env.get_string(&sort_column).unwrap().into();

    match merge_streaming(&input_files_vec, &output_path, &sort_column) {
        Ok(_) => 0,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e);
            -1
        }
    }
}

/// =========================
/// PURE ARROW STREAMING K-WAY MERGE
/// =========================
pub fn merge_streaming(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
) -> Result<(), String> {
    let mut cursors: Vec<FileCursor> = Vec::new();

    for (file_id, path) in input_files.iter().enumerate() {
        if let Some(cursor) = FileCursor::try_new(path, file_id, BATCH_SIZE, sort_column)? {
            cursors.push(cursor);
        }
    }

    if cursors.is_empty() {
        return Ok(());
    }

    // Get schema from first cursor, add row_id column
    let input_schema = cursors[0].batch.schema();
    let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false)));
    let output_schema = Arc::new(Schema::new(fields));

    // Create output writer
    let output_file = File::create(output_path).map_err(|e| e.to_string())?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(output_file, output_schema.clone(), Some(props))
        .map_err(|e| e.to_string())?;

    let mut heap = BinaryHeap::new();

    for c in &cursors {
        heap.push(HeapItem {
            sort_value: c.current_sort_value(),
            file_id: c.file_id,
        });
    }

    let mut output_batches: Vec<RecordBatch> = Vec::new();
    let mut output_row_count = 0usize;
    let mut next_row_id = 0i64;

    while let Some(item) = heap.pop() {
        let cursor = &mut cursors[item.file_id];

        loop {
            let threshold = heap.peek()
                .map(|top| top.sort_value)
                .unwrap_or(i64::MAX);

            let take_count = cursor.count_rows_le(threshold);

            if take_count > 0 {
                let rows = cursor.slice(cursor.row_idx, take_count);
                output_row_count += rows.num_rows();
                output_batches.push(rows);
                cursor.row_idx += take_count;
            }

            if cursor.row_idx >= cursor.batch.num_rows() {
                if !cursor.next_batch()? {
                    break;
                }
                continue;
            }

            heap.push(HeapItem {
                sort_value: cursor.current_sort_value(),
                file_id: cursor.file_id,
            });
            break;
        }

        if output_row_count >= OUTPUT_FLUSH_ROWS {
            flush_arrow(&mut writer, &output_batches, &input_schema, &mut next_row_id)?;
            output_batches.clear();
            output_row_count = 0;
        }
    }

    if !output_batches.is_empty() {
        flush_arrow(&mut writer, &output_batches, &input_schema, &mut next_row_id)?;
    }

    writer.close().map_err(|e| e.to_string())?;

    Ok(())
}

/// Flush batches to parquet with row_id column
fn flush_arrow(
    writer: &mut ArrowWriter<File>,
    batches: &[RecordBatch],
    input_schema: &Arc<Schema>,
    row_id: &mut i64,
) -> Result<(), String> {
    if batches.is_empty() {
        return Ok(());
    }

    // Concatenate all batches
    let combined = arrow::compute::concat_batches(input_schema, batches)
        .map_err(|e| e.to_string())?;

    let n = combined.num_rows();

    // Create row_id column
    let row_ids: Int64Array = (*row_id..*row_id + n as i64).collect();

    // Add row_id column to batch
    let mut columns: Vec<Arc<dyn Array>> = combined.columns().to_vec();
    columns.push(Arc::new(row_ids));

    let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, false)));
    let output_schema = Arc::new(Schema::new(fields));

    let output_batch = RecordBatch::try_new(output_schema, columns)
        .map_err(|e| e.to_string())?;

    writer.write(&output_batch).map_err(|e| e.to_string())?;

    *row_id += n as i64;
    Ok(())
}

fn convert_java_list_to_vec(env: &mut JNIEnv, list: JObject) -> Result<Vec<String>, Box<dyn Error>> {
    let size = env.call_method(&list, "size", "()I", &[])?.i()? as usize;
    let mut out = Vec::with_capacity(size);

    for i in 0..size {
        let obj = env.call_method(&list, "get", "(I)Ljava/lang/Object;", &[(i as i32).into()])?.l()?;
        let s = env.call_method(&obj, "toString", "()Ljava/lang/String;", &[])?.l()?;
        out.push(env.get_string(&s.into())?.into());
    }
    Ok(out)
}
