/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, RecordBatch,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{
    DataType as ArrowDataType, Date32Type, Date64Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Field as ArrowField, Int32Type, Int64Type, Schema as ArrowSchema,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt32Type, UInt64Type,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::{ArrowRowGroupWriterFactory, compute_leaves};
use parquet::basic::{Compression, Repetition};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;

use rayon::prelude::*;
use rayon::ThreadPool;

const ROW_ID_COLUMN_NAME: &str = "___row_id";
const BATCH_SIZE: usize = 50_000;
const OUTPUT_FLUSH_ROWS: usize = 100_000;
const RAYON_NUM_THREADS: usize = 4;

// =============================================================================
// Process-wide shared Rayon thread pool — lazily initialized, lives forever
// =============================================================================

static MERGE_POOL: OnceLock<ThreadPool> = OnceLock::new();

fn get_merge_pool() -> &'static ThreadPool {
    MERGE_POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(RAYON_NUM_THREADS)
            .thread_name(|idx| format!("parquet-merge-{}", idx))
            .build()
            .expect("Failed to build parquet-merge Rayon thread pool")
    })
}

// =============================================================================
// Timing accumulator
// =============================================================================

struct MergeTimings {
    file_open_and_init: Duration,
    parquet_read: Duration,
    take_slice: Duration,
    concat_batches: Duration,
    append_row_id: Duration,
    parallel_encode: Duration,
    rg_append_close: Duration,
    writer_close: Duration,
    heap_ops: Duration,
    sort_value_lookups: Duration,

    file_open_count: usize,
    parquet_read_count: usize,
    take_slice_count: usize,
    concat_count: usize,
    append_row_id_count: usize,
    encode_count: usize,
    sort_lookup_count: usize,
    flush_count: usize,

    total_rows_read: usize,
    total_rows_sliced: usize,
    total_rows_flushed: usize,
    total_rows_written: usize,
}

impl MergeTimings {
    fn new() -> Self {
        Self {
            file_open_and_init: Duration::ZERO,
            parquet_read: Duration::ZERO,
            take_slice: Duration::ZERO,
            concat_batches: Duration::ZERO,
            append_row_id: Duration::ZERO,
            parallel_encode: Duration::ZERO,
            rg_append_close: Duration::ZERO,
            writer_close: Duration::ZERO,
            heap_ops: Duration::ZERO,
            sort_value_lookups: Duration::ZERO,

            file_open_count: 0,
            parquet_read_count: 0,
            take_slice_count: 0,
            concat_count: 0,
            append_row_id_count: 0,
            encode_count: 0,
            sort_lookup_count: 0,
            flush_count: 0,

            total_rows_read: 0,
            total_rows_sliced: 0,
            total_rows_flushed: 0,
            total_rows_written: 0,
        }
    }

    fn print_summary(&self, total_elapsed: Duration) {
        let t = total_elapsed.as_secs_f64();

        println!();
        println!("======================================================================");
        println!("MERGE TIMING BREAKDOWN");
        println!("======================================================================");
        println!("Total wall time:          {:>10.3}s", t);
        println!();

        println!("--- FILE OPEN + INIT ---");
        println!(
            "  file_open_and_init:     {:>10.3}s  ({} files)",
            self.file_open_and_init.as_secs_f64(),
            self.file_open_count
        );
        println!();

        println!("--- READ (arrow-rs ParquetRecordBatchReader::next) ---");
        println!(
            "  parquet_read:           {:>10.3}s  ({} calls, {} rows)",
            self.parquet_read.as_secs_f64(),
            self.parquet_read_count,
            self.total_rows_read
        );
        if self.total_rows_read > 0 && self.parquet_read.as_secs_f64() > 0.0 {
            println!(
                "  read throughput:        {:>10.0} rows/sec",
                self.total_rows_read as f64 / self.parquet_read.as_secs_f64()
            );
        }
        println!();

        println!("--- SLICE (RecordBatch::slice — zero-copy offset) ---");
        println!(
            "  take_slice:             {:>10.3}s  ({} calls, {} rows)",
            self.take_slice.as_secs_f64(),
            self.take_slice_count,
            self.total_rows_sliced
        );
        println!();

        println!("--- FLUSH (concat + row_id + parallel encode + rg close) ---");
        println!(
            "  concat_batches:         {:>10.3}s  ({} concats, {} rows)",
            self.concat_batches.as_secs_f64(),
            self.concat_count,
            self.total_rows_flushed
        );
        println!(
            "  append_row_id:          {:>10.3}s  ({} calls)",
            self.append_row_id.as_secs_f64(),
            self.append_row_id_count
        );
        println!(
            "  parallel_encode:        {:>10.3}s  ({} row groups, {} rows)",
            self.parallel_encode.as_secs_f64(),
            self.encode_count,
            self.total_rows_written
        );
        println!(
            "  rg_append_close:        {:>10.3}s",
            self.rg_append_close.as_secs_f64()
        );
        if self.total_rows_written > 0 && self.parallel_encode.as_secs_f64() > 0.0 {
            println!(
                "  encode throughput:      {:>10.0} rows/sec",
                self.total_rows_written as f64 / self.parallel_encode.as_secs_f64()
            );
        }
        let flush_total =
            self.concat_batches + self.append_row_id + self.parallel_encode + self.rg_append_close;
        println!(
            "  flush total:            {:>10.3}s  ({} flushes)",
            flush_total.as_secs_f64(),
            self.flush_count
        );
        println!();

        println!("--- CLOSE (SerializedFileWriter::close — flush footer) ---");
        println!(
            "  writer_close:           {:>10.3}s",
            self.writer_close.as_secs_f64()
        );
        println!();

        println!("--- MERGE LOGIC ---");
        println!(
            "  heap_ops:               {:>10.3}s",
            self.heap_ops.as_secs_f64()
        );
        println!(
            "  sort_value_lookups:     {:>10.3}s  ({} lookups)",
            self.sort_value_lookups.as_secs_f64(),
            self.sort_lookup_count
        );
        println!();

        let accounted = self.file_open_and_init
            + self.parquet_read
            + self.take_slice
            + self.concat_batches
            + self.append_row_id
            + self.parallel_encode
            + self.rg_append_close
            + self.writer_close
            + self.heap_ops
            + self.sort_value_lookups;

        let unaccounted = if total_elapsed > accounted {
            total_elapsed - accounted
        } else {
            Duration::ZERO
        };

        println!("--- SUMMARY ---");
        println!(
            "  accounted:              {:>10.3}s  ({:.1}%)",
            accounted.as_secs_f64(),
            accounted.as_secs_f64() / t * 100.0
        );
        println!(
            "  unaccounted:            {:>10.3}s  ({:.1}%)",
            unaccounted.as_secs_f64(),
            unaccounted.as_secs_f64() / t * 100.0
        );
        println!();

        println!("--- PERCENTAGE BREAKDOWN ---");
        println!("  file_open_and_init:     {:>6.1}%", self.file_open_and_init.as_secs_f64() / t * 100.0);
        println!("  parquet_read:           {:>6.1}%", self.parquet_read.as_secs_f64() / t * 100.0);
        println!("  take_slice:             {:>6.1}%", self.take_slice.as_secs_f64() / t * 100.0);
        println!("  concat_batches:         {:>6.1}%", self.concat_batches.as_secs_f64() / t * 100.0);
        println!("  append_row_id:          {:>6.1}%", self.append_row_id.as_secs_f64() / t * 100.0);
        println!("  parallel_encode:        {:>6.1}%", self.parallel_encode.as_secs_f64() / t * 100.0);
        println!("  rg_append_close:        {:>6.1}%", self.rg_append_close.as_secs_f64() / t * 100.0);
        println!("  writer_close:           {:>6.1}%", self.writer_close.as_secs_f64() / t * 100.0);
        println!("  heap_ops:               {:>6.1}%", self.heap_ops.as_secs_f64() / t * 100.0);
        println!("  sort_lookups:           {:>6.1}%", self.sort_value_lookups.as_secs_f64() / t * 100.0);
        println!("  unaccounted:            {:>6.1}%", unaccounted.as_secs_f64() / t * 100.0);
        println!("======================================================================");
        println!();
    }
}

// =============================================================================
// MergeError
// =============================================================================

pub type MergeResult<T> = Result<T, MergeError>;

#[derive(Debug)]
pub enum MergeError {
    Arrow(arrow::error::ArrowError),
    Parquet(parquet::errors::ParquetError),
    Io(std::io::Error),
    Logic(String),
}

impl std::fmt::Display for MergeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeError::Arrow(e) => write!(f, "Arrow error: {e}"),
            MergeError::Parquet(e) => write!(f, "Parquet error: {e}"),
            MergeError::Io(e) => write!(f, "IO error: {e}"),
            MergeError::Logic(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for MergeError {}

impl From<arrow::error::ArrowError> for MergeError {
    fn from(e: arrow::error::ArrowError) -> Self { MergeError::Arrow(e) }
}
impl From<parquet::errors::ParquetError> for MergeError {
    fn from(e: parquet::errors::ParquetError) -> Self { MergeError::Parquet(e) }
}
impl From<std::io::Error> for MergeError {
    fn from(e: std::io::Error) -> Self { MergeError::Io(e) }
}

// =============================================================================
// Build output parquet schema from input file + add ___row_id
// =============================================================================

fn build_parquet_root_schema(first_input_path: &str) -> MergeResult<Arc<Type>> {
    let input_file = File::open(first_input_path)?;
    let reader = SerializedFileReader::new(input_file)?;
    let input_schema_descr = reader.metadata().file_metadata().schema_descr_ptr();
    let input_root = input_schema_descr.root_schema();

    // Collect fields, filtering out any existing ___row_id
    let mut parquet_fields: Vec<Arc<Type>> = Vec::new();
    for field in input_root.get_fields() {
        if field.name() != ROW_ID_COLUMN_NAME {
            parquet_fields.push(Arc::new(field.as_ref().clone()));
        }
    }

    // Append ___row_id as INT64 REQUIRED
    let row_id_type =
        Type::primitive_type_builder(ROW_ID_COLUMN_NAME, parquet::basic::Type::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()?;
    parquet_fields.push(Arc::new(row_id_type));

    let parquet_root = Type::group_type_builder("schema")
        .with_fields(parquet_fields)
        .build()?;

    Ok(Arc::new(parquet_root))
}

// =============================================================================
// FileCursor — streams RecordBatches from a parquet file using arrow-rs
// =============================================================================

struct FileCursor {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    current_batch: Option<RecordBatch>,
    row_idx: usize,
    file_id: usize,
    sort_col_idx: usize,
    sort_col_type: ArrowDataType,
}

impl FileCursor {
    fn new(
        path: &str,
        file_id: usize,
        sort_column: &str,
        batch_size: usize,
        timings: &mut MergeTimings,
    ) -> MergeResult<Option<Self>> {
        let t0 = Instant::now();

        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();

        let sort_col_type = schema
            .fields()
            .iter()
            .find(|f| f.name() == sort_column)
            .map(|f| f.data_type().clone())
            .ok_or_else(|| {
                let cols: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
                MergeError::Logic(format!(
                    "Sort column '{}' not found in file {} (cursor {}). Available: {:?}",
                    sort_column, path, file_id, cols
                ))
            })?;

        let parquet_schema = builder.parquet_schema().clone();
        let projection_indices: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() != ROW_ID_COLUMN_NAME)
            .map(|(i, _)| i)
            .collect();

        let projection =
            parquet::arrow::ProjectionMask::roots(&parquet_schema, projection_indices);

        let mut reader = builder
            .with_batch_size(batch_size)
            .with_projection(projection)
            .build()?;

        timings.file_open_and_init += t0.elapsed();
        timings.file_open_count += 1;

        // Read first batch
        let t1 = Instant::now();
        let batch = match reader.next() {
            Some(Ok(b)) if b.num_rows() > 0 => {
                timings.parquet_read += t1.elapsed();
                timings.parquet_read_count += 1;
                timings.total_rows_read += b.num_rows();
                b
            }
            Some(Err(e)) => return Err(e.into()),
            _ => {
                timings.parquet_read += t1.elapsed();
                timings.parquet_read_count += 1;
                return Ok(None);
            }
        };

        let sort_col_idx = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == sort_column)
            .ok_or_else(|| {
                MergeError::Logic(format!(
                    "Sort column '{}' disappeared after projection in file {}",
                    sort_column, path
                ))
            })?;

        Ok(Some(Self {
            reader,
            current_batch: Some(batch),
            row_idx: 0,
            file_id,
            sort_col_idx,
            sort_col_type,
        }))
    }

    fn load_next_batch(&mut self, timings: &mut MergeTimings) -> MergeResult<bool> {
        self.current_batch = None;
        let t0 = Instant::now();
        match self.reader.next() {
            Some(Ok(batch)) if batch.num_rows() > 0 => {
                timings.parquet_read += t0.elapsed();
                timings.parquet_read_count += 1;
                timings.total_rows_read += batch.num_rows();
                self.current_batch = Some(batch);
                self.row_idx = 0;
                Ok(true)
            }
            Some(Err(e)) => Err(e.into()),
            _ => {
                timings.parquet_read += t0.elapsed();
                timings.parquet_read_count += 1;
                Ok(false)
            }
        }
    }

    #[inline]
    fn current_sort_value_raw(&self) -> MergeResult<i64> {
        let batch = self.current_batch.as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        get_sort_value(batch, self.row_idx, self.sort_col_idx, &self.sort_col_type)
    }

    #[inline]
    fn current_sort_value(&self, timings: &mut MergeTimings) -> MergeResult<i64> {
        let batch = self.current_batch.as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        let t0 = Instant::now();
        let val = get_sort_value(batch, self.row_idx, self.sort_col_idx, &self.sort_col_type)?;
        timings.sort_value_lookups += t0.elapsed();
        timings.sort_lookup_count += 1;
        Ok(val)
    }

    #[inline]
    fn last_sort_value(&self, timings: &mut MergeTimings) -> MergeResult<i64> {
        let batch = self.current_batch.as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        let t0 = Instant::now();
        let val = get_sort_value(
            batch,
            batch.num_rows() - 1,
            self.sort_col_idx,
            &self.sort_col_type,
        )?;
        timings.sort_value_lookups += t0.elapsed();
        timings.sort_lookup_count += 1;
        Ok(val)
    }

    #[inline]
    fn batch_height(&self) -> usize {
        self.current_batch.as_ref().map_or(0, |b| b.num_rows())
    }

    #[inline]
    fn take_slice(
        &self,
        start: usize,
        len: usize,
        timings: &mut MergeTimings,
    ) -> RecordBatch {
        let t0 = Instant::now();
        let slice = self.current_batch.as_ref().unwrap().slice(start, len);
        timings.take_slice += t0.elapsed();
        timings.take_slice_count += 1;
        timings.total_rows_sliced += len;
        slice
    }

    fn advance(&mut self, timings: &mut MergeTimings) -> MergeResult<bool> {
        if self.current_batch.is_none() {
            return Ok(false);
        }
        self.row_idx += 1;
        if self.row_idx >= self.current_batch.as_ref().unwrap().num_rows() {
            self.current_batch = None;
            return self.load_next_batch(timings);
        }
        Ok(true)
    }

    fn advance_past_batch(&mut self, timings: &mut MergeTimings) -> MergeResult<bool> {
        self.current_batch = None;
        self.load_next_batch(timings)
    }
}

// =============================================================================
// HeapItem for min-heap (ascending sort)
// =============================================================================

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

// =============================================================================
// Sort value extraction — pure arrow-rs
// =============================================================================

#[inline]
fn get_sort_value(
    batch: &RecordBatch,
    row: usize,
    col_idx: usize,
    dtype: &ArrowDataType,
) -> MergeResult<i64> {
    let col = batch.column(col_idx);
    let val = match dtype {
        ArrowDataType::Int64 => col.as_primitive::<Int64Type>().value(row),
        ArrowDataType::Int32 => col.as_primitive::<Int32Type>().value(row) as i64,
        ArrowDataType::UInt64 => col.as_primitive::<UInt64Type>().value(row) as i64,
        ArrowDataType::UInt32 => col.as_primitive::<UInt32Type>().value(row) as i64,
        ArrowDataType::Date32 => col.as_primitive::<Date32Type>().value(row) as i64,
        ArrowDataType::Date64 => col.as_primitive::<Date64Type>().value(row),
        ArrowDataType::Timestamp(unit, _) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                col.as_primitive::<TimestampSecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                col.as_primitive::<TimestampMillisecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                col.as_primitive::<TimestampMicrosecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                col.as_primitive::<TimestampNanosecondType>().value(row)
            }
        },
        ArrowDataType::Duration(unit) => match unit {
            arrow::datatypes::TimeUnit::Second => {
                col.as_primitive::<DurationSecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Millisecond => {
                col.as_primitive::<DurationMillisecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Microsecond => {
                col.as_primitive::<DurationMicrosecondType>().value(row)
            }
            arrow::datatypes::TimeUnit::Nanosecond => {
                col.as_primitive::<DurationNanosecondType>().value(row)
            }
        },
        other => {
            return Err(MergeError::Logic(format!(
                "Unsupported sort column type: {:?}",
                other
            )));
        }
    };
    Ok(val)
}

// =============================================================================
// Append ___row_id column to a RecordBatch
// =============================================================================

fn append_row_id(
    batch: &RecordBatch,
    start_id: i64,
    output_schema: &Arc<ArrowSchema>,
    timings: &mut MergeTimings,
) -> MergeResult<RecordBatch> {
    let t0 = Instant::now();
    let n = batch.num_rows() as i64;
    let row_ids = Int64Array::from_iter_values(start_id..start_id + n);
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(row_ids));
    let result = RecordBatch::try_new(output_schema.clone(), columns)?;
    timings.append_row_id += t0.elapsed();
    timings.append_row_id_count += 1;
    Ok(result)
}

// =============================================================================
// JNI Entry Point
// =============================================================================

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesInRust(
    mut env: JNIEnv,
    _class: JClass,
    input_files: JObject,
    output_file: JString,
    sort_column: JString,
    is_reverse: jint,
) -> jint {
    let input_files_vec = match convert_java_list_to_vec(&mut env, input_files) {
        Ok(v) => v,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };
    let output_path: String = match env.get_string(&output_file) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };
    let sort_col: String = match env.get_string(&sort_column) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };
    let _reverse = is_reverse != 0;

    match merge_streaming(&input_files_vec, &output_path, &sort_col) {
        Ok(_) => 0,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", format!("{:?}", e));
            -1
        }
    }
}

// =============================================================================
// STREAMING K-WAY MERGE with parallel column encoding via rayon
// =============================================================================

pub fn merge_streaming(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
) -> MergeResult<()> {
    merge_streaming_with_config(
        input_files,
        output_path,
        sort_column,
        BATCH_SIZE,
        OUTPUT_FLUSH_ROWS,
    )
}

pub fn merge_streaming_with_config(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
    batch_size: usize,
    output_flush_rows: usize,
) -> MergeResult<()> {
    if input_files.is_empty() {
        return Ok(());
    }

    let total_start = Instant::now();
    let mut timings = MergeTimings::new();

    // Eagerly initialise the shared pool (first caller pays init cost)
    let pool = get_merge_pool();

    println!(
        "Starting streaming merge of {} files (shared rayon pool: {} threads)",
        input_files.len(),
        pool.current_num_threads()
    );

    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    // ── Initialise cursors ──────────────────────────────────────────────
    let mut cursors: Vec<Option<FileCursor>> = Vec::with_capacity(input_files.len());

    for (file_id, path) in input_files.iter().enumerate() {
        println!("Initializing cursor for file {}: {}", file_id, path);
        match FileCursor::new(path, file_id, sort_column, batch_size, &mut timings)? {
            Some(cursor) => cursors.push(Some(cursor)),
            None => {
                cursors.push(None);
                eprintln!("Skipping empty file: {}", path);
            }
        }
    }

    let active_count = cursors.iter().filter(|c| c.is_some()).count();
    if active_count == 0 {
        println!("All files were empty");
        timings.print_summary(total_start.elapsed());
        return Ok(());
    }

    // ── Build schemas ───────────────────────────────────────────────────
    let first_active_idx = cursors.iter().position(|c| c.is_some()).unwrap();
    let first_batch = cursors[first_active_idx]
        .as_ref()
        .unwrap()
        .current_batch
        .as_ref()
        .unwrap();
    let input_schema = first_batch.schema();

    // Data schema (without ___row_id) — used for concat_batches
    let data_fields: Vec<ArrowField> = input_schema
        .fields()
        .iter()
        .filter(|f| f.name() != ROW_ID_COLUMN_NAME)
        .map(|f| f.as_ref().clone())
        .collect();
    let data_schema = Arc::new(ArrowSchema::new(data_fields.clone()));

    // Output arrow schema = data + ___row_id (for ArrowRowGroupWriterFactory)
    let mut output_fields = data_fields;
    output_fields.push(ArrowField::new(
        ROW_ID_COLUMN_NAME,
        ArrowDataType::Int64,
        false,
    ));
    let output_schema = Arc::new(ArrowSchema::new(output_fields));

    // Parquet schema read from input file + ___row_id appended
    let parquet_root = build_parquet_root_schema(&input_files[first_active_idx])?;

    println!(
        "Initialized {} active cursors, starting merge",
        active_count
    );

    // ── Open SerializedFileWriter (low-level for parallel encoding) ─────
    let output_file = File::create(output_path)?;

    let writer_props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_row_count(Some(1_000_000))
            .set_data_page_size_limit(1024 * 1024)
            .build(),
    );

    let mut writer =
        SerializedFileWriter::new(output_file, parquet_root, writer_props.clone())?;

    let mut row_group_index: usize = 0;

    // ── Seed min-heap ───────────────────────────────────────────────────
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(active_count);
    for cursor_opt in &cursors {
        if let Some(cursor) = cursor_opt {
            let sv = cursor.current_sort_value_raw()?;
            let t0 = Instant::now();
            heap.push(HeapItem {
                sort_value: sv,
                file_id: cursor.file_id,
            });
            timings.heap_ops += t0.elapsed();
        }
    }

    // ── Output buffer ───────────────────────────────────────────────────
    let mut output_chunks: Vec<RecordBatch> = Vec::new();
    let mut output_row_count: usize = 0;
    let mut next_row_id: i64 = 0;

    // ── Flush: concat → row_id → parallel encode → write row group ──────
    macro_rules! flush {
        () => {
            if !output_chunks.is_empty() {
                // 1. Concat all buffered slices into one contiguous RecordBatch
                let t_concat = Instant::now();
                let merged = concat_batches(&data_schema, &output_chunks)?;
                output_chunks.clear();
                timings.concat_batches += t_concat.elapsed();
                timings.concat_count += 1;
                timings.total_rows_flushed += merged.num_rows();

                let n = merged.num_rows();

                // 2. Append ___row_id column
                let with_id =
                    append_row_id(&merged, next_row_id, &output_schema, &mut timings)?;
                drop(merged);

                // 3. Create per-column writers for this row group
                let col_writers = {
                    let factory = ArrowRowGroupWriterFactory::new(
                        &writer,
                        output_schema.clone(),
                    );
                    factory.create_column_writers(row_group_index)?
                };

                // 4. Compute leaf columns and pair with writers
                let mut leaves_and_writers = Vec::new();
                {
                    let mut writer_iter = col_writers.into_iter();
                    for (arr, field) in with_id
                        .columns()
                        .iter()
                        .zip(output_schema.fields())
                    {
                        for leaf in compute_leaves(field, arr)? {
                            let col_writer = writer_iter.next().ok_or_else(|| {
                                MergeError::Logic(
                                    "Fewer column writers than leaf columns".into(),
                                )
                            })?;
                            leaves_and_writers.push((leaf, col_writer));
                        }
                    }
                }

                // 5. Encode ALL columns in parallel on the shared pool
                let t_encode = Instant::now();
                let chunk_results: Vec<
                    Result<
                        parquet::arrow::arrow_writer::ArrowColumnChunk,
                        parquet::errors::ParquetError,
                    >,
                > = get_merge_pool().install(|| {
                    leaves_and_writers
                        .into_par_iter()
                        .map(|(leaf, mut col_writer)| {
                            col_writer.write(&leaf)?;
                            col_writer.close()
                        })
                        .collect()
                });
                timings.parallel_encode += t_encode.elapsed();
                timings.encode_count += 1;

                // 6. Open row group, append encoded chunks, close
                let t_rg = Instant::now();
                let mut rg_writer = writer.next_row_group()?;

                for chunk_result in chunk_results {
                    let chunk = chunk_result?;
                    chunk.append_to_row_group(&mut rg_writer)?;
                }
                rg_writer.close()?;
                timings.rg_append_close += t_rg.elapsed();

                row_group_index += 1;
                next_row_id += n as i64;
                timings.flush_count += 1;
                timings.total_rows_written += n;
                output_row_count = 0;
            }
        };
    }

    // =====================================================================
    // K-way merge loop — three-tier cascade
    // =====================================================================
    while let Some(item) = {
        let t0 = Instant::now();
        let item = heap.pop();
        timings.heap_ops += t0.elapsed();
        item
    } {
        let file_id = item.file_id;

        // TIER 1: only one cursor left → dump everything
        if heap.is_empty() {
            let cursor = cursors[file_id].as_mut().unwrap();
            loop {
                let remaining = cursor.batch_height() - cursor.row_idx;
                if remaining > 0 {
                    let slice =
                        cursor.take_slice(cursor.row_idx, remaining, &mut timings);
                    output_row_count += remaining;
                    output_chunks.push(slice);

                    if output_row_count >= output_flush_rows {
                        flush!();
                    }
                }
                if !cursor.advance_past_batch(&mut timings)? {
                    break;
                }
            }
            break;
        }

        // TIER 2 & 3: multiple cursors active
        let cursor = cursors[file_id].as_mut().unwrap();

        loop {
            let t0 = Instant::now();
            let heap_top = heap.peek().unwrap().sort_value;
            timings.heap_ops += t0.elapsed();

            // TIER 2: entire remaining batch fits
            let last_val = cursor.last_sort_value(&mut timings)?;
            if last_val <= heap_top {
                let remaining = cursor.batch_height() - cursor.row_idx;
                let slice =
                    cursor.take_slice(cursor.row_idx, remaining, &mut timings);
                output_chunks.push(slice);
                output_row_count += remaining;

                if output_row_count >= output_flush_rows {
                    flush!();
                }

                if !cursor.advance_past_batch(&mut timings)? {
                    break;
                }
                continue;
            }

            // TIER 3: binary search for cut point
            let run_start = cursor.row_idx;
            let batch_h = cursor.batch_height();
            let batch = cursor.current_batch.as_ref().unwrap();

            let mut lo = run_start;
            let mut hi = batch_h - 1;

            while lo + 1 < hi {
                let mid = lo + (hi - lo) / 2;
                let t0 = Instant::now();
                let mid_val = get_sort_value(
                    batch,
                    mid,
                    cursor.sort_col_idx,
                    &cursor.sort_col_type,
                )?;
                timings.sort_value_lookups += t0.elapsed();
                timings.sort_lookup_count += 1;

                if mid_val <= heap_top {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            let run_end = lo;

            let run_len = run_end - run_start + 1;
            if run_len > 0 {
                let slice = cursor.take_slice(run_start, run_len, &mut timings);
                output_chunks.push(slice);
                output_row_count += run_len;

                if output_row_count >= output_flush_rows {
                    flush!();
                }
            }

            cursor.row_idx = run_end;
            if !cursor.advance(&mut timings)? {
                break;
            }

            let next_val = cursor.current_sort_value(&mut timings)?;
            if next_val > heap_top {
                let t0 = Instant::now();
                heap.push(HeapItem {
                    sort_value: next_val,
                    file_id,
                });
                timings.heap_ops += t0.elapsed();
                break;
            }
        }
    }

    // Final flush
    flush!();

    let t_close = Instant::now();
    let _metadata = writer.close()?;
    timings.writer_close = t_close.elapsed();

    let total_elapsed = total_start.elapsed();
    timings.print_summary(total_elapsed);

    println!(
        "Merge complete: {} total rows written",
        timings.total_rows_written
    );
    Ok(())
}

// =============================================================================
// JNI helper
// =============================================================================

fn convert_java_list_to_vec(
    env: &mut JNIEnv,
    list: JObject,
) -> Result<Vec<String>, Box<dyn Error>> {
    let size = env.call_method(&list, "size", "()I", &[])?.i()? as usize;
    let mut result = Vec::with_capacity(size);
    for i in 0..size {
        env.push_local_frame(4)?;
        let obj = env
            .call_method(
                &list,
                "get",
                "(I)Ljava/lang/Object;",
                &[(i as i32).into()],
            )?
            .l()?;
        let jstring = env
            .call_method(&obj, "toString", "()Ljava/lang/String;", &[])?
            .l()?;
        let rust_string: String = env.get_string(&jstring.into())?.into();
        result.push(rust_string);
        unsafe {
            env.pop_local_frame(&JObject::null())?;
        }
    }
    Ok(result)
}
