//! # Streaming K-Way Merge for Sorted Parquet Files
//!
//! This module implements a high-performance streaming k-way merge for sorted
//! Parquet files. It is designed for the OpenSearch Composite Engine merge path
//! where multiple individually-sorted segment files must be merged into a single
//! globally-sorted output file.
//!
//! ## Merge Algorithm
//!
//! The k-way merge uses a three-tier cascade for efficiency:
//!
//! 1. **Tier 1 (single cursor)**: When only one input cursor remains, its
//!    batches are dumped directly to the output — no heap operations needed.
//! 2. **Tier 2 (whole batch)**: If the last sort value in a cursor's current
//!    batch is ≤ the heap's next-smallest value, the entire remaining batch
//!    is emitted as a zero-copy slice.
//! 3. **Tier 3 (binary search)**: Otherwise, a binary search within the batch
//!    finds the exact boundary where the cursor's values exceed the heap top,
//!    and only the safe prefix is emitted.
//!
//! ## Row ID Rewriting
//!
//! The output file always contains a `___row_id` column with sequential values
//! `0..N`, ensuring globally unique row identifiers in the merged output.

use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{ArrayRef, AsArray, Int64Array, RecordBatch};
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
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;

use rayon::prelude::*;
use rayon::ThreadPool;

use tokio::runtime::Runtime;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::rate_limited_writer::RateLimitedWriter;
use crate::{log_debug, log_error, log_info};

// =============================================================================
// Constants
// =============================================================================

/// Reserved column name for the synthetic row identifier added during merge.
const ROW_ID_COLUMN_NAME: &str = "___row_id";

/// Number of rows to request per Parquet read batch. Larger batches reduce
/// per-batch overhead but increase peak memory. 100K rows is a good balance
/// for wide schemas with many columns.
const BATCH_SIZE: usize = 100_000;

/// Approximate number of rows to buffer before flushing a row group to the
/// output file. This controls output row group size and peak memory usage.
const OUTPUT_FLUSH_ROWS: usize = 1_000_000;

/// Number of threads in the shared Rayon pool used for parallel column
/// encoding and batch prefetch. Kept modest to avoid contention with other
/// engine threads.
const RAYON_NUM_THREADS: usize = 4;

/// Disk write rate limit in MB/s. Throttles merge IO to avoid saturating
/// shared storage (e.g. EBS, NFS) and impacting search latency.
const RATE_LIMIT_MB_PER_SEC: f64 = 20.0;

/// Maximum number of rows per output row group. Aligns with Parquet best
/// practices for query performance.
const MAX_ROW_GROUP_ROWS: usize = 1_000_000;

/// Target data page size in bytes. Smaller pages improve predicate pushdown
/// granularity at the cost of slightly more metadata overhead.
const DATA_PAGE_SIZE_LIMIT: usize = 1024 * 1024;

/// Bounded channel capacity between the merge loop and the IO task.
/// A buffer of 2 allows the merge loop to prepare one row group ahead
/// while the IO task is still writing the previous one.
const IO_CHANNEL_BUFFER: usize = 2;

// =============================================================================
// Process-wide shared Rayon thread pool
// =============================================================================

/// Lazily initialized, process-wide Rayon thread pool for merge operations.
/// Shared across all concurrent merges to bound total CPU usage.
static MERGE_POOL: OnceLock<ThreadPool> = OnceLock::new();

/// Returns a reference to the shared merge thread pool, initializing it on
/// first access.
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
// Process-wide shared Tokio runtime for async IO
// =============================================================================

/// Lazily initialized, process-wide Tokio runtime dedicated to merge IO.
/// Uses a small number of worker threads since IO tasks are primarily
/// dispatched to `spawn_blocking`.
static IO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Returns a reference to the shared IO runtime, initializing it on first access.
fn get_io_runtime() -> &'static Runtime {
    IO_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("parquet-io")
            .enable_all()
            .build()
            .expect("Failed to build tokio IO runtime")
    })
}

// =============================================================================
// IO task protocol
// =============================================================================

/// Commands sent from the merge loop to the background IO task.
enum IoCommand {
    /// Fully-encoded column chunks for one row group. The IO task appends
    /// them to the output file and closes the row group.
    WriteRowGroup(Vec<parquet::arrow::arrow_writer::ArrowColumnChunk>),

    /// Signals the IO task to flush the Parquet footer, close the file, and
    /// send back the final file metadata via the oneshot channel.
    Close(oneshot::Sender<MergeResult<ParquetMetaData>>),
}

/// Drains remaining commands from the channel after an IO error, ensuring
/// that senders (the merge loop) don't block forever on a dead channel.
async fn drain_on_error(rx: &mut tokio_mpsc::Receiver<IoCommand>, msg: &str) {
    while let Some(cmd) = rx.recv().await {
        if let IoCommand::Close(reply) = cmd {
            let _ = reply.send(Err(MergeError::Logic(
                format!("Prior IO write failed: {msg}"),
            )));
        }
    }
}

/// Spawns the background IO task on the shared Tokio runtime.
///
/// The IO task owns the `SerializedFileWriter` and receives encoded row groups
/// over a bounded channel. Each disk write is dispatched to `spawn_blocking`
/// but is **not** awaited immediately — this allows the merge loop to prepare
/// the next row group while the current one is still being flushed to disk,
/// providing genuine pipeline parallelism.
///
/// The in-flight write handle is only awaited when the next command arrives
/// or when the task is shutting down.
fn spawn_io_task(
    writer: SerializedFileWriter<RateLimitedWriter<File>>,
) -> tokio_mpsc::Sender<IoCommand> {
    let (tx, mut rx) = tokio_mpsc::channel::<IoCommand>(IO_CHANNEL_BUFFER);

    get_io_runtime().spawn(async move {
        // The writer is moved between the async task and spawn_blocking via Option.
        let mut writer: Option<SerializedFileWriter<RateLimitedWriter<File>>> = Some(writer);

        // Handle to the currently in-flight spawn_blocking write, if any.
        let mut in_flight: Option<
            JoinHandle<MergeResult<SerializedFileWriter<RateLimitedWriter<File>>>>,
        > = None;

        while let Some(cmd) = rx.recv().await {
            match cmd {
                IoCommand::WriteRowGroup(chunks) => {
                    // Await the previous write before starting a new one.
                    if let Some(handle) = in_flight.take() {
                        match handle.await {
                            Ok(Ok(w)) => writer = Some(w),
                            Ok(Err(e)) => {
                                let msg = format!("{e}");
                                log_error!("[RUST] IO write error during merge: {}", e);
                                drain_on_error(&mut rx, &msg).await;
                                return;
                            }
                            Err(e) => {
                                let msg = format!("{e}");
                                log_error!("[RUST] IO spawn_blocking panicked during merge: {}", e);
                                drain_on_error(&mut rx, &msg).await;
                                return;
                            }
                        }
                    }

                    // Dispatch the write to a blocking thread — don't await yet.
                    let w = writer.take().unwrap();
                    in_flight = Some(tokio::task::spawn_blocking(move || {
                        let mut w = w;
                        let mut rg_writer = w.next_row_group()?;
                        for chunk in chunks {
                            chunk.append_to_row_group(&mut rg_writer)?;
                        }
                        rg_writer.close()?;
                        Ok(w)
                    }));
                    // Loop back immediately — merge loop can send the next row group.
                }

                IoCommand::Close(reply) => {
                    // Drain any in-flight write before closing.
                    if let Some(handle) = in_flight.take() {
                        match handle.await {
                            Ok(Ok(w)) => writer = Some(w),
                            Ok(Err(e)) => {
                                let _ = reply.send(Err(e));
                                return;
                            }
                            Err(e) => {
                                let _ = reply.send(Err(MergeError::Logic(
                                    format!("IO panic during final write: {e}"),
                                )));
                                return;
                            }
                        }
                    }

                    // Write the Parquet footer and close the file.
                    let w = writer.take().unwrap();
                    let result = tokio::task::spawn_blocking(move || {
                        let mut w = w;
                        w.close().map_err(MergeError::from)
                    })
                        .await;

                    let _ = match result {
                        Ok(r) => reply.send(r),
                        Err(e) => reply.send(Err(MergeError::Logic(
                            format!("Close panicked: {e}"),
                        ))),
                    };
                    return;
                }
            }
        }
    });

    tx
}

// =============================================================================
// Error types
// =============================================================================

/// Result type alias for merge operations.
pub type MergeResult<T> = Result<T, MergeError>;

/// Unified error type for all merge failures.
#[derive(Debug)]
pub enum MergeError {
    /// Error from the Arrow compute or array layer.
    Arrow(arrow::error::ArrowError),
    /// Error from the Parquet reader or writer.
    Parquet(parquet::errors::ParquetError),
    /// Filesystem or network IO error.
    Io(std::io::Error),
    /// Logic or invariant violation within the merge algorithm.
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
    fn from(e: arrow::error::ArrowError) -> Self {
        MergeError::Arrow(e)
    }
}

impl From<parquet::errors::ParquetError> for MergeError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        MergeError::Parquet(e)
    }
}

impl From<std::io::Error> for MergeError {
    fn from(e: std::io::Error) -> Self {
        MergeError::Io(e)
    }
}

// =============================================================================
// Schema helpers
// =============================================================================

/// Builds the output Parquet schema from the first input file.
///
/// The output schema is identical to the input schema except:
/// - Any existing `___row_id` column is removed.
/// - A fresh `___row_id` INT64 REQUIRED column is appended at the end.
///
/// This ensures the merged file always has a clean, sequential row ID column
/// regardless of what the input files contained.
fn build_parquet_root_schema(first_input_path: &str) -> MergeResult<Arc<Type>> {
    let input_file = File::open(first_input_path)?;
    let reader = SerializedFileReader::new(input_file)?;
    let input_schema_descr = reader.metadata().file_metadata().schema_descr_ptr();
    let input_root = input_schema_descr.root_schema();

    // Collect all fields except the old row ID column.
    let mut parquet_fields: Vec<Arc<Type>> = Vec::new();
    for field in input_root.get_fields() {
        if field.name() != ROW_ID_COLUMN_NAME {
            parquet_fields.push(Arc::new(field.as_ref().clone()));
        }
    }

    // Append a fresh row ID column.
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
// FileCursor — streaming reader with one-shot prefetch
// =============================================================================

/// A cursor over a single sorted Parquet input file.
///
/// Each cursor reads batches sequentially and prefetches the next batch on the
/// shared Rayon pool to overlap IO with merge computation. The cursor exposes
/// the current sort value for heap-based k-way merging and supports zero-copy
/// slicing of the current batch.
struct FileCursor {
    /// Shared reader handle; accessed by the prefetch task on the Rayon pool.
    reader: Arc<Mutex<parquet::arrow::arrow_reader::ParquetRecordBatchReader>>,

    /// Channel for receiving prefetched batches from the Rayon pool.
    prefetch_rx: std::sync::mpsc::Receiver<Option<MergeResult<RecordBatch>>>,

    /// Sender clone given to the Rayon prefetch task.
    prefetch_tx: std::sync::mpsc::SyncSender<Option<MergeResult<RecordBatch>>>,

    /// Whether a prefetch task is currently in flight.
    prefetch_pending: bool,

    /// The batch currently being consumed by the merge loop.
    current_batch: Option<RecordBatch>,

    /// Current row position within `current_batch`.
    row_idx: usize,

    /// Unique identifier for this cursor (index into the cursors array).
    file_id: usize,

    /// Column index of the sort column within each batch.
    sort_col_idx: usize,

    /// Arrow data type of the sort column (cached for fast dispatch).
    sort_col_type: ArrowDataType,
}

impl FileCursor {
    /// Opens a Parquet file and creates a cursor positioned at the first row.
    ///
    /// Returns `Ok(None)` if the file is empty (zero rows). The `___row_id`
    /// column is excluded from the projection since it will be rewritten
    /// during merge.
    fn new(
        path: &str,
        file_id: usize,
        sort_column: &str,
        batch_size: usize,
    ) -> MergeResult<Option<Self>> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();

        // Resolve the sort column's data type for fast value extraction.
        let sort_col_type = schema
            .fields()
            .iter()
            .find(|f| f.name() == sort_column)
            .map(|f| f.data_type().clone())
            .ok_or_else(|| {
                let available: Vec<_> = schema.fields().iter().map(|f| f.name().clone()).collect();
                MergeError::Logic(format!(
                    "Sort column '{}' not found in file '{}' (cursor {}). Available columns: {:?}",
                    sort_column, path, file_id, available
                ))
            })?;

        // Project out the ___row_id column — it will be rewritten.
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

        // Read the first batch to verify the file has data.
        let first_batch = match reader.next() {
            Some(Ok(b)) if b.num_rows() > 0 => b,
            Some(Err(e)) => return Err(e.into()),
            _ => return Ok(None),
        };

        // Resolve sort column index within the projected schema.
        let sort_col_idx = first_batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == sort_column)
            .ok_or_else(|| {
                MergeError::Logic(format!(
                    "Sort column '{}' not found after projection in file '{}'",
                    sort_column, path
                ))
            })?;

        // Bounded channel of capacity 1: at most one prefetched batch in flight.
        let (prefetch_tx, prefetch_rx) =
            std::sync::mpsc::sync_channel::<Option<MergeResult<RecordBatch>>>(1);

        let reader = Arc::new(Mutex::new(reader));

        let mut cursor = Self {
            reader,
            prefetch_rx,
            prefetch_tx,
            prefetch_pending: false,
            current_batch: Some(first_batch),
            row_idx: 0,
            file_id,
            sort_col_idx,
            sort_col_type,
        };

        // Kick off prefetch for the second batch.
        cursor.start_prefetch();

        Ok(Some(cursor))
    }

    /// Dispatches a prefetch task on the Rayon pool to read the next batch.
    /// No-op if a prefetch is already in flight.
    fn start_prefetch(&mut self) {
        if self.prefetch_pending {
            return;
        }
        self.prefetch_pending = true;

        let reader = Arc::clone(&self.reader);
        let tx = self.prefetch_tx.clone();

        get_merge_pool().spawn(move || {
            let mut reader = reader.lock().unwrap();
            let result = match reader.next() {
                Some(Ok(batch)) if batch.num_rows() > 0 => Some(Ok(batch)),
                Some(Err(e)) => Some(Err(MergeError::Arrow(e))),
                _ => None, // EOF or empty batch
            };
            let _ = tx.send(result);
        });
    }

    /// Blocks until the prefetched batch arrives and makes it the current batch.
    /// Returns `false` if there are no more batches (EOF).
    fn load_next_batch(&mut self) -> MergeResult<bool> {
        self.current_batch = None;

        match self.prefetch_rx.recv() {
            Ok(Some(Ok(batch))) => {
                self.current_batch = Some(batch);
                self.row_idx = 0;
                self.prefetch_pending = false;
                self.start_prefetch();
                Ok(true)
            }
            Ok(Some(Err(e))) => {
                self.prefetch_pending = false;
                Err(e)
            }
            Ok(None) | Err(_) => {
                self.prefetch_pending = false;
                Ok(false)
            }
        }
    }

    /// Returns the sort column value at the current row position.
    #[inline]
    fn current_sort_value(&self) -> MergeResult<i64> {
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        get_sort_value(batch, self.row_idx, self.sort_col_idx, &self.sort_col_type)
    }

    /// Returns the sort column value at the last row of the current batch.
    /// Used by Tier 2 to check if the entire batch can be emitted at once.
    #[inline]
    fn last_sort_value(&self) -> MergeResult<i64> {
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        get_sort_value(
            batch,
            batch.num_rows() - 1,
            self.sort_col_idx,
            &self.sort_col_type,
        )
    }

    /// Returns the number of rows in the current batch.
    #[inline]
    fn batch_height(&self) -> usize {
        self.current_batch.as_ref().map_or(0, |b| b.num_rows())
    }

    /// Returns a zero-copy slice of the current batch.
    #[inline]
    fn take_slice(&self, start: usize, len: usize) -> RecordBatch {
        self.current_batch.as_ref().unwrap().slice(start, len)
    }

    /// Advances the cursor by one row. If the current batch is exhausted,
    /// loads the next batch. Returns `false` if no more data is available.
    fn advance(&mut self) -> MergeResult<bool> {
        if self.current_batch.is_none() {
            return Ok(false);
        }
        self.row_idx += 1;
        if self.row_idx >= self.current_batch.as_ref().unwrap().num_rows() {
            self.current_batch = None;
            return self.load_next_batch();
        }
        Ok(true)
    }

    /// Discards the current batch entirely and loads the next one.
    /// Used when the entire remaining batch has been emitted.
    fn advance_past_batch(&mut self) -> MergeResult<bool> {
        self.current_batch = None;
        self.load_next_batch()
    }
}

// =============================================================================
// Min-heap item for k-way merge
// =============================================================================

/// An entry in the min-heap representing one input cursor's current sort value.
///
/// The heap is a standard `BinaryHeap` (max-heap), so `Ord` is reversed to
/// produce min-heap behavior: the smallest sort value is popped first.
#[derive(Debug)]
struct HeapItem {
    /// The sort column value at the cursor's current row.
    sort_value: i64,
    /// Index into the cursors array identifying which input file this is from.
    file_id: usize,
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_value == other.sort_value
    }
}

impl Ord for HeapItem {
    /// Reversed comparison: smallest sort_value has highest priority.
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
// Sort value extraction
// =============================================================================

/// Extracts a sort column value as `i64` from the given row and column.
///
/// Supports all integer, date, timestamp, and duration types that can be
/// losslessly represented as `i64`. Returns a `MergeError::Logic` for
/// unsupported types (e.g. strings, floats).
///
/// # Performance
///
/// This is on the hot path — called once per row in Tier 3 and once per batch
/// boundary in Tier 2. The match is optimized by the compiler into a jump table.
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
                "Unsupported sort column type: {:?}. Only integer, date, timestamp, \
                 and duration types are supported for merge sorting.",
                other
            )));
        }
    };
    Ok(val)
}

// =============================================================================
// Row ID helper
// =============================================================================

/// Appends a `___row_id` column with sequential values `[start_id, start_id + N)`
/// to the given batch, producing a new batch with the output schema.
fn append_row_id(
    batch: &RecordBatch,
    start_id: i64,
    output_schema: &Arc<ArrowSchema>,
) -> MergeResult<RecordBatch> {
    let n = batch.num_rows() as i64;
    let row_ids = Int64Array::from_iter_values(start_id..start_id + n);
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(row_ids));
    let result = RecordBatch::try_new(output_schema.clone(), columns)?;
    Ok(result)
}

// =============================================================================
// JNI entry point
// =============================================================================

/// JNI bridge for `RustBridge.mergeParquetFilesInRust`.
///
/// Accepts a Java `List<String>` of input file paths, an output file path,
/// a sort column name, and a reverse flag (currently unused — merge is always
/// ascending). Returns 0 on success, -1 on failure (with a Java exception thrown).
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
            log_error!("[RUST] Failed to convert input file list: {}", e);
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };

    let output_path: String = match env.get_string(&output_file) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("[RUST] Failed to read output file path: {}", e);
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };

    let sort_col: String = match env.get_string(&sort_column) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("[RUST] Failed to read sort column name: {}", e);
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };

    // Note: reverse sort is accepted but not yet implemented in the merge algorithm.
    let _reverse = is_reverse != 0;

    match merge_streaming(&input_files_vec, &output_path, &sort_col) {
        Ok(_) => 0,
        Err(e) => {
            log_error!("[RUST] Merge failed: {:?}", e);
            let _ = env.throw_new("java/lang/RuntimeException", format!("{}", e));
            -1
        }
    }
}

// =============================================================================
// Public merge API
// =============================================================================

/// Performs a streaming k-way merge of sorted Parquet files using default
/// configuration (batch size, flush threshold).
///
/// # Arguments
///
/// * `input_files` - Paths to sorted Parquet input files.
/// * `output_path` - Path for the merged output Parquet file.
/// * `sort_column` - Name of the column by which the inputs are sorted.
///
/// # Errors
///
/// Returns `MergeError` if any input file cannot be read, the sort column
/// is missing or has an unsupported type, or the output file cannot be written.
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

/// Performs a streaming k-way merge with explicit configuration for batch size
/// and output flush threshold. See [`merge_streaming`] for the default version.
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

    let pool = get_merge_pool();

    log_info!(
        "[RUST] Starting streaming merge: {} input files, sort_column='{}', \
         batch_size={}, flush_rows={}, merge_threads={}, output='{}'",
        input_files.len(),
        sort_column,
        batch_size,
        output_flush_rows,
        pool.current_num_threads(),
        output_path
    );

    // Ensure the output directory exists.
    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    // ── Phase 1: Initialize cursors ─────────────────────────────────────
    let mut cursors: Vec<Option<FileCursor>> = Vec::with_capacity(input_files.len());

    for (file_id, path) in input_files.iter().enumerate() {
        log_debug!("[RUST] Opening cursor {} for file: {}", file_id, path);
        match FileCursor::new(path, file_id, sort_column, batch_size)? {
            Some(cursor) => cursors.push(Some(cursor)),
            None => {
                cursors.push(None);
                log_info!("[RUST] Skipping empty file: {}", path);
            }
        }
    }

    let active_count = cursors.iter().filter(|c| c.is_some()).count();
    if active_count == 0 {
        log_info!("[RUST] All input files were empty, nothing to merge");
        return Ok(());
    }

    // ── Phase 2: Build Arrow and Parquet schemas ────────────────────────
    let first_active_idx = cursors.iter().position(|c| c.is_some()).unwrap();
    let first_batch = cursors[first_active_idx]
        .as_ref()
        .unwrap()
        .current_batch
        .as_ref()
        .unwrap();
    let input_schema = first_batch.schema();

    // Data schema: all columns except ___row_id (used for concat_batches).
    let data_fields: Vec<ArrowField> = input_schema
        .fields()
        .iter()
        .filter(|f| f.name() != ROW_ID_COLUMN_NAME)
        .map(|f| f.as_ref().clone())
        .collect();
    let data_schema = Arc::new(ArrowSchema::new(data_fields.clone()));

    // Output schema: data fields + fresh ___row_id at the end.
    let mut output_fields = data_fields;
    output_fields.push(ArrowField::new(
        ROW_ID_COLUMN_NAME,
        ArrowDataType::Int64,
        false,
    ));
    let output_schema = Arc::new(ArrowSchema::new(output_fields));

    // Parquet schema with the same structure for the file writer.
    let parquet_root = build_parquet_root_schema(&input_files[first_active_idx])?;

    log_info!(
        "[RUST] Merge initialized: {} active cursors, {} columns in output schema",
        active_count,
        output_schema.fields().len()
    );

    // ── Phase 3: Open output writer and spawn IO task ───────────────────
    let output_file = File::create(output_path)?;
    let throttled_writer = RateLimitedWriter::new(output_file, RATE_LIMIT_MB_PER_SEC)
        .map_err(MergeError::Io)?;

    let writer_props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_row_count(Some(MAX_ROW_GROUP_ROWS))
            .set_data_page_size_limit(DATA_PAGE_SIZE_LIMIT)
            .build(),
    );

    let writer =
        SerializedFileWriter::new(throttled_writer, parquet_root, writer_props.clone())?;

    let rg_writer_factory =
        ArrowRowGroupWriterFactory::new(&writer, output_schema.clone());

    let io_tx = spawn_io_task(writer);

    let mut row_group_index: usize = 0;

    // ── Phase 4: Seed the min-heap ──────────────────────────────────────
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(active_count);
    for cursor_opt in &cursors {
        if let Some(cursor) = cursor_opt {
            let sv = cursor.current_sort_value()?;
            heap.push(HeapItem {
                sort_value: sv,
                file_id: cursor.file_id,
            });
        }
    }

    // ── Output accumulator ──────────────────────────────────────────────
    let mut output_chunks: Vec<RecordBatch> = Vec::new();
    let mut output_row_count: usize = 0;
    let mut next_row_id: i64 = 0;
    let mut total_rows_written: usize = 0;

    // ── Flush helper ────────────────────────────────────────────────────
    // Encapsulated as a macro to capture mutable borrows from the enclosing scope.
    macro_rules! flush {
        () => {
            if !output_chunks.is_empty() {
                // Concatenate buffered slices into a single batch.
                let merged = concat_batches(&data_schema, &output_chunks)?;
                output_chunks.clear();
                let n = merged.num_rows();

                // Append sequential row IDs.
                let with_id = append_row_id(&merged, next_row_id, &output_schema)?;
                drop(merged);

                // Create per-column writers for this row group.
                let col_writers =
                    rg_writer_factory.create_column_writers(row_group_index)?;

                // Pair each leaf array with its column writer.
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

                // Encode all columns in parallel on the Rayon pool.
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

                let mut encoded_chunks = Vec::with_capacity(chunk_results.len());
                for r in chunk_results {
                    encoded_chunks.push(r?);
                }

                // Send encoded row group to the IO task (may block if the
                // channel is full, providing natural backpressure).
                io_tx
                    .blocking_send(IoCommand::WriteRowGroup(encoded_chunks))
                    .map_err(|_| {
                        MergeError::Logic("IO task terminated unexpectedly".into())
                    })?;

                row_group_index += 1;
                next_row_id += n as i64;
                total_rows_written += n;
                output_row_count = 0;

                log_debug!(
                    "[RUST] Flushed row group {}: {} rows (total: {})",
                    row_group_index - 1,
                    n,
                    total_rows_written
                );
            }
        };
    }

    // =====================================================================
    // Phase 5: K-way merge loop — three-tier cascade
    // =====================================================================
    while let Some(item) = heap.pop() {
        let file_id = item.file_id;

        // ── TIER 1: Single cursor remaining ─────────────────────────────
        // No heap comparisons needed — dump all remaining data directly.
        if heap.is_empty() {
            let cursor = cursors[file_id].as_mut().unwrap();
            loop {
                let remaining = cursor.batch_height() - cursor.row_idx;
                if remaining > 0 {
                    let slice = cursor.take_slice(cursor.row_idx, remaining);
                    output_row_count += remaining;
                    output_chunks.push(slice);

                    if output_row_count >= output_flush_rows {
                        flush!();
                    }
                }
                if !cursor.advance_past_batch()? {
                    break;
                }
            }
            break;
        }

        // ── TIER 2 & 3: Multiple cursors active ────────────────────────
        let cursor = cursors[file_id].as_mut().unwrap();

        loop {
            let heap_top = heap.peek().unwrap().sort_value;

            // TIER 2: Check if the entire remaining batch can be emitted.
            // If the last value in the batch is ≤ the heap's next smallest,
            // the whole batch is safe to output without per-row inspection.
            let last_val = cursor.last_sort_value()?;
            if last_val <= heap_top {
                let remaining = cursor.batch_height() - cursor.row_idx;
                let slice = cursor.take_slice(cursor.row_idx, remaining);
                output_chunks.push(slice);
                output_row_count += remaining;

                if output_row_count >= output_flush_rows {
                    flush!();
                }

                if !cursor.advance_past_batch()? {
                    break;
                }
                continue;
            }

            // TIER 3: Binary search for the exact boundary within the batch.
            // Find the last row whose sort value is ≤ heap_top.
            let run_start = cursor.row_idx;
            let batch_h = cursor.batch_height();
            let batch = cursor.current_batch.as_ref().unwrap();

            let mut lo = run_start;
            let mut hi = batch_h - 1;

            while lo + 1 < hi {
                let mid = lo + (hi - lo) / 2;
                let mid_val = get_sort_value(
                    batch,
                    mid,
                    cursor.sort_col_idx,
                    &cursor.sort_col_type,
                )?;

                if mid_val <= heap_top {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            let run_end = lo;

            // Emit the safe prefix.
            let run_len = run_end - run_start + 1;
            if run_len > 0 {
                let slice = cursor.take_slice(run_start, run_len);
                output_chunks.push(slice);
                output_row_count += run_len;

                if output_row_count >= output_flush_rows {
                    flush!();
                }
            }

            // Advance past the emitted run.
            cursor.row_idx = run_end;
            if !cursor.advance()? {
                break;
            }

            // Re-insert into the heap if the cursor's next value exceeds heap_top.
            let next_val = cursor.current_sort_value()?;
            if next_val > heap_top {
                heap.push(HeapItem {
                    sort_value: next_val,
                    file_id,
                });
                break;
            }
            // Otherwise, continue the inner loop — this cursor still has the
            // smallest value and can emit more rows.
        }
    }

    // Flush any remaining buffered rows.
    flush!();

    // ── Phase 6: Close the output file ──────────────────────────────────
    let (reply_tx, reply_rx) = oneshot::channel::<MergeResult<ParquetMetaData>>();

    io_tx
        .blocking_send(IoCommand::Close(reply_tx))
        .map_err(|_| MergeError::Logic("IO task terminated before close".into()))?;

    // Drop the sender so the IO task knows no more commands are coming.
    drop(io_tx);

    // Wait for the IO task to flush the footer and close the file.
    let _metadata = reply_rx
        .blocking_recv()
        .map_err(|_| MergeError::Logic("IO task terminated during close".into()))??;

    log_info!(
        "[RUST] Merge complete: {} total rows written to '{}' in {} row groups",
        total_rows_written,
        output_path,
        row_group_index
    );

    Ok(())
}

// =============================================================================
// JNI helper
// =============================================================================

/// Converts a Java `List<String>` to a Rust `Vec<String>` via JNI.
///
/// Uses `push_local_frame` / `pop_local_frame` to avoid leaking JNI local
/// references when processing large lists.
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

        // SAFETY: pop_local_frame with null releases all local refs created
        // since the matching push_local_frame.
        unsafe {
            env.pop_local_frame(&JObject::null())?;
        }
    }

    Ok(result)
}
