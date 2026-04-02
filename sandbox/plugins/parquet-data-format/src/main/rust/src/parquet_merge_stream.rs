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
//!    batch is ≤ (or ≥ for reverse) the heap's next-smallest (or next-largest)
//!    value, the entire remaining batch is emitted as a zero-copy slice.
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
use parquet::basic::Repetition;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;

use rayon::prelude::*;
use rayon::ThreadPool;

use tokio::runtime::Runtime;
use tokio::sync::{mpsc as tokio_mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::rate_limited_writer::RateLimitedWriter;
use crate::{log_debug, log_error, log_info, SETTINGS_STORE};
use crate::writer_properties_builder::WriterPropertiesBuilder;

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

/// Bounded channel capacity between the merge loop and the IO task.
/// A buffer of 2 allows the merge loop to prepare one row group ahead
/// while the IO task is still writing the previous one.
const IO_CHANNEL_BUFFER: usize = 2;

// =============================================================================
// Sort-direction helpers
// =============================================================================

/// Returns `true` if value tuple `a` should come before or at the same position as
/// value tuple `b` in the requested output order (lexicographic comparison).
#[inline(always)]
fn comes_before_or_equal(a: &[i64], b: &[i64], reverse_sorts: &[bool]) -> bool {
    for (i, (av, bv)) in a.iter().zip(b.iter()).enumerate() {
        if av != bv {
            let reverse = reverse_sorts.get(i).copied().unwrap_or(false);
            return if reverse { av > bv } else { av < bv };
        }
    }
    true // all equal
}

/// Returns `true` if value tuple `a` strictly exceeds value tuple `b` in the sort
/// direction (lexicographic comparison).
#[inline(always)]
fn exceeds(a: &[i64], b: &[i64], reverse_sorts: &[bool]) -> bool {
    for (i, (av, bv)) in a.iter().zip(b.iter()).enumerate() {
        if av != bv {
            let reverse = reverse_sorts.get(i).copied().unwrap_or(false);
            return if reverse { av < bv } else { av > bv };
        }
    }
    false // all equal => not exceeding
}

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
            .worker_threads(4)
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

impl Error for MergeError {}

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

/// Builds the output Parquet schema as the union of all input files.
///
/// The output schema contains every column seen across all input files, except:
/// - Any existing `___row_id` column is removed.
/// - A fresh `___row_id` INT64 REQUIRED column is appended at the end.
///
/// This ensures the merged file always has a clean, sequential row ID column
/// and includes all columns from all inputs.
fn build_parquet_root_schema(input_paths: &[String]) -> MergeResult<Arc<Type>> {
    // Collect all fields (except ___row_id) from every input file,
    // deduplicating by name (first occurrence wins).
    let mut seen_names: Vec<String> = Vec::new();
    let mut parquet_fields: Vec<Arc<Type>> = Vec::new();

    for path in input_paths {
        let input_file = File::open(path)?;
        let reader = SerializedFileReader::new(input_file)?;
        let input_schema_descr = reader.metadata().file_metadata().schema_descr_ptr();
        let input_root = input_schema_descr.root_schema();

        for field in input_root.get_fields() {
            if field.name() != ROW_ID_COLUMN_NAME
                && !seen_names.contains(&field.name().to_string())
            {
                seen_names.push(field.name().to_string());
                parquet_fields.push(Arc::new(field.as_ref().clone()));
            }
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

/// Validates that every input file contains a `___row_id` column, has the
/// sort column, and has at least one row. Called once upfront before opening
/// any cursors, so that we fail fast with a clear error rather than
/// discovering problems mid-merge.
fn validate_input_files(input_files: &[String], sort_columns: &[String]) -> MergeResult<()> {
    for (idx, path) in input_files.iter().enumerate() {
        let file = File::open(path).map_err(|e| {
            MergeError::Logic(format!(
                "Cannot open input file '{}' (index {}): {}",
                path, idx, e
            ))
        })?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();
        let file_metadata = metadata.file_metadata();

        let num_rows = file_metadata.num_rows();
        if num_rows == 0 {
            return Err(MergeError::Logic(format!(
                "Input file '{}' (index {}) is empty (0 rows). \
                 All input files must be non-empty.",
                path, idx
            )));
        }

        let schema_descr = file_metadata.schema_descr_ptr();
        let root = schema_descr.root_schema();
        let has_row_id = root
            .get_fields()
            .iter()
            .any(|f| f.name() == ROW_ID_COLUMN_NAME);
        if !has_row_id {
            return Err(MergeError::Logic(format!(
                "Input file '{}' (index {}) is missing required '{}' column. \
                 All input files must contain a row ID column.",
                path, idx, ROW_ID_COLUMN_NAME
            )));
        }

        for sort_col in sort_columns {
            let has_sort_col = root
                .get_fields()
                .iter()
                .any(|f| f.name() == sort_col.as_str());
            if !has_sort_col {
                let available: Vec<_> = root
                    .get_fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
                return Err(MergeError::Logic(format!(
                    "Sort column '{}' not found in file '{}' (index {}). \
                     Available columns: {:?}",
                    sort_col, path, idx, available
                )));
            }
        }
    }
    Ok(())
}

/// Pads a batch to conform to the target schema by adding null-filled columns
/// for any fields present in `target_schema` but missing from the batch.
///
/// Returns the batch unchanged (no copy) when schemas already match.
fn pad_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &Arc<ArrowSchema>,
) -> MergeResult<RecordBatch> {
    let batch_schema = batch.schema();
    if batch_schema.fields() == target_schema.fields() {
        return Ok(batch.clone());
    }

    let num_rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());

    for field in target_schema.fields() {
        match batch_schema.index_of(field.name()) {
            Ok(col_idx) => columns.push(batch.column(col_idx).clone()),
            Err(_) => {
                columns.push(arrow::array::new_null_array(field.data_type(), num_rows));
            }
        }
    }

    Ok(RecordBatch::try_new(target_schema.clone(), columns)?)
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

    /// Column indices of the sort columns within each batch.
    sort_col_indices: Vec<usize>,

    /// Arrow data types of the sort columns (cached for fast dispatch).
    sort_col_types: Vec<ArrowDataType>,
}

impl FileCursor {
    /// Opens a Parquet file and creates a cursor positioned at the first row.
    ///
    /// Returns the cursor and the projected Arrow schema (all columns except
    /// `___row_id`) for union-schema computation. The schema is not stored
    /// in the cursor since it is only needed once during initialization.
    ///
    /// The `___row_id` column is excluded from the projection since it will be
    /// rewritten during merge.
    ///
    /// # Preconditions
    ///
    /// The caller must have already validated (via [`validate_input_files`])
    /// that the file is non-empty and contains the required columns.
    fn new(
        path: &str,
        file_id: usize,
        sort_columns: &[String],
        batch_size: usize,
    ) -> MergeResult<(Self, Arc<ArrowSchema>)> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();

        // Resolve each sort column's data type for fast value extraction.
        let mut sort_col_types = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let dt = schema
                .fields()
                .iter()
                .find(|f| f.name() == col_name.as_str())
                .map(|f| f.data_type().clone())
                .ok_or_else(|| {
                    MergeError::Logic(format!(
                        "Sort column '{}' not found in file '{}' (cursor {})",
                        col_name, path, file_id
                    ))
                })?;
            sort_col_types.push(dt);
        }

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

        // Read the first batch — file is guaranteed non-empty by upfront validation.
        let first_batch = match reader.next() {
            Some(Ok(b)) if b.num_rows() > 0 => b,
            Some(Err(e)) => return Err(e.into()),
            _ => {
                return Err(MergeError::Logic(format!(
                    "File '{}' (cursor {}) yielded no rows despite passing validation",
                    path, file_id
                )));
            }
        };

        // Capture the projected schema for union-schema computation.
        let projected_schema = first_batch.schema();

        // Resolve sort column indices within the projected schema.
        let mut sort_col_indices = Vec::with_capacity(sort_columns.len());
        for col_name in sort_columns {
            let idx = projected_schema
                .fields()
                .iter()
                .position(|f| f.name() == col_name.as_str())
                .ok_or_else(|| {
                    MergeError::Logic(format!(
                        "Sort column '{}' not found after projection in file '{}'",
                        col_name, path
                    ))
                })?;
            sort_col_indices.push(idx);
        }

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
            sort_col_indices,
            sort_col_types,
        };

        // Kick off prefetch for the second batch.
        cursor.start_prefetch();

        Ok((cursor, projected_schema))
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

    /// Returns the sort column values at the current row position.
    #[inline]
    fn current_sort_values(&self) -> MergeResult<Vec<i64>> {
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        get_sort_values(batch, self.row_idx, &self.sort_col_indices, &self.sort_col_types)
    }

    /// Returns the sort column values at the last row of the current batch.
    #[inline]
    fn last_sort_values(&self) -> MergeResult<Vec<i64>> {
        let batch = self
            .current_batch
            .as_ref()
            .ok_or_else(|| MergeError::Logic("Cursor exhausted".into()))?;
        get_sort_values(
            batch,
            batch.num_rows() - 1,
            &self.sort_col_indices,
            &self.sort_col_types,
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
// Min-heap / Max-heap item for k-way merge
// =============================================================================

/// An entry in the binary heap representing one input cursor's current sort
/// value.
///
/// The heap is a standard `BinaryHeap` (max-heap internally). The `Ord`
/// implementation is parameterised by `reverse_sorts`:
///
/// - **ascending** (`reverse_sorts[i] = false`): comparison is *reversed* so
///   that the **smallest** `sort_value` has highest priority (min-heap).
/// - **descending** (`reverse_sorts[i] = true`): comparison is *natural* so
///   that the **largest** `sort_value` has highest priority (max-heap).
#[derive(Debug)]
struct HeapItem {
    /// The sort column values at the cursor's current row.
    sort_values: Vec<i64>,
    /// Index into the cursors array identifying which input file this is from.
    file_id: usize,
    /// Sort direction per column: `false` = ascending (min-heap), `true` = descending
    /// (max-heap).
    reverse_sorts: Vec<bool>,
}

impl Eq for HeapItem {}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_values == other.sort_values
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Lexicographic comparison across all sort columns
        for (i, (a, b)) in self.sort_values.iter().zip(other.sort_values.iter()).enumerate() {
            let reverse = self.reverse_sorts.get(i).copied().unwrap_or(false);
            let c = if reverse {
                a.cmp(b)
            } else {
                b.cmp(a)
            };
            if c != Ordering::Equal {
                return c;
            }
        }
        Ordering::Equal
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

/// Extracts sort column values as `Vec<i64>` from the given row for multiple columns.
#[inline]
fn get_sort_values(
    batch: &RecordBatch,
    row: usize,
    col_indices: &[usize],
    dtypes: &[ArrowDataType],
) -> MergeResult<Vec<i64>> {
    let mut values = Vec::with_capacity(col_indices.len());
    for (col_idx, dtype) in col_indices.iter().zip(dtypes.iter()) {
        values.push(get_sort_value(batch, row, *col_idx, dtype)?);
    }
    Ok(values)
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
/// Accepts a Java `List<Path>` of input file paths, an output file path,
/// a `List<String>` of sort column names, and a `List<Boolean>` of per-column
/// reverse flags.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_mergeParquetFilesInRust(
    mut env: JNIEnv,
    _class: JClass,
    input_files: JObject,
    output_file: JString,
    index_name: JString,
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

    let idx_name: String = match env.get_string(&index_name) {
        Ok(s) => s.into(),
        Err(e) => {
            log_error!("[RUST] Failed to read index name: {}", e);
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            return -1;
        }
    };

    // Look up sort config from SETTINGS_STORE by index name
    let (sort_cols, reverse_flags) = SETTINGS_STORE
        .get(&idx_name)
        .map(|s| (s.sort_columns.clone(), s.reverse_sorts.clone()))
        .unwrap_or_default();

    match merge_streaming_full(&input_files_vec, &output_path, &idx_name, &sort_cols, &reverse_flags) {
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

/// Performs a streaming k-way merge of sorted Parquet files in **ascending**
/// order using default configuration (batch size, flush threshold).
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
    index_name: &str,
    sort_columns: &[String],
) -> MergeResult<()> {
    let default_reverse = vec![false; sort_columns.len()];
    merge_streaming_full(input_files, output_path, index_name, sort_columns, &default_reverse)
}

/// Performs a streaming k-way merge with an explicit sort direction per column.
///
/// Each entry in `reverse_sorts` corresponds to the sort column at the same
/// index. `true` means descending, `false` means ascending.
pub fn merge_streaming_full(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    sort_columns: &[String],
    reverse_sorts: &[bool],
) -> MergeResult<()> {
    merge_streaming_with_config(
        input_files,
        output_path,
        index_name,
        sort_columns,
        reverse_sorts,
        BATCH_SIZE,
        OUTPUT_FLUSH_ROWS,
    )
}

/// Performs a streaming k-way merge with explicit configuration for batch size,
/// output flush threshold, and sort direction.
pub fn merge_streaming_with_config(
    input_files: &[String],
    output_path: &str,
    index_name: &str,
    sort_columns: &[String],
    reverse_sorts: &[bool],
    batch_size: usize,
    output_flush_rows: usize,
) -> MergeResult<()> {
    if input_files.is_empty() {
        return Ok(());
    }

    if sort_columns.is_empty() {
        return Err(MergeError::Logic(
            "At least one sort column is required for merge".into(),
        ));
    }

    let pool = get_merge_pool();
    let direction_label = if reverse_sorts.iter().all(|&r| !r) { "ascending" } else if reverse_sorts.iter().all(|&r| r) { "descending" } else { "mixed" };

    log_info!(
        "[RUST] Starting streaming merge ({}): {} input files, sort_columns={:?}, \
         batch_size={}, flush_rows={}, merge_threads={}, output='{}'",
        direction_label,
        input_files.len(),
        sort_columns,
        batch_size,
        output_flush_rows,
        pool.current_num_threads(),
        output_path
    );

    // Validate that the output directory already exists.
    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.exists() {
            return Err(MergeError::Logic(format!(
                "Output directory '{}' does not exist. \
                 The caller must ensure the directory is created before merge.",
                parent.display()
            )));
        }
    }

    // ── Upfront validation ──────────────────────────────────────────────
    // Check all files once: non-empty, have ___row_id, have sort column.
    validate_input_files(input_files, sort_columns)?;

    // ── Phase 1: Initialize cursors and collect projected schemas ────────
    let mut cursors: Vec<FileCursor> = Vec::with_capacity(input_files.len());
    let mut all_schemas: Vec<ArrowSchema> = Vec::with_capacity(input_files.len());

    for (file_id, path) in input_files.iter().enumerate() {
        log_debug!("[RUST] Opening cursor {} for file: {}", file_id, path);
        let (cursor, projected_schema) =
            FileCursor::new(path, file_id, sort_columns, batch_size)?;
        cursors.push(cursor);
        all_schemas.push(projected_schema.as_ref().clone());
    }

    let num_cursors = cursors.len();

    // ── Phase 2: Build union Arrow schema ───────────────────────────────
    //
    // Each file may have a slightly different set of columns (e.g. sparse
    // fields added by later indexing). We merge all projected schemas into
    // a single union schema so the output contains every column seen across
    // all inputs. Batches from files missing a column are padded with nulls.
    let union_data_schema = ArrowSchema::try_merge(all_schemas).map_err(|e| {
        MergeError::Logic(format!(
            "Failed to compute union schema across input files: {}. \
             All input files must have compatible column types.",
            e
        ))
    })?;
    let data_schema = Arc::new(union_data_schema);

    // Output schema: union data fields + fresh ___row_id at the end.
    let mut output_fields: Vec<ArrowField> = data_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    output_fields.push(ArrowField::new(
        ROW_ID_COLUMN_NAME,
        ArrowDataType::Int64,
        false,
    ));
    let output_schema = Arc::new(ArrowSchema::new(output_fields));

    // Parquet schema as union of all input files.
    let parquet_root = build_parquet_root_schema(input_files)?;

    log_info!(
        "[RUST] Merge initialized ({}): {} cursors, {} columns in output schema",
        direction_label,
        num_cursors,
        output_schema.fields().len()
    );

    // ── Phase 3: Open output writer and spawn IO task ───────────────────
    let output_file = File::create(output_path)?;
    let throttled_writer = RateLimitedWriter::new(output_file, RATE_LIMIT_MB_PER_SEC)
        .map_err(MergeError::Io)?;

    // Look up settings for this index; fall back to defaults if not found
    let config = SETTINGS_STORE
        .get(index_name)
        .map(|r| r.clone())
        .unwrap_or_default();
    let writer_props = Arc::new(WriterPropertiesBuilder::build(&config));

    let writer =
        SerializedFileWriter::new(throttled_writer, parquet_root, writer_props.clone())?;

    let rg_writer_factory =
        ArrowRowGroupWriterFactory::new(&writer, output_schema.clone());

    let io_tx = spawn_io_task(writer);

    let mut row_group_index: usize = 0;

    // ── Phase 4: Seed the min/max-heap ──────────────────────────────────
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(num_cursors);
    for cursor in &cursors {
        let sv = cursor.current_sort_values()?;
        heap.push(HeapItem {
            sort_values: sv,
            file_id: cursor.file_id,
            reverse_sorts: reverse_sorts.to_vec(),
        });
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
            let cursor = &mut cursors[file_id];
            loop {
                let remaining = cursor.batch_height() - cursor.row_idx;
                if remaining > 0 {
                    let slice = cursor.take_slice(cursor.row_idx, remaining);
                    let padded = pad_batch_to_schema(&slice, &data_schema)?;
                    output_row_count += remaining;
                    output_chunks.push(padded);

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
        let cursor = &mut cursors[file_id];

        loop {
            let heap_top = &heap.peek().unwrap().sort_values;

            // TIER 2: Check if the entire remaining batch can be emitted.
            let last_val = cursor.last_sort_values()?;
            if comes_before_or_equal(&last_val, heap_top, reverse_sorts) {
                let remaining = cursor.batch_height() - cursor.row_idx;
                let slice = cursor.take_slice(cursor.row_idx, remaining);
                let padded = pad_batch_to_schema(&slice, &data_schema)?;
                output_chunks.push(padded);
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
            let run_start = cursor.row_idx;
            let batch_h = cursor.batch_height();
            let batch = cursor.current_batch.as_ref().unwrap();

            let mut lo = run_start;
            let mut hi = batch_h - 1;

            while lo + 1 < hi {
                let mid = lo + (hi - lo) / 2;
                let mid_val = get_sort_values(
                    batch,
                    mid,
                    &cursor.sort_col_indices,
                    &cursor.sort_col_types,
                )?;

                if comes_before_or_equal(&mid_val, heap_top, reverse_sorts) {
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
                let padded = pad_batch_to_schema(&slice, &data_schema)?;
                output_chunks.push(padded);
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

            // Re-insert into the heap if the cursor's next values exceed
            // heap_top in the sort direction.
            let next_val = cursor.current_sort_values()?;
            if exceeds(&next_val, heap_top, reverse_sorts) {
                heap.push(HeapItem {
                    sort_values: next_val,
                    file_id,
                    reverse_sorts: reverse_sorts.to_vec(),
                });
                break;
            }
            // Otherwise, continue the inner loop — this cursor still has the
            // best value and can emit more rows.
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
        "[RUST] Merge complete ({}): {} total rows written to '{}' in {} row groups",
        direction_label,
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

