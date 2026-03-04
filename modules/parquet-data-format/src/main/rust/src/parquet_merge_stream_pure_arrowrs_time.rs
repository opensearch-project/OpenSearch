use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use polars::prelude::*;
use polars::datatypes::DataType;
use polars_core::utils::concat_df;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::time::{Duration, Instant};

const ROW_ID_COLUMN_NAME: &str = "___row_id";
const BATCH_SIZE: usize = 50_000;
const OUTPUT_FLUSH_ROWS: usize = 500_000;

// =============================================================================
// Timing accumulator
// =============================================================================

struct MergeTimings {
    file_open_and_init: Duration,
    parquet_read: Duration,
    take_slice: Duration,
    concat_batches: Duration,
    append_row_id: Duration,
    polars_write: Duration,
    writer_finish: Duration,
    heap_ops: Duration,
    sort_value_lookups: Duration,
    schema_validation: Duration,
    writer_init: Duration,

    file_open_count: usize,
    parquet_read_count: usize,
    take_slice_count: usize,
    concat_count: usize,
    append_row_id_count: usize,
    polars_write_count: usize,
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
            polars_write: Duration::ZERO,
            writer_finish: Duration::ZERO,
            heap_ops: Duration::ZERO,
            sort_value_lookups: Duration::ZERO,
            schema_validation: Duration::ZERO,
            writer_init: Duration::ZERO,

            file_open_count: 0,
            parquet_read_count: 0,
            take_slice_count: 0,
            concat_count: 0,
            append_row_id_count: 0,
            polars_write_count: 0,
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
        println!("MERGE TIMING BREAKDOWN (Polars)");
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

        println!("--- SCHEMA VALIDATION + WRITER INIT ---");
        println!(
            "  schema_validation:      {:>10.3}s",
            self.schema_validation.as_secs_f64()
        );
        println!(
            "  writer_init:            {:>10.3}s",
            self.writer_init.as_secs_f64()
        );
        println!();

        println!("--- READ (Polars ParquetReader::with_slice) ---");
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

        println!("--- SLICE (DataFrame::slice — zero-copy offset) ---");
        println!(
            "  take_slice:             {:>10.3}s  ({} calls, {} rows)",
            self.take_slice.as_secs_f64(),
            self.take_slice_count,
            self.total_rows_sliced
        );
        println!();

        println!("--- FLUSH (concat + row_id + write) ---");
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
            "  polars_write:           {:>10.3}s  ({} calls, {} rows)",
            self.polars_write.as_secs_f64(),
            self.polars_write_count,
            self.total_rows_written
        );
        if self.total_rows_written > 0 && self.polars_write.as_secs_f64() > 0.0 {
            println!(
                "  write throughput:       {:>10.0} rows/sec",
                self.total_rows_written as f64 / self.polars_write.as_secs_f64()
            );
        }
        let flush_total = self.concat_batches + self.append_row_id + self.polars_write;
        println!(
            "  flush total:            {:>10.3}s  ({} flushes)",
            flush_total.as_secs_f64(),
            self.flush_count
        );
        println!();

        println!("--- FINISH (writer.finish — flush footer) ---");
        println!(
            "  writer_finish:          {:>10.3}s",
            self.writer_finish.as_secs_f64()
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
            + self.polars_write
            + self.writer_finish
            + self.heap_ops
            + self.sort_value_lookups
            + self.schema_validation
            + self.writer_init;

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
        println!("  schema_validation:      {:>6.1}%", self.schema_validation.as_secs_f64() / t * 100.0);
        println!("  writer_init:            {:>6.1}%", self.writer_init.as_secs_f64() / t * 100.0);
        println!("  parquet_read:           {:>6.1}%", self.parquet_read.as_secs_f64() / t * 100.0);
        println!("  take_slice:             {:>6.1}%", self.take_slice.as_secs_f64() / t * 100.0);
        println!("  concat_batches:         {:>6.1}%", self.concat_batches.as_secs_f64() / t * 100.0);
        println!("  append_row_id:          {:>6.1}%", self.append_row_id.as_secs_f64() / t * 100.0);
        println!("  polars_write:           {:>6.1}%", self.polars_write.as_secs_f64() / t * 100.0);
        println!("  writer_finish:          {:>6.1}%", self.writer_finish.as_secs_f64() / t * 100.0);
        println!("  heap_ops:               {:>6.1}%", self.heap_ops.as_secs_f64() / t * 100.0);
        println!("  sort_lookups:           {:>6.1}%", self.sort_value_lookups.as_secs_f64() / t * 100.0);
        println!("  unaccounted:            {:>6.1}%", unaccounted.as_secs_f64() / t * 100.0);
        println!("======================================================================");
        println!();
    }
}

// =============================================================================
// FileCursor — reads BATCH_SIZE rows at a time via with_slice
// =============================================================================

struct FileCursor {
    path: String,
    rows_read: usize,
    current_batch: Option<DataFrame>,
    row_idx: usize,
    file_id: usize,
    sort_column: String,
    batch_size: usize,
}

impl FileCursor {
    fn new(
        path: String,
        file_id: usize,
        sort_column: String,
        batch_size: usize,
        timings: &mut MergeTimings,
    ) -> PolarsResult<Option<Self>> {
        let t0 = Instant::now();

        let mut cursor = Self {
            path,
            rows_read: 0,
            current_batch: None,
            row_idx: 0,
            file_id,
            sort_column,
            batch_size,
        };

        if !cursor.load_next_batch(timings)? {
            timings.file_open_and_init += t0.elapsed();
            timings.file_open_count += 1;
            return Ok(None);
        }

        timings.file_open_and_init += t0.elapsed();
        timings.file_open_count += 1;
        Ok(Some(cursor))
    }

    fn load_next_batch(&mut self, timings: &mut MergeTimings) -> PolarsResult<bool> {
        // Drop old batch FIRST to free memory before allocating new one
        self.current_batch = None;

        let t0 = Instant::now();

        let file = File::open(&self.path).map_err(|e| PolarsError::IO {
            error: e.into(),
            msg: None,
        })?;

        let mut df = ParquetReader::new(file)
            .with_slice(Some((self.rows_read, self.batch_size)))
            .finish()?;

        let _ = df.drop_in_place(ROW_ID_COLUMN_NAME);

        let elapsed = t0.elapsed();

        if df.height() == 0 {
            timings.parquet_read += elapsed;
            timings.parquet_read_count += 1;
            return Ok(false);
        }

        timings.parquet_read += elapsed;
        timings.parquet_read_count += 1;
        timings.total_rows_read += df.height();

        self.rows_read += df.height();
        self.current_batch = Some(df);
        self.row_idx = 0;
        Ok(true)
    }

    #[inline]
    fn current_sort_value_raw(&self) -> PolarsResult<i64> {
        match &self.current_batch {
            Some(batch) => get_sort_value(batch, self.row_idx, &self.sort_column),
            None => Err(PolarsError::NoData("Cursor exhausted".into())),
        }
    }

    #[inline]
    fn current_sort_value(&self, timings: &mut MergeTimings) -> PolarsResult<i64> {
        let batch = self.current_batch.as_ref()
            .ok_or_else(|| PolarsError::NoData("Cursor exhausted".into()))?;
        let t0 = Instant::now();
        let val = get_sort_value(batch, self.row_idx, &self.sort_column)?;
        timings.sort_value_lookups += t0.elapsed();
        timings.sort_lookup_count += 1;
        Ok(val)
    }

    #[inline]
    fn last_sort_value(&self, timings: &mut MergeTimings) -> PolarsResult<i64> {
        let batch = self.current_batch.as_ref()
            .ok_or_else(|| PolarsError::NoData("Cursor exhausted".into()))?;
        let t0 = Instant::now();
        let val = get_sort_value(batch, batch.height() - 1, &self.sort_column)?;
        timings.sort_value_lookups += t0.elapsed();
        timings.sort_lookup_count += 1;
        Ok(val)
    }

    #[inline]
    fn batch_height(&self) -> usize {
        self.current_batch.as_ref().map_or(0, |b| b.height())
    }

    #[inline]
    fn take_slice(&self, start: usize, len: usize, timings: &mut MergeTimings) -> DataFrame {
        let t0 = Instant::now();
        let mut slice = self
            .current_batch
            .as_ref()
            .unwrap()
            .slice(start as i64, len);
        slice.rechunk_mut();
        timings.take_slice += t0.elapsed();
        timings.take_slice_count += 1;
        timings.total_rows_sliced += len;
        slice
    }

    fn advance(&mut self, timings: &mut MergeTimings) -> PolarsResult<bool> {
        if self.current_batch.is_none() {
            return Ok(false);
        }

        self.row_idx += 1;

        let batch_height = self.current_batch.as_ref().unwrap().height();
        if self.row_idx >= batch_height {
            self.current_batch = None;
            return self.load_next_batch(timings);
        }
        Ok(true)
    }

    fn advance_past_batch(&mut self, timings: &mut MergeTimings) -> PolarsResult<bool> {
        self.current_batch = None;
        self.load_next_batch(timings)
    }

    fn is_exhausted(&self) -> bool {
        self.current_batch.is_none()
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
// STREAMING K-WAY MERGE with buffered flush + timing
// =============================================================================

pub fn merge_streaming(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
) -> PolarsResult<()> {
    merge_streaming_with_config(input_files, output_path, sort_column, BATCH_SIZE, OUTPUT_FLUSH_ROWS)
}

pub fn merge_streaming_with_config(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
    batch_size: usize,
    output_flush_rows: usize,
) -> PolarsResult<()> {
    if input_files.is_empty() {
        return Ok(());
    }

    let total_start = Instant::now();
    let mut timings = MergeTimings::new();

    println!("Starting streaming merge of {} files", input_files.len());

    if let Some(parent) = Path::new(output_path).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| PolarsError::IO {
                error: e.into(),
                msg: None,
            })?;
        }
    }

    // ── Initialise cursors (indexed by file_id) ─────────────────────────
    let mut cursors: Vec<Option<FileCursor>> = Vec::with_capacity(input_files.len());

    for (file_id, path) in input_files.iter().enumerate() {
        println!("Initializing cursor for file {}: {}", file_id, path);
        match FileCursor::new(path.clone(), file_id, sort_column.to_string(), batch_size, &mut timings)? {
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

    // ── Validate sort column + build schema ─────────────────────────────
    let t_validate = Instant::now();

    for (i, cursor_opt) in cursors.iter().enumerate() {
        if let Some(cursor) = cursor_opt {
            let batch = cursor.current_batch.as_ref().unwrap();
            if !batch.schema().contains(sort_column) {
                let cols: Vec<_> = batch.schema().iter().map(|(n, _)| n.to_string()).collect();
                return Err(PolarsError::ColumnNotFound(
                    format!(
                        "Sort column '{}' not found in file {} (cursor {}). Available: {:?}",
                        sort_column,
                        input_files.get(i).unwrap_or(&"unknown".to_string()),
                        i,
                        cols
                    )
                        .into(),
                ));
            }
        }
    }

    // Build output schema WITH ___row_id
    let first_cursor = cursors.iter().find_map(|c| c.as_ref()).unwrap();
    let mut output_schema = first_cursor
        .current_batch
        .as_ref()
        .unwrap()
        .schema()
        .as_ref()
        .clone();
    output_schema.insert(ROW_ID_COLUMN_NAME.into(), DataType::Int64);

    timings.schema_validation = t_validate.elapsed();

    println!("Initialized {} active cursors, starting merge", active_count);

    // ── Open batched ParquetWriter ──────────────────────────────────────
    let t_writer_init = Instant::now();

    let output_file = File::create(output_path).map_err(|e| PolarsError::IO {
        error: e.into(),
        msg: None,
    })?;
    let mut writer = ParquetWriter::new(output_file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(output_flush_rows))
        .batched(&output_schema)?;

    timings.writer_init = t_writer_init.elapsed();

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
    let mut output_chunks: Vec<DataFrame> = Vec::new();
    let mut output_row_count = 0usize;
    let mut next_row_id = 0i64;
    let sort_col_owned = sort_column.to_string();

    // ── Flush: concat → row_id → write ──────────────────────────────────
    macro_rules! flush {
        () => {
            if !output_chunks.is_empty() {
                // concat
                let t_concat = Instant::now();
                let mut df = concat_df(output_chunks.as_slice())?;
                output_chunks.clear();
                df.rechunk_mut();
                timings.concat_batches += t_concat.elapsed();
                timings.concat_count += 1;
                timings.total_rows_flushed += df.height();

                let n = df.height();

                // append ___row_id
                let t_rowid = Instant::now();
                let row_ids: Vec<i64> = (next_row_id..next_row_id + n as i64).collect();
                let row_id_series = Series::new(ROW_ID_COLUMN_NAME.into(), row_ids);
                df.with_column(row_id_series)?;
                timings.append_row_id += t_rowid.elapsed();
                timings.append_row_id_count += 1;

                // write
                let t_write = Instant::now();
                writer.write_batch(&df)?;
                timings.polars_write += t_write.elapsed();
                timings.polars_write_count += 1;
                timings.total_rows_written += n;

                drop(df);

                next_row_id += n as i64;
                timings.flush_count += 1;
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

        // ─────────────────────────────────────────────────────────────
        // TIER 1: only one cursor left → dump everything
        // ─────────────────────────────────────────────────────────────
        if heap.is_empty() {
            let cursor = cursors[file_id].as_mut().unwrap();
            loop {
                let remaining = cursor.batch_height() - cursor.row_idx;
                if remaining > 0 {
                    let slice = cursor.take_slice(cursor.row_idx, remaining, &mut timings);
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

        // ─────────────────────────────────────────────────────────────
        // TIER 2 & 3: multiple cursors active
        // ─────────────────────────────────────────────────────────────
        let cursor = cursors[file_id].as_mut().unwrap();

        loop {
            let t0 = Instant::now();
            let heap_top = heap.peek().unwrap().sort_value;
            timings.heap_ops += t0.elapsed();

            // ── TIER 2: entire remaining batch fits ─────────────────
            let last_val = cursor.last_sort_value(&mut timings)?;
            if last_val <= heap_top {
                let remaining = cursor.batch_height() - cursor.row_idx;
                let slice = cursor.take_slice(cursor.row_idx, remaining, &mut timings);
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

            // ── TIER 3: binary search for cut point ─────────────────
            let run_start = cursor.row_idx;
            let batch_h = cursor.batch_height();
            let batch = cursor.current_batch.as_ref().unwrap();

            let mut lo = run_start;
            let mut hi = batch_h - 1;

            while lo + 1 < hi {
                let mid = lo + (hi - lo) / 2;
                let t0 = Instant::now();
                let mid_val = get_sort_value(batch, mid, &sort_col_owned)?;
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

            // Advance past the emitted run
            cursor.row_idx = run_end;
            if !cursor.advance(&mut timings)? {
                break;
            }

            // Re-check: does next value still beat heap?
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

    let t_finish = Instant::now();
    writer.finish()?;
    timings.writer_finish = t_finish.elapsed();

    let total_elapsed = total_start.elapsed();
    timings.print_summary(total_elapsed);

    println!("Merge complete: {} total rows written", timings.total_rows_written);
    Ok(())
}

// =============================================================================
// Helpers
// =============================================================================

fn get_sort_value(df: &DataFrame, row: usize, col: &str) -> PolarsResult<i64> {
    let series = df.column(col)?;
    match series.dtype() {
        DataType::Int64 => series.i64()?.get(row)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::Int32 => series.i32()?.get(row).map(|v| v as i64)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::UInt64 => series.u64()?.get(row).map(|v| v as i64)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::UInt32 => series.u32()?.get(row).map(|v| v as i64)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::Datetime(_, _) => series.datetime()?.phys.get(row)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::Date => series.date()?.phys.get(row).map(|v| v as i64)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        DataType::Duration(_) => series.duration()?.phys.get(row)
            .ok_or_else(|| PolarsError::NoData("Null sort value".into())),
        other => Err(PolarsError::InvalidOperation(
            format!("Unsupported sort type: {:?}", other).into(),
        )),
    }
}

fn convert_java_list_to_vec(
    env: &mut JNIEnv,
    list: JObject,
) -> Result<Vec<String>, Box<dyn Error>> {
    let size = env.call_method(&list, "size", "()I", &[])?.i()? as usize;
    let mut result = Vec::with_capacity(size);
    for i in 0..size {
        env.push_local_frame(4)?;
        let obj = env
            .call_method(&list, "get", "(I)Ljava/lang/Object;", &[(i as i32).into()])?
            .l()?;
        let jstring = env
            .call_method(&obj, "toString", "()Ljava/lang/String;", &[])?
            .l()?;
        let rust_string: String = env.get_string(&jstring.into())?.into();
        result.push(rust_string);
        unsafe { env.pop_local_frame(&JObject::null())?; }
    }
    Ok(result)
}
