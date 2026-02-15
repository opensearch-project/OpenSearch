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

const ROW_ID_COLUMN_NAME: &str = "___row_id";
const BATCH_SIZE: usize = 50_000;
const OUTPUT_FLUSH_ROWS: usize = 500_000;

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
    ) -> PolarsResult<Option<Self>> {
        let mut cursor = Self {
            path,
            rows_read: 0,
            current_batch: None,
            row_idx: 0,
            file_id,
            sort_column,
            batch_size,
        };

        if !cursor.load_next_batch()? {
            return Ok(None);
        }
        Ok(Some(cursor))
    }

    fn load_next_batch(&mut self) -> PolarsResult<bool> {
        // Drop old batch FIRST to free memory before allocating new one
        self.current_batch = None;

        let file = File::open(&self.path).map_err(|e| PolarsError::IO {
            error: e.into(),
            msg: None,
        })?;

        let mut df = ParquetReader::new(file)
            .with_slice(Some((self.rows_read, self.batch_size)))
            .finish()?;

        let _ = df.drop_in_place(ROW_ID_COLUMN_NAME);

        if df.height() == 0 {
            return Ok(false);
        }

        self.rows_read += df.height();
        self.current_batch = Some(df);
        self.row_idx = 0;
        Ok(true)
    }

    fn current_sort_value(&self) -> PolarsResult<i64> {
        match &self.current_batch {
            Some(batch) => get_sort_value(batch, self.row_idx, &self.sort_column),
            None => Err(PolarsError::NoData("Cursor exhausted".into())),
        }
    }

    /// Sort value of the LAST row in current batch — used for batch-level fast path
    fn last_sort_value(&self) -> PolarsResult<i64> {
        match &self.current_batch {
            Some(batch) => get_sort_value(batch, batch.height() - 1, &self.sort_column),
            None => Err(PolarsError::NoData("Cursor exhausted".into())),
        }
    }

    fn batch_height(&self) -> usize {
        self.current_batch.as_ref().map_or(0, |b| b.height())
    }

    fn take_slice(&self, start: usize, len: usize) -> DataFrame {
        let mut slice = self
            .current_batch
            .as_ref()
            .unwrap()
            .slice(start as i64, len);
        slice.rechunk_mut();
        slice
    }

    fn advance(&mut self) -> PolarsResult<bool> {
        if self.current_batch.is_none() {
            return Ok(false);
        }

        self.row_idx += 1;

        let batch_height = self.current_batch.as_ref().unwrap().height();
        if self.row_idx >= batch_height {
            self.current_batch = None;
            return self.load_next_batch();
        }
        Ok(true)
    }

    /// Skip remainder of current batch and load the next one.
    /// Drops old batch before loading to minimize peak memory.
    fn advance_past_batch(&mut self) -> PolarsResult<bool> {
        self.current_batch = None;
        self.load_next_batch()
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
// STREAMING K-WAY MERGE
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
        match FileCursor::new(path.clone(), file_id, sort_column.to_string(), batch_size)? {
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
        return Ok(());
    }

    // Validate sort column
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

    println!("Initialized {} active cursors, starting merge", active_count);

    // ── Open batched ParquetWriter ──────────────────────────────────────
    let output_file = File::create(output_path).map_err(|e| PolarsError::IO {
        error: e.into(),
        msg: None,
    })?;
    let mut writer = ParquetWriter::new(output_file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(output_flush_rows))
        .batched(&output_schema)?;

    // ── Seed min-heap ───────────────────────────────────────────────────
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(active_count);
    for cursor_opt in &cursors {
        if let Some(cursor) = cursor_opt {
            heap.push(HeapItem {
                sort_value: cursor.current_sort_value()?,
                file_id: cursor.file_id,
            });
        }
    }

    // ── Output buffer ───────────────────────────────────────────────────
    let mut output_chunks: Vec<DataFrame> = Vec::new();
    let mut output_row_count = 0usize;
    let mut next_row_id = 0i64;
    let mut total_rows_written = 0usize;
    let sort_col_owned = sort_column.to_string();

    // ── Flush macro ─────────────────────────────────────────────────────
    macro_rules! flush {
        () => {
            if !output_chunks.is_empty() {
                let mut df = concat_df(output_chunks.as_slice())?;
                output_chunks.clear();

                df.rechunk_mut();

                let n = df.height();
                let row_ids: Vec<i64> = (next_row_id..next_row_id + n as i64).collect();
                let row_id_series = Series::new(ROW_ID_COLUMN_NAME.into(), row_ids);
                df.with_column(row_id_series)?;

                writer.write_batch(&df)?;
                drop(df);

                next_row_id += n as i64;
                total_rows_written += output_row_count;
                output_row_count = 0;
            }
        };
    }

    // =====================================================================
    // K-way merge loop — three-tier cascade:
    //   1. heap empty    → dump everything              O(1)
    //   2. last <= top   → emit whole remaining batch   O(1)
    //   3. binary search → emit partial batch           O(log n)
    // =====================================================================
    while let Some(item) = heap.pop() {
        let file_id = item.file_id;

        // ─────────────────────────────────────────────────────────────
        // TIER 1: only one cursor left → dump everything, no comparisons
        // ─────────────────────────────────────────────────────────────
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

        // ─────────────────────────────────────────────────────────────
        // TIER 2 & 3: multiple cursors active
        // ─────────────────────────────────────────────────────────────
        let cursor = cursors[file_id].as_mut().unwrap();

        loop {
            let heap_top = heap.peek().unwrap().sort_value;

            // ── TIER 2: entire remaining batch fits ─────────────────
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
                    break; // cursor exhausted
                }
                // New batch loaded — loop to compare again
                continue;
            }

            // ── TIER 3: binary search for cut point ─────────────────
            // We know:
            //   value[row_idx]    <= heap_top  (this cursor won the heap)
            //   value[batch_h-1]  >  heap_top  (tier 2 failed)
            // Find rightmost index where value <= heap_top
            let run_start = cursor.row_idx;
            let batch_h = cursor.batch_height();
            let batch = cursor.current_batch.as_ref().unwrap();

            let mut lo = run_start;
            let mut hi = batch_h - 1;

            while lo + 1 < hi {
                let mid = lo + (hi - lo) / 2;
                let mid_val = get_sort_value(batch, mid, &sort_col_owned)?;
                if mid_val <= heap_top {
                    lo = mid;
                } else {
                    hi = mid;
                }
            }
            let run_end = lo;

            let run_len = run_end - run_start + 1;
            let slice = cursor.take_slice(run_start, run_len);
            output_chunks.push(slice);
            output_row_count += run_len;

            if output_row_count >= output_flush_rows {
                flush!();
            }

            // Advance past the emitted run
            cursor.row_idx = run_end;
            if !cursor.advance()? {
                break; // exhausted
            }

            // Re-check: does next value still beat heap?
            let next_val = cursor.current_sort_value()?;
            if next_val > heap_top {
                heap.push(HeapItem {
                    sort_value: next_val,
                    file_id,
                });
                break;
            }
            // else: this cursor still wins, continue inner loop
        }
    }

    // Final flush
    flush!();
    writer.finish()?;

    println!("Merge complete: {} total rows written", total_rows_written);
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
