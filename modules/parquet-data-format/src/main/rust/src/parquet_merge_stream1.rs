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
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

const ROW_ID_COLUMN_NAME: &str = "___row_id";
const BATCH_SIZE: usize = 10_000;
const OUTPUT_FLUSH_ROWS: usize = 100_000;

// =============================================================================
// FileCursor - Uses sink_batches
// =============================================================================

struct FileCursor {
    receiver: Receiver<DataFrame>,
    _producer_handle: JoinHandle<()>,
    current_batch: Option<DataFrame>,
    row_idx: usize,
    file_id: usize,
    sort_column: String,
}

impl FileCursor {
    fn new(
        path: String,
        file_id: usize,
        sort_column: String,
        batch_size: usize,
    ) -> PolarsResult<Option<Self>> {
        let (sender, receiver) = sync_channel::<DataFrame>(0);
        let thread_path = path.clone();

        let producer_handle = thread::spawn(move || {
            let result = Self::produce_batches(thread_path, sender, batch_size);
            if let Err(e) = result {
                eprintln!("Batch producer error: {:?}", e);
            }
        });

        let mut cursor = Self {
            receiver,
            _producer_handle: producer_handle,
            current_batch: None,
            row_idx: 0,
            file_id,
            sort_column,
        };

        if !cursor.load_next_batch() {
            return Ok(None);
        }

        Ok(Some(cursor))
    }

    fn produce_batches(
        path: String,
        sender: SyncSender<DataFrame>,
        batch_size: usize,
    ) -> PolarsResult<()> {
        let pl_path = PlPath::Local(Arc::from(Path::new(&path)));
        let lf = LazyFrame::scan_parquet(pl_path, ScanArgsParquet::default())?;

        let callback = move |df: DataFrame| -> PolarsResult<bool> {
            if df.height() == 0 {
                return Ok(false);
            }
            match sender.send(df) {
                Ok(_) => Ok(false),
                Err(_) => Ok(true),
            }
        };

        lf.sink_batches(
            PlanCallback::new(callback),
            true,
            NonZeroUsize::new(batch_size),
        )?
        .collect()?;

        Ok(())
    }

    fn load_next_batch(&mut self) -> bool {
        match self.receiver.recv() {
            Ok(mut df) if df.height() > 0 => {
                // Strip old ___row_id — will be regenerated with 0..n-1 during flush
                let _ = df.drop_in_place(ROW_ID_COLUMN_NAME);
                self.current_batch = Some(df);
                self.row_idx = 0;
                true
            }
            _ => {
                self.current_batch = None;
                false
            }
        }
    }

    fn current_sort_value(&self) -> PolarsResult<i64> {
        match &self.current_batch {
            Some(batch) => get_sort_value(batch, self.row_idx, &self.sort_column),
            None => Err(PolarsError::NoData("Cursor exhausted".into())),
        }
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
            return Ok(self.load_next_batch());
        }

        Ok(true)
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
// JNI Entry Points
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

    let result = merge_streaming(&input_files_vec, &output_path, &sort_col);

    match result {
        Ok(_) => 0,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", format!("{:?}", e));
            -1
        }
    }
}

// =============================================================================
// STREAMING K-WAY MERGE (Ascending)
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
    unsafe {
        std::env::set_var("POLARS_AUTO_NEW_STREAMING", "1");
    }

    if input_files.is_empty() {
        return Ok(());
    }

    println!("Starting streaming merge of {} files", input_files.len());

    // Create output directory if needed
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| PolarsError::IO {
                error: e.into(),
                msg: None,
            })?;
        }
    }

    // Initialize cursors
    let mut cursors: Vec<FileCursor> = Vec::with_capacity(input_files.len());

    for (file_id, path) in input_files.iter().enumerate() {
        println!("Initializing cursor for file {}: {}", file_id, path);
        match FileCursor::new(path.clone(), file_id, sort_column.to_string(), batch_size)? {
            Some(cursor) => cursors.push(cursor),
            None => eprintln!("Skipping empty file: {}", path),
        }
    }

    if cursors.is_empty() {
        println!("All files were empty");
        return Ok(());
    }

    // Validate sort column exists in every cursor
    for (i, cursor) in cursors.iter().enumerate() {
        let batch = cursor.current_batch.as_ref().unwrap();
        let schema = batch.schema();

        if !schema.contains(sort_column) {
            let available_cols: Vec<String> = schema
                .iter()
                .map(|(name, _)| name.to_string())
                .collect();
            return Err(PolarsError::ColumnNotFound(
                format!(
                    "Sort column '{}' not found in file {} (cursor {}). Available columns: {:?}",
                    sort_column,
                    input_files.get(cursor.file_id).unwrap_or(&"unknown".to_string()),
                    i,
                    available_cols
                )
                .into(),
            ));
        }
    }

    // Schema WITHOUT ___row_id (already stripped in load_next_batch)
    let base_schema = cursors[0]
        .current_batch
        .as_ref()
        .unwrap()
        .schema()
        .as_ref()
        .clone();

    // Schema WITH ___row_id for output
    let mut output_schema = base_schema.clone();
    output_schema.insert(ROW_ID_COLUMN_NAME.into(), DataType::Int64);

    println!("Initialized {} cursors, starting merge", cursors.len());

    // =========================================================================
    // Open a SINGLE batched ParquetWriter
    // =========================================================================
    let output_file = File::create(output_path).map_err(|e| PolarsError::IO {
        error: e.into(),
        msg: None,
    })?;

    let mut writer = ParquetWriter::new(output_file)
        .with_compression(ParquetCompression::Zstd(None))
        .with_row_group_size(Some(output_flush_rows))
        .batched(&output_schema)?;

    // Initialize min-heap
    let mut heap: BinaryHeap<HeapItem> = BinaryHeap::with_capacity(cursors.len());

    for cursor in &cursors {
        if !cursor.is_exhausted() {
            heap.push(HeapItem {
                sort_value: cursor.current_sort_value()?,
                file_id: cursor.file_id,
            });
        }
    }

    // Output buffer
    let mut output_chunks: Vec<DataFrame> = Vec::with_capacity(128);
    let mut output_row_count = 0usize;
    let mut next_row_id = 0i64;
    let mut total_rows_written = 0usize;

    let sort_col_owned = sort_column.to_string();

    // =========================================================================
    // Flush helper — writes buffered chunks as one Parquet row group
    // =========================================================================
    macro_rules! flush {
        () => {
            if !output_chunks.is_empty() {
                let mut df = concat_df(output_chunks.as_slice())?;
                output_chunks.clear(); // free early before rechunk
                df.rechunk_mut();
                let n = df.height();

                // Add ___row_id: sequential 0..n-1 across entire merged file
                let row_ids: Vec<i64> = (next_row_id..next_row_id + n as i64).collect();
                let row_id_series = Series::new(ROW_ID_COLUMN_NAME.into(), row_ids);
                df.with_column(row_id_series)?;

                writer.write_batch(&df)?;
                next_row_id += n as i64;
                total_rows_written += output_row_count;
                output_row_count = 0;
            }
        };
    }

    // =========================================================================
    // K-way merge loop — drains SLICES, not single rows
    // =========================================================================
    while let Some(item) = heap.pop() {
        let cursor = &mut cursors[item.file_id];

        loop {
            let run_start = cursor.row_idx;
            let batch_height = cursor.current_batch.as_ref().unwrap().height();

            while cursor.row_idx + 1 < batch_height {
                let next_val = get_sort_value(
                    cursor.current_batch.as_ref().unwrap(),
                    cursor.row_idx + 1,
                    &sort_col_owned,
                )?;

                match heap.peek() {
                    Some(top) if next_val <= top.sort_value => {
                        cursor.row_idx += 1;
                    }
                    _ => break,
                }
            }

            let run_len = cursor.row_idx - run_start + 1;

            let slice = cursor.take_slice(run_start, run_len);
            output_chunks.push(slice);
            output_row_count += run_len;

            // Flush if buffer full
            if output_row_count >= output_flush_rows {
                flush!();
            }

            if !cursor.advance()? {
                break;
            }

            let next_val = cursor.current_sort_value()?;
            match heap.peek() {
                Some(top) if next_val <= top.sort_value => {
                    continue;
                }
                _ => {
                    heap.push(HeapItem {
                        sort_value: next_val,
                        file_id: cursor.file_id,
                    });
                    break;
                }
            }
        }
    }

    // Flush remaining rows
    flush!();

    // Write Parquet footer & close
    writer.finish()?;

    println!("Merge complete: {} total rows written", total_rows_written);

    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

fn get_sort_value(df: &DataFrame, row: usize, col: &str) -> PolarsResult<i64> {
    let series = df.column(col)?;

    match series.dtype() {
        DataType::Int64 => {
            series
                .i64()?
                .get(row)
                .ok_or_else(|| PolarsError::NoData("Null value in sort column".into()))
        }
        DataType::Int32 => {
            series
                .i32()?
                .get(row)
                .map(|v| v as i64)
                .ok_or_else(|| PolarsError::NoData("Null value in sort column".into()))
        }
        DataType::UInt64 => {
            series
                .u64()?
                .get(row)
                .map(|v| v as i64)
                .ok_or_else(|| PolarsError::NoData("Null value in sort column".into()))
        }
        DataType::UInt32 => {
            series
                .u32()?
                .get(row)
                .map(|v| v as i64)
                .ok_or_else(|| PolarsError::NoData("Null value in sort column".into()))
        }
        DataType::Datetime(_, _) => {
            series
                .datetime()?
                .phys
                .get(row)
                .ok_or_else(|| PolarsError::NoData("Null value in sort column".into()))
        }
        DataType::Date => {
            series
                .date()?
                .phys
                .get(row)
                .map(|v| v as i64)
                .ok_or_else(|| PolarsError::NoData("Null value in sort column".into()))
        }
        DataType::Duration(_) => {
            series
                .duration()?
                .phys
                .get(row)
                .ok_or_else(|| PolarsError::NoData("Null value in sort column".into()))
        }
        other => Err(PolarsError::InvalidOperation(
            format!(
                "Unsupported sort column type: {:?}. Supported: Int32, Int64, UInt32, UInt64, Datetime, Date, Duration",
                other
            )
            .into(),
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
        let obj = env
            .call_method(&list, "get", "(I)Ljava/lang/Object;", &[(i as i32).into()])?
            .l()?;

        let jstring = env
            .call_method(&obj, "toString", "()Ljava/lang/String;", &[])?
            .l()?;

        let rust_string: String = env.get_string(&jstring.into())?.into();
        result.push(rust_string);
    }

    Ok(result)
}
