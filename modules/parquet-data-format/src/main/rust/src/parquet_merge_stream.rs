// src/parquet_merge_stream.rs

use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::error::Error;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use polars::prelude::*;
use polars::datatypes::DataType;
use polars_core::utils::concat_df;

const ROW_ID_COLUMN_NAME: &str = "__row_id";
const BATCH_SIZE: usize = 50_000;
const OUTPUT_FLUSH_ROWS: usize = 10_000;

/// Cursor over one parquet file (streaming)
struct FileCursor {
    batches: std::vec::IntoIter<DataFrame>,
    batch: DataFrame,
    row_idx: usize,
    file_id: usize,
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
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesInRust11(
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
            let _ = env.throw_new("java/lang/RuntimeException", format!("{:?}", e));
            -1
        }
    }
}

/// Collect batches from a LazyFrame using sink_batches
fn collect_batches_from_lazyframe(lf: LazyFrame, batch_size: usize) -> PolarsResult<Vec<DataFrame>> {
    use std::sync::Mutex;

    let batches: Arc<Mutex<Vec<DataFrame>>> = Arc::new(Mutex::new(Vec::new()));
    let batches_clone = batches.clone();

    let callback = move |df: DataFrame| -> PolarsResult<bool> {
        batches_clone.lock().unwrap().push(df);
        Ok(false) // false = continue, true = stop early
    };

    // Execute the streaming query
    lf.sink_batches(
        PlanCallback::new(callback),
        false, // maintain_order
        NonZeroUsize::new(batch_size),
    )?
    .collect()?;

    // Extract collected batches
    let result = std::mem::take(&mut *batches.lock().unwrap());
    Ok(result)
}

/// =========================
/// STREAMING K-WAY MERGE
/// =========================
pub fn merge_streaming(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
) -> PolarsResult<()> {
    let mut cursors = Vec::new();

    // -----------------------------
    // Init cursors (streaming read)
    // -----------------------------
    for (file_id, path) in input_files.iter().enumerate() {
        let lf = LazyFrame::scan_parquet(
            PlPath::Local(Arc::from(Path::new(path))),
            ScanArgsParquet::default(),
        )?;

        let all_batches = collect_batches_from_lazyframe(lf, BATCH_SIZE)?;
        let mut batches = all_batches.into_iter();

        let first_batch = match batches.next() {
            Some(b) => b,
            None => continue,
        };

        cursors.push(FileCursor {
            batches,
            batch: first_batch,
            row_idx: 0,
            file_id,
        });
    }

    let mut heap = BinaryHeap::new();

    for c in &cursors {
        let v = get_sort_value(&c.batch, c.row_idx, sort_column)?;
        heap.push(HeapItem {
            sort_value: v,
            file_id: c.file_id,
        });
    }

    let mut output_chunks = Vec::new();
    let mut next_row_id = 0i64;

    // -----------------------------
    // K-way merge
    // -----------------------------
    while let Some(item) = heap.pop() {
        let cursor = &mut cursors[item.file_id];

        loop {
            let row = cursor.batch.slice(cursor.row_idx as i64, 1);
            output_chunks.push(row);
            cursor.row_idx += 1;

            if cursor.row_idx >= cursor.batch.height() {
                match cursor.batches.next() {
                    Some(b) => {
                        cursor.batch = b;
                        cursor.row_idx = 0;
                    }
                    None => break,
                }
            }

            let next_val = get_sort_value(&cursor.batch, cursor.row_idx, sort_column)?;

            match heap.peek() {
                Some(top) if next_val <= top.sort_value => continue,
                _ => {
                    heap.push(HeapItem {
                        sort_value: next_val,
                        file_id: cursor.file_id,
                    });
                    break;
                }
            }
        }

        if output_chunks.len() >= OUTPUT_FLUSH_ROWS {
            flush(&output_chunks, output_path, &mut next_row_id)?;
            output_chunks.clear();
        }
    }

    if !output_chunks.is_empty() {
        flush(&output_chunks, output_path, &mut next_row_id)?;
    }

    Ok(())
}

/// -----------------------------
/// Helpers
/// -----------------------------
fn flush(
    chunks: &[DataFrame],
    path: &str,
    row_id: &mut i64,
) -> PolarsResult<()> {
    let mut df = concat_df(chunks)?;
    let n = df.height();

    let row_ids = Series::new(
        ROW_ID_COLUMN_NAME.into(), // Convert &str to PlSmallStr
        (*row_id..*row_id + n as i64).collect::<Vec<_>>(),
    );
    df.with_column(row_ids)?;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;

    ParquetWriter::new(&mut file).finish(&mut df)?;
    *row_id += n as i64;
    Ok(())
}

fn get_sort_value(df: &DataFrame, row: usize, col: &str) -> PolarsResult<i64> {
    let s = df.column(col)?;
    Ok(match s.dtype() {
        DataType::Int64 => s.i64()?.get(row).unwrap(),
        DataType::Datetime(_, _) => s.datetime()?.phys.get(row).unwrap(),
        _ => return Err(PolarsError::InvalidOperation("unsupported sort type".into())),
    })
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
