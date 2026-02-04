use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use std::error::Error;
use std::path::Path;
use std::collections::BinaryHeap;
use std::cmp::Ordering;
use polars::prelude::*;
use polars_core::utils::concat_df;
use std::sync::Arc;

const ROW_ID_COLUMN_NAME: &str = "__row_id";
const BATCH_SIZE: usize = 1_00_000;

#[derive(Debug)]
struct DataFrameItem {
    sort_value: i64,
    file_index: usize,
    row_index: usize,
    reverse_sort: bool,
}

impl Eq for DataFrameItem {}

impl PartialEq for DataFrameItem {
    fn eq(&self, other: &Self) -> bool {
        self.sort_value == other.sort_value
    }
}

impl Ord for DataFrameItem {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.reverse_sort {
            // descending
            self.sort_value.cmp(&other.sort_value)
        } else {
            // ascending
            other.sort_value.cmp(&self.sort_value)
        }
    }
}

impl PartialOrd for DataFrameItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct RowRange {
    file_idx: usize,
    start: usize,
    end: usize, // exclusive
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesInRust(
    mut env: JNIEnv,
    _class: JClass,
    input_files: JObject,
    output_file: JString,
    sort_column: JString,
    is_reverse: jint,
) -> jint {
    // Debug: Check each parameter individually
    if input_files.is_null() {
        let _ = env.throw_new("java/lang/RuntimeException", "Input files parameter is null");
        return -1;
    }

    if output_file.is_null() {
        let _ = env.throw_new("java/lang/RuntimeException", "Output file parameter is null");
        return -1;
    }

    if sort_column.is_null() {
        let _ = env.throw_new("java/lang/RuntimeException", "Sort column parameter is null");
        return -1;
    }

    let input_files_vec = match convert_java_list_to_vec(&mut env, input_files) {
        Ok(vec) if !vec.is_empty() => vec,
        Ok(_) => {
            let _ = env.throw_new("java/lang/RuntimeException", "Input files list is empty");
            return -1;
        }
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("Failed to convert input files: {:?}", e));
            return -1;
        }
    };

    let output_path: String = match env.get_string(&output_file) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("Invalid output path: {:?}", e));
            return -1;
        }
    };

    let sort_column_name: String = match env.get_string(&sort_column) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("Invalid sort column: {:?}", e));
            return -1;
        }
    };

    let reverse_sort = is_reverse != 0;

    match merge_with_priority_queue(&input_files_vec, &output_path, &sort_column_name, reverse_sort) {
        Ok(_) => 0,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("Merge failed: {:?}", e));
            -1
        }
    }
}

pub fn merge_with_priority_queue(
    input_files: &[String],
    output_path: &str,
    sort_column: &str,
    reverse_sort: bool,
) -> PolarsResult<()> {
    let mut dataframes: Vec<DataFrame> = Vec::new();
    for file_path in input_files {
        let df = LazyFrame::scan_parquet(
            PlPath::Local(Arc::from(Path::new(file_path))),
            ScanArgsParquet {
                use_statistics: false,
                ..Default::default()
            }
        )?.collect()?;
        dataframes.push(df);
    }

    let mut heap = BinaryHeap::new();
    let mut row_indices = vec![0usize; dataframes.len()];

    for (file_idx, df) in dataframes.iter().enumerate() {
        if df.height() > 0 {
            let sort_value = get_sort_value(df, 0, sort_column)?;
            heap.push(DataFrameItem {
                sort_value,
                file_index: file_idx,
                row_index: 0,
                reverse_sort,
            });
        }
    }

    let mut temp_files = Vec::new();
    let mut current_batch_indices = Vec::new();
    let mut current_row_id = 0i64;
    let mut batch_count = 0;
    let mut current_batch_rows: usize = 0;
    while let Some(item) = heap.pop() {
        let start_row = item.row_index;
        let df = &dataframes[item.file_index];
        let mut end_row = start_row + 1;

        let heap_top_sort_value = heap.peek()
            .map_or(if reverse_sort { i64::MIN } else { i64::MAX },
                    |top| top.sort_value);

        // Collect contiguous rows from same file as long as global ordering is safe
        while end_row < df.height() {
            let next_sort_value = get_sort_value(df, end_row, sort_column)?;

            let take = if reverse_sort {
                next_sort_value >= heap_top_sort_value
            } else {
                next_sort_value <= heap_top_sort_value
            };

            if take {
                end_row += 1;
            } else {
                break;
            }
        }

        current_batch_indices.push(RowRange {
            file_idx: item.file_index,
            start: start_row,
            end: end_row,
        });

        // Update row index and push next unprocessed row
        row_indices[item.file_index] = end_row;
        if end_row < df.height() {
            let sort_value = get_sort_value(df, end_row, sort_column)?;
            heap.push(DataFrameItem {
                sort_value,
                file_index: item.file_index,
                row_index: end_row,
                reverse_sort,
            });
        }

        // Write batch if full
        let rows_added = end_row - start_row;
        current_batch_rows += rows_added;
        if current_batch_rows >= BATCH_SIZE {
            let batch_df = create_batch_from_ranges(&dataframes, &current_batch_indices, current_row_id)?;
            let temp_path = format!("{}.batch_{}", output_path, batch_count);
            write_batch_to_file(&batch_df, &temp_path)?;
            temp_files.push(temp_path);

            current_row_id += current_batch_rows as i64;
            current_batch_indices.clear();
            current_batch_rows = 0;
            batch_count += 1;
        }
    }

    let total_rows: usize = current_batch_indices.iter().map(|r| r.end - r.start).sum();
    // Write remaining rows
    if !current_batch_indices.is_empty() {
        let batch_df = create_batch_from_ranges(&dataframes, &current_batch_indices, current_row_id)?;
        let temp_path = format!("{}.batch_{}", output_path, batch_count);
        write_batch_to_file(&batch_df, &temp_path)?;
        temp_files.push(temp_path);
    }

    concatenate_temp_files(&temp_files, output_path)?;

    // Clean up temp files
    for temp_file in temp_files {
        let _ = std::fs::remove_file(temp_file);
    }

    Ok(())
}

fn get_sort_value(df: &DataFrame, row_idx: usize, sort_column: &str) -> PolarsResult<i64> {
    let series = df.column(sort_column)?;

    match series.dtype() {
        DataType::Int64 => {
            series.i64()?.get(row_idx)
                .ok_or_else(|| PolarsError::InvalidOperation("Sort value not found".into()))
        }
        DataType::Datetime(_, _) => {
            series.datetime()?.phys.get(row_idx)
                .ok_or_else(|| PolarsError::InvalidOperation("Sort value not found".into()))
        }
        DataType::Date => {
            series.date()?.phys.get(row_idx)
                .map(|d| d as i64)
                .ok_or_else(|| PolarsError::InvalidOperation("Sort value not found".into()))
        }
        _ => {
            Err(PolarsError::InvalidOperation(
                format!("Unsupported sort column type: {:?}", series.dtype()).into()
            ))
        }
    }
}

fn create_batch_from_ranges(
    dataframes: &[DataFrame],
    ranges: &[RowRange],
    start_row_id: i64,
) -> PolarsResult<DataFrame> {
    let mut slices = Vec::with_capacity(ranges.len());

    for r in ranges {
        let slice = dataframes[r.file_idx]
            .slice(r.start as i64, r.end - r.start);
        slices.push(slice);
    }

    let mut batch = concat_df(&slices)?;

    let num_rows = batch.height();
    let row_ids = Series::new(
        ROW_ID_COLUMN_NAME.into(),
        (start_row_id..start_row_id + num_rows as i64).collect::<Vec<_>>(),
    );
    batch.replace(ROW_ID_COLUMN_NAME, row_ids)?;

    Ok(batch)
}

fn create_batch_from_indices(
    dataframes: &[DataFrame],
    indices: &[(usize, usize)],
    start_row_id: i64,
) -> PolarsResult<DataFrame> {
    let mut rows = Vec::new();
    for &(file_idx, row_idx) in indices {
        rows.push(dataframes[file_idx].slice(row_idx as i64, 1));
    }

    let mut batch = concat_df(&rows)?;

    // Update row IDs
    let num_rows = batch.height();
    let new_row_ids = Series::new(
        ROW_ID_COLUMN_NAME.into(),
        (start_row_id..start_row_id + num_rows as i64).collect::<Vec<i64>>()
    );
    batch.replace(ROW_ID_COLUMN_NAME, new_row_ids)?;

    Ok(batch)
}

fn write_batch_to_file(df: &DataFrame, path: &str) -> PolarsResult<()> {
    let mut file = std::fs::File::create(path)
        .map_err(|e| PolarsError::InvalidOperation(format!("Failed to create temp file: {}", e).into()))?;
    ParquetWriter::new(&mut file).finish(&mut df.clone())?;
    Ok(())
}

fn concatenate_temp_files(temp_files: &[String], output_path: &str) -> PolarsResult<()> {
    if temp_files.is_empty() {
        return Ok(());
    }

    let mut lazy_frames = Vec::new();
    for temp_file in temp_files {
        let lf = LazyFrame::scan_parquet(
            PlPath::Local(Arc::from(Path::new(temp_file))),
            ScanArgsParquet::default()
        )?;
        lazy_frames.push(lf);
    }

    let combined_df = concat(&lazy_frames, UnionArgs::default())?;
    let mut file = std::fs::File::create(output_path)
        .map_err(|e| PolarsError::InvalidOperation(format!("Failed to create output file: {}", e).into()))?;
    ParquetWriter::new(&mut file).finish(&mut combined_df.collect()?)?;
    Ok(())
}

fn convert_java_list_to_vec(env: &mut JNIEnv, list: JObject) -> Result<Vec<String>, Box<dyn Error>> {
    let list_size = env.call_method(&list, "size", "()I", &[])?.i()? as usize;
    let mut result = Vec::with_capacity(list_size);

    for i in 0..list_size {
        let item = env.call_method(&list, "get", "(I)Ljava/lang/Object;", &[(i as i32).into()])?.l()?;
        if !item.is_null() {
            let jstring = env.call_method(&item, "toString", "()Ljava/lang/String;", &[])?.l()?;
            result.push(env.get_string(&jstring.into())?.into());
        }
    }

    Ok(result)
}
