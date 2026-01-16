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

const ROW_ID_COLUMN_NAME: &str = "___row_id";
const BATCH_SIZE: usize = 100000;

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
            // For descending order (reverse sort)
            self.sort_value.cmp(&other.sort_value)
        } else {
            // For ascending order (normal sort)
            other.sort_value.cmp(&self.sort_value)
        }
    }
}

impl PartialOrd for DataFrameItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

fn merge_with_priority_queue(input_files: &[String], output_path: &str, sort_column: &str, reverse_sort: bool) -> PolarsResult<()> {
    // Load all files as DataFrames
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

    // Initialize priority queue with first row from each file
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

    while let Some(item) = heap.pop() {
        current_batch_indices.push((item.file_index, item.row_index));

        // Add next row from same file to heap
        row_indices[item.file_index] += 1;
        let next_row = row_indices[item.file_index];
        if next_row < dataframes[item.file_index].height() {
            let sort_value = get_sort_value(&dataframes[item.file_index], next_row, sort_column)?;
            heap.push(DataFrameItem {
                sort_value,
                file_index: item.file_index,
                row_index: next_row,
                reverse_sort,
            });
        }

        // Write batch when full
        if current_batch_indices.len() >= BATCH_SIZE {
            let batch_df = create_batch_from_indices(&dataframes, &current_batch_indices, current_row_id)?;
            let temp_path = format!("{}.batch_{}", output_path, batch_count);
            write_batch_to_file(&batch_df, &temp_path)?;
            temp_files.push(temp_path);

            current_row_id += current_batch_indices.len() as i64;
            current_batch_indices.clear();
            batch_count += 1;
        }
    }

    // Write remaining rows
    if !current_batch_indices.is_empty() {
        let batch_df = create_batch_from_indices(&dataframes, &current_batch_indices, current_row_id)?;
        let temp_path = format!("{}.batch_{}", output_path, batch_count);
        write_batch_to_file(&batch_df, &temp_path)?;
        temp_files.push(temp_path);
    }

    // Concatenate temp files to final output
    concatenate_temp_files(&temp_files, output_path)?;

    // // Clean up temp files
    for temp_file in temp_files {
        let _ = std::fs::remove_file(temp_file);
    }

    Ok(())
}

fn get_sort_value(df: &DataFrame, row_idx: usize, sort_column: &str) -> PolarsResult<i64> {
    let series = df.column(sort_column)?;

    // Handle different data types for sorting
    match series.dtype() {
        DataType::Int64 => {
            series.i64()?.get(row_idx)
                .ok_or_else(|| PolarsError::InvalidOperation("Sort value not found".into()))
        }
        DataType::Datetime(_, _) => {
            // Convert datetime to timestamp (milliseconds since epoch)
            series.datetime()?.phys.get(row_idx)
                .ok_or_else(|| PolarsError::InvalidOperation("Sort value not found".into()))
        }
        DataType::Date => {
            // Convert date to days since epoch
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
