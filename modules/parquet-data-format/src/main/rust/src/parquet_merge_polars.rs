use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::jint;
use std::error::Error;
use std::path::Path;
use polars::prelude::*;

const ROW_ID_COLUMN_NAME: &str = "___row_id";
const SORT_COLUMN_NAME: &str = "EventDate";

#[unsafe(no_mangle)]
pub extern "system" fn Java_com_parquet_parquetdataformat_bridge_RustBridge_mergeParquetFilesInRust(
    mut env: JNIEnv,
    _class: JClass,
    input_files: JObject,
    output_file: JString,
) -> jint {
    // Add debug logging and validation
    if input_files.is_null() {
        let _ = env.throw_new("java/lang/RuntimeException", "Input files list is null");
        return -1;
    }

    if output_file.is_null() {
        let _ = env.throw_new("java/lang/RuntimeException", "Output file path is null");
        return -1;
    }

    // Extract JNI operations outside catch_unwind
    let input_files_vec = match convert_java_list_to_vec(&mut env, input_files) {
        Ok(vec) => {
            if vec.is_empty() {
                let _ = env.throw_new("java/lang/RuntimeException", "No input files provided");
                return -1;
            }
            vec
        },
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("Failed to convert input files: {:?}", e));
            return -1;
        }
    };

    let output_path: String = match env.get_string(&output_file) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("Failed to get output path: {:?}", e));
            return -1;
        }
    };

    match merge_parquet_files_sorted(&input_files_vec, &output_path) {
        Ok(_) => 0,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", &format!("{:?}", e));
            -1
        }
    }
}

fn merge_parquet_files_sorted(input_files: &[String], output_path: &str) -> PolarsResult<()> {
    if input_files.is_empty() {
        return Err(PolarsError::InvalidOperation("No input files".into()));
    }

    // Simplified approach: collect and write
    let mut lazy_frames = Vec::new();

    for file_path in input_files {
        let lf = LazyFrame::scan_parquet(
            PlPath::Local(Arc::from(Path::new(file_path))),
            ScanArgsParquet {
                use_statistics: false,
                ..Default::default()
            }
        )?;
        // .with_row_index("tmpRowIndex", None);
        lazy_frames.push(lf);
    }

    let mut merged_df = concat(lazy_frames, UnionArgs::default())?
        .sort([SORT_COLUMN_NAME], SortMultipleOptions::default())
        .collect()?;

    // Update ___row_id column with sequential values
    let num_rows = merged_df.height();
    let new_row_ids = Series::new(ROW_ID_COLUMN_NAME.into(), (0..num_rows as i64).collect::<Vec<i64>>());
    merged_df.replace(ROW_ID_COLUMN_NAME, new_row_ids)?;

    // Write using simple file creation
    let mut file = std::fs::File::create(output_path)
        .map_err(|e| PolarsError::InvalidOperation(format!("Failed to create file: {}", e).into()))?;

    ParquetWriter::new(&mut file)
//     .with_statistics(StatisticsOptions::default().with_stats(false))
    .finish(&mut merged_df.clone())?;

//     // Replace the file creation and ParquetWriter with:
//     let write_options = ParquetWriteOptions::default()
//         .with_statistics(StatisticsOptions::default().with_stats(false));
//
//     merged_df.write_parquet(output_path, write_options)?;

    Ok(())
}

fn convert_java_list_to_vec(env: &mut JNIEnv, list: JObject) -> Result<Vec<String>, Box<dyn Error>> {
    if list.is_null() {
        return Err("Input list is null".into());
    }

    let list_size = env.call_method(&list, "size", "()I", &[])?.i()? as usize;
    let mut result = Vec::with_capacity(list_size);

    for i in 0..list_size {
        let item = env.call_method(&list, "get", "(I)Ljava/lang/Object;", &[(i as i32).into()])?.l()?;
        if item.is_null() {
            continue;
        }
        let jstring = env.call_method(&item, "toString", "()Ljava/lang/String;", &[])?.l()?;
        let string_item = env.get_string(&jstring.into())?.into();
        result.push(string_item);
    }

    Ok(result)
}
