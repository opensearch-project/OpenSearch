/*
 * SPDX-License-Identifier: Apache-2.0
 */

use anyhow::Result;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatch;
use datafusion::execution::cache::cache_manager::FileMetadata;
use jni::objects::{GlobalRef, JObject, JObjectArray, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use object_store::{path::Path as ObjectPath, ObjectMeta, ObjectStore};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use datafusion::error::DataFusionError;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use crate::CustomFileMeta;
use std::sync::Arc;
use datafusion::datasource::physical_plan::parquet::CachedParquetMetaData;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;

/// Set error message from a result using a Consumer<String> Java callback
pub fn set_error_message_batch<Err: Error>(
    env: &mut JNIEnv,
    callback: JObject,
    result: Result<Vec<RecordBatch>, Err>,
) {
    if result.is_err() {
        set_error_message(env, callback, Result::Err(result.unwrap_err()));
    } else {
        let res: Result<(), Err> = Result::Ok(());
        set_error_message(env, callback, res);
    }

}

pub fn set_error_message<Err: Error>(env: &mut JNIEnv, callback: JObject, result: Result<(), Err>) {
    match result {
        Ok(_) => {
            let err_message = JObject::null();
            env.call_method(
                callback,
                "accept",
                "(Ljava/lang/Object;)V",
                &[(&err_message).into()],
            )
            .expect("Failed to call error handler with null message");
        }
        Err(err) => {
            let err_message = env
                .new_string(err.to_string())
                .expect("Couldn't create java string for error message");
            env.call_method(
                callback,
                "accept",
                "(Ljava/lang/Object;)V",
                &[(&err_message).into()],
            )
            .expect("Failed to call error handler with error message");
        }
    };
}

/// Parse a string map from JNI arrays
pub fn parse_string_map(
    env: &mut JNIEnv,
    keys: JObjectArray,
    values: JObjectArray,
) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();

    let keys_len = env.get_array_length(&keys)?;
    let values_len = env.get_array_length(&values)?;

    if keys_len != values_len {
        return Err(anyhow::anyhow!(
            "Keys and values arrays must have the same length"
        ));
    }

    for i in 0..keys_len {
        let key_obj = env.get_object_array_element(&keys, i)?;
        let value_obj = env.get_object_array_element(&values, i)?;

        let key_jstring = JString::from(key_obj);
        let value_jstring = JString::from(value_obj);

        let key_str = env.get_string(&key_jstring)?;
        let value_str = env.get_string(&value_jstring)?;

        map.insert(
            key_str.to_string_lossy().to_string(),
            value_str.to_string_lossy().to_string(),
        );
    }

    Ok(map)
}

// Parse a string map from JNI arrays
pub fn parse_string_arr(env: &mut JNIEnv, files: JObjectArray) -> Result<Vec<String>> {
    let length = env.get_array_length(&files).unwrap();
    let mut rust_strings: Vec<String> = Vec::with_capacity(length as usize);
    for i in 0..length {
        let file_obj = env.get_object_array_element(&files, i).unwrap();
        let jstring = JString::from(file_obj);
        let rust_str: String = env
            .get_string(&jstring)
            .expect("Couldn't get java string!")
            .into();
        rust_strings.push(rust_str);
    }
    Ok(rust_strings)
}

pub fn parse_string(env: &mut JNIEnv, file: JString) -> Result<String> {
    let rust_str: String = env
        .get_string(&file)
        .expect("Couldn't get java string")
        .into();

    Ok(rust_str)
}

/// Throw a Java exception
pub fn throw_exception(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new("java/lang/RuntimeException", message);
}

pub fn create_file_meta_from_filenames(
    base_path: &str,
    filenames: Vec<String>,
) -> Result<Vec<CustomFileMeta>, DataFusionError> {
    let mut row_base: i64 = 0;
    filenames
        .into_iter()
        .map(|filename| {
            let filename = filename.as_str();

            // Handle both full paths and relative filenames
            let full_path = if filename.starts_with('/') || filename.contains(base_path) {
                // Already a full path
                filename.to_string()
            } else {
                // Just a filename, needs base_path
                format!("{}/{}", base_path.trim_end_matches('/'), filename)
            };

            let file_size = fs::metadata(&full_path).map(|m| m.len()).unwrap_or(0);
            let file_result = fs::File::open(&full_path.clone());
            if (file_result.is_err()) {
                return Err(DataFusionError::Execution(format!(
                    "{} {}",
                    file_result.unwrap_err().to_string(),
                    full_path
                )));
            }
            let file = file_result.unwrap();
            let parquet_metadata = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let row_group_row_counts: Vec<i64> = parquet_metadata
                .metadata()
                .row_groups()
                .iter()
                .map(|row_group| row_group.num_rows())
                .collect();

            let modified = fs::metadata(&full_path)
                .and_then(|m| m.modified())
                .map(|t| DateTime::<Utc>::from(t))
                .unwrap_or_else(|_| Utc::now());

            let file_meta = CustomFileMeta::new(
                row_group_row_counts.clone(),
                row_base,
                ObjectMeta {
                    location: ObjectPath::from(full_path),
                    last_modified: modified,
                    size: file_size,
                    e_tag: None,
                    version: None,
                },
            );
            //TODO: ensure ordering of files
            row_base += row_group_row_counts.iter().sum::<i64>();
            Ok(file_meta)
        })
        .collect()
}

pub fn create_object_meta_from_file(file_path: &str) -> Result<Vec<ObjectMeta>, DataFusionError> {
    let file_size = fs::metadata(&file_path)
        .map(|m| m.len())
        .map_err(|e| DataFusionError::Execution(format!("Failed to get file metadata for {}: {}", file_path, e)))?;

    let modified = fs::metadata(&file_path)
        .and_then(|m| m.modified())
        .map(|t| DateTime::<Utc>::from(t))
        .unwrap_or_else(|_| Utc::now());

    let object_meta = ObjectMeta {
        location: ObjectPath::from(file_path),
        last_modified: modified,
        size: file_size,
        e_tag: None,
        version: None,
    };

    Ok(vec![object_meta])
}

/// Set success result by calling an ActionListener
pub fn set_action_listener_ok(env: &mut JNIEnv, listener: JObject, value: jlong) {
    let long_obj = env.new_object("java/lang/Long", "(J)V", &[value.into()])
        .expect("Failed to create Long object");
    
    env.call_method(
        listener,
        "onResponse",
        "(Ljava/lang/Object;)V",
        &[(&long_obj).into()],
    )
    .expect("Failed to call ActionListener onResponse");
}

/// Set error result by calling an ActionListener
pub fn set_action_listener_error<T: Error>(env: &mut JNIEnv, listener: JObject, error: &T) {
    let error_msg = env.new_string(error.to_string())
        .expect("Failed to create error string");
    let exception = env.new_object(
        "java/lang/RuntimeException",
        "(Ljava/lang/String;)V",
        &[(&error_msg).into()],
    ).expect("Failed to create exception");
    
    env.call_method(
        listener,
        "onFailure",
        "(Ljava/lang/Exception;)V",
        &[(&exception).into()],
    )
    .expect("Failed to call ActionListener onFailure");
}

/// Set success result by calling an ActionListener with GlobalRef
pub fn set_action_listener_ok_global(env: &mut JNIEnv, listener: &GlobalRef, value: jlong) {
    let long_obj = env.new_object("java/lang/Long", "(J)V", &[value.into()])
        .expect("Failed to create Long object");
    
    env.call_method(
        listener.as_obj(),
        "onResponse",
        "(Ljava/lang/Object;)V",
        &[(&long_obj).into()],
    )
    .expect("Failed to call ActionListener onResponse");
}

/// Set error result by calling an ActionListener with GlobalRef
pub fn set_action_listener_error_global<T: Error>(env: &mut JNIEnv, listener: &GlobalRef, error: &T) {
    let error_msg = env.new_string(error.to_string())
        .expect("Failed to create error string");
    let exception = env.new_object(
        "java/lang/RuntimeException",
        "(Ljava/lang/String;)V",
        &[(&error_msg).into()],
    ).expect("Failed to create exception");
    
    env.call_method(
        listener.as_obj(),
        "onFailure",
        "(Ljava/lang/Exception;)V",
        &[(&exception).into()],
    )
    .expect("Failed to call ActionListener onFailure");
}
