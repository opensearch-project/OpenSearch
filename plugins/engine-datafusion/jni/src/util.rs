/*
 * SPDX-License-Identifier: Apache-2.0
 */

use anyhow::Result;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatch;
use jni::objects::{JObject, JObjectArray, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use object_store::{path::Path as ObjectPath, ObjectMeta, ObjectStore};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::sync::Arc;
use datafusion::datasource::physical_plan::parquet::CachedParquetMetaData;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::execution::cache::cache_manager::FileMetadata;

/// Set error message from a result using a Consumer<String> Java callback
pub fn set_error_message_batch<Err: Error>(env: &mut JNIEnv, callback: JObject, result: Result<Vec<RecordBatch>, Err>) {
    if result.is_err() {
        set_error_message(env, callback, Result::Err(result.unwrap_err()));
    } else {
        let res : Result<(), Err> = Result::Ok(());
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

/// Call an ObjectResultCallback to return either a pointer to a newly created object or an error message
pub fn set_object_result<T, Err: Error>(
    env: &mut JNIEnv,
    callback: JObject,
    address: Result<*mut T, Err>,
) {
    match address {
        Ok(address) => set_object_result_ok(env, callback, address),
        Err(err) => set_object_result_error(env, callback, &err),
    };
}

/// Set success result by calling an ObjectResultCallback
pub fn set_object_result_ok<T>(env: &mut JNIEnv, callback: JObject, address: *mut T) {
    let err_message = JObject::null();
    env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[(&err_message).into(), (address as jlong).into()],
    )
        .expect("Failed to call object result callback with address");
}

/// Set error result by calling an ObjectResultCallback
pub fn set_object_result_error<T: Error>(env: &mut JNIEnv, callback: JObject, error: &T) {
    let err_message = env
        .new_string(error.to_string())
        .expect("Couldn't create java string for error message");
    let address = -1 as jlong;
    env.call_method(
        callback,
        "callback",
        "(Ljava/lang/String;J)V",
        &[(&err_message).into(), address.into()],
    )
        .expect("Failed to call object result callback with error");
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
        return Err(anyhow::anyhow!("Keys and values arrays must have the same length"));
    }

    for i in 0..keys_len {
        let key_obj = env.get_object_array_element(&keys, i)?;
        let value_obj = env.get_object_array_element(&values, i)?;

        let key_jstring = JString::from(key_obj);
        let value_jstring = JString::from(value_obj);

        let key_str = env.get_string(&key_jstring)?;
        let value_str = env.get_string(&value_jstring)?;

        map.insert(key_str.to_string_lossy().to_string(), value_str.to_string_lossy().to_string());
    }

    Ok(map)
}

// Parse a string map from JNI arrays
pub fn parse_string_arr(
    env: &mut JNIEnv,
    files: JObjectArray,
) -> Result<Vec<String>> {
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

pub fn parse_string(
    env: &mut JNIEnv,
    file: JString
) -> Result<String> {
    let rust_str: String = env.get_string(&file)
        .expect("Couldn't get java string")
        .into();

    Ok(rust_str)
}

/// Throw a Java exception
pub fn throw_exception(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new("java/lang/RuntimeException", message);
}

pub fn create_object_meta_from_filenames(base_path: &str, filenames: Vec<String>) -> Vec<ObjectMeta> {
    filenames.into_iter().map(|filename| {
        let filename = filename.as_str();
        // Handle both full paths and relative filenames
        let full_path = if filename.starts_with('/') || filename.contains(base_path) {
            // Already a full path
            filename.to_string()
        } else {
            // Just a filename, needs base_path
            format!("{}/{}", base_path.trim_end_matches('/'), filename)
        };        create_object_meta_from_file(&full_path)
    }).collect()
}

pub fn create_object_meta_from_file(file_path: &str) -> ObjectMeta {
    let file_size = fs::metadata(&file_path).map(|m| m.len()).unwrap_or(0);
    let modified = fs::metadata(&file_path)
        .and_then(|m| m.modified())
        .map(|t| DateTime::<Utc>::from(t))
        .unwrap_or_else(|_| Utc::now());

        ObjectMeta {
            location: ObjectPath::from(file_path),
            last_modified: modified,
            size: file_size,
            e_tag: None,
            version: None,
        }
}

pub async fn construct_file_metadata(
    store: &dyn ObjectStore,
    object_meta: &ObjectMeta,
    data_format: &str,
) -> Result<Arc<dyn FileMetadata>, Box<dyn std::error::Error>> {
    match data_format.to_lowercase().as_str() {
        "parquet" => {
            let df_metadata = DFParquetMetadata::new(
                store,
                object_meta
            );

            let parquet_metadata = df_metadata.fetch_metadata().await?;
            let par = CachedParquetMetaData::new(parquet_metadata);
            Ok(Arc::new(par))
        },
        _ => Err(format!("Unsupported data format: {}", data_format).into())
    }
}
