/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jlong, jobject};
use jni::JNIEnv;
use parquet::format::FileMetaData as FormatFileMetaData;

use crate::log_error;
use crate::object_store_handle;
use crate::writer::{self, NativeParquetWriter};

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    native_bridge_spi::init_logger_from_env(&env);
}

// ---------------------------------------------------------------------------
// Object Store lifecycle
// ---------------------------------------------------------------------------

/// Creates a native ObjectStore and returns an opaque handle.
/// `store_type`: "local", "s3", "gcs", "azure"
/// `config_json`: JSON string with backend-specific configuration.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_createObjectStore(
    mut env: JNIEnv,
    _class: JClass,
    store_type: JString,
    config_json: JString,
) -> jlong {
    let st: String = env.get_string(&store_type).expect("Couldn't get store_type string").into();
    let cfg: String = env.get_string(&config_json).expect("Couldn't get config_json string").into();
    match object_store_handle::create_object_store(&st, &cfg) {
        Ok(store) => object_store_handle::store_to_handle(store),
        Err(e) => {
            log_error!("ERROR: Failed to create object store: {:?}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to create object store: {}", e));
            0
        }
    }
}

/// Destroys a native ObjectStore, releasing its resources.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_destroyObjectStore(
    _env: JNIEnv,
    _class: JClass,
    store_handle: jlong,
) {
    unsafe { object_store_handle::drop_store(store_handle) };
}

// ---------------------------------------------------------------------------
// Writer lifecycle
// ---------------------------------------------------------------------------

/// Creates a native writer backed by the given ObjectStore handle.
/// Returns an opaque writer handle.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_createWriter(
    mut env: JNIEnv,
    _class: JClass,
    store_handle: jlong,
    path: JString,
    schema_address: jlong,
) -> jlong {
    let p: String = env.get_string(&path).expect("Couldn't get path string").into();
    let store = unsafe { object_store_handle::store_from_handle(store_handle) };
    match NativeParquetWriter::create(store, p, schema_address as i64) {
        Ok(w) => writer::writer_to_handle(w),
        Err(e) => {
            log_error!("ERROR: Failed to create writer: {:?}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to create writer: {}", e));
            0
        }
    }
}

/// Writes an Arrow batch to the writer identified by the given handle.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_write(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    array_address: jlong,
    schema_address: jlong,
) {
    let w = unsafe { writer::writer_from_handle(handle) };
    if let Err(e) = w.write_data(array_address as i64, schema_address as i64) {
        log_error!("ERROR: Failed to write data: {:?}", e);
        let _ = env.throw_new("java/io/IOException", &format!("Failed to write data: {}", e));
    }
}

/// Finalizes the writer and returns a ParquetFileMetadata Java object.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_finalizeWriter(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jobject {
    let w = unsafe { writer::writer_from_handle(handle) };
    match w.finalize_writer() {
        Ok(Some(metadata)) => match create_java_metadata(&mut env, &metadata) {
            Ok(java_obj) => java_obj.into_raw(),
            Err(e) => {
                log_error!("ERROR: Failed to create Java metadata object: {:?}", e);
                let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                JObject::null().into_raw()
            }
        },
        Ok(None) => JObject::null().into_raw(),
        Err(e) => {
            log_error!("ERROR: Failed to finalize writer: {:?}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to finalize writer: {}", e));
            JObject::null().into_raw()
        }
    }
}

/// Returns the native memory used by this writer.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_getWriterMemoryUsage(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    let w = unsafe { writer::writer_from_handle(handle) };
    w.memory_size() as jlong
}

/// Drops the native writer, freeing all Rust-side resources.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_destroyWriter(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    unsafe { writer::drop_writer(handle) };
}

// ---------------------------------------------------------------------------
// Utility (no handle needed)
// ---------------------------------------------------------------------------

/// Reads metadata from an existing Parquet file on local disk.
#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_getFileMetadata(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::get_file_metadata(filename) {
        Ok(file_metadata) => {
            let format_metadata = FormatFileMetaData {
                version: file_metadata.version(),
                num_rows: file_metadata.num_rows(),
                created_by: file_metadata.created_by().map(|s| s.to_string()),
                schema: vec![],
                row_groups: vec![],
                key_value_metadata: None,
                encryption_algorithm: None,
                footer_signing_key_metadata: None,
                column_orders: None,
            };
            match create_java_metadata(&mut env, &format_metadata) {
                Ok(java_obj) => java_obj.into_raw(),
                Err(e) => {
                    log_error!("ERROR: Failed to create Java metadata: {:?}", e);
                    let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                    JObject::null().into_raw()
                }
            }
        }
        Err(e) => {
            log_error!("ERROR: Failed to get file metadata: {:?}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to get metadata: {}", e));
            JObject::null().into_raw()
        }
    }
}

fn create_java_metadata<'local>(
    env: &mut JNIEnv<'local>,
    metadata: &FormatFileMetaData,
) -> Result<JObject<'local>, Box<dyn std::error::Error>> {
    let class = env.find_class("org/opensearch/parquet/bridge/ParquetFileMetadata")?;
    let created_by_jstring = match &metadata.created_by {
        Some(created_by) => env.new_string(created_by)?,
        None => JObject::null().into(),
    };
    let java_metadata = env.new_object(
        &class,
        "(IJLjava/lang/String;)V",
        &[
            (metadata.version).into(),
            (metadata.num_rows).into(),
            (&created_by_jstring).into(),
        ],
    )?;
    Ok(java_metadata)
}
