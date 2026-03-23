/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong, jobject};
use jni::JNIEnv;
use parquet::format::FileMetaData as FormatFileMetaData;

use crate::log_error;
use crate::writer::NativeParquetWriter;

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_initLogger(
    env: JNIEnv,
    _class: JClass,
) {
    native_bridge_spi::init_logger_from_env(&env);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_createWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    schema_address: jlong,
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::create_writer(filename, schema_address as i64) {
        Ok(_) => 0,
        Err(e) => {
            log_error!("ERROR: Failed to create writer: {:?}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_write(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    array_address: jlong,
    schema_address: jlong,
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::write_data(filename, array_address as i64, schema_address as i64) {
        Ok(_) => 0,
        Err(e) => {
            log_error!("ERROR: Failed to write data: {:?}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_closeWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::close_writer(filename) {
        Ok(Some(metadata)) => {
            match create_java_metadata(&mut env, &metadata) {
                Ok(java_obj) => java_obj.into_raw(),
                Err(e) => {
                    log_error!("ERROR: Failed to create Java metadata object: {:?}", e);
                    let _ = env.throw_new("java/io/IOException", "Failed to create metadata object");
                    JObject::null().into_raw()
                }
            }
        }
        Ok(None) => JObject::null().into_raw(),
        Err(e) => {
            log_error!("ERROR: Failed to close writer: {:?}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to close writer: {}", e));
            JObject::null().into_raw()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_flushToDisk(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
) -> jint {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::flush_to_disk(filename) {
        Ok(_) => 0,
        Err(e) => {
            log_error!("ERROR: Failed to flush to disk: {:?}", e);
            -1
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_getFilteredNativeBytesUsed(
    mut env: JNIEnv,
    _class: JClass,
    path_prefix: JString,
) -> jlong {
    let prefix: String = env.get_string(&path_prefix).expect("Couldn't get java string!").into();
    match NativeParquetWriter::get_filtered_writer_memory_usage(prefix) {
        Ok(memory) => memory as jlong,
        Err(e) => {
            log_error!("ERROR: Failed to get filtered native bytes used: {:?}", e);
            0
        }
    }
}

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

fn create_java_metadata<'local>(env: &mut JNIEnv<'local>, metadata: &FormatFileMetaData) -> Result<JObject<'local>, Box<dyn std::error::Error>> {
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
