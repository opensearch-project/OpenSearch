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
use parquet::file::metadata::FileMetaData;

use crate::{log_info, log_error};
use crate::native_settings::NativeSettings;
use crate::writer::{NativeParquetWriter, SETTINGS_STORE};

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
    index_name: JString,
    schema_address: jlong,
    sort_columns: JObject,
    reverse_sorts: JObject,
) {
    let filename: String = env.get_string(&file).expect("Couldn't get file string!").into();
    let index_name: String = env.get_string(&index_name).expect("Couldn't get index_name string!").into();

    let sort_cols: Vec<String> = if sort_columns.is_null() {
        vec![]
    } else {
        match convert_java_list_to_string_vec(&mut env, sort_columns) {
            Ok(v) => v,
            Err(e) => {
                log_error!("Failed to convert sort columns list: {}", e);
                let _ = env.throw_new("java/io/IOException", &format!("Failed to read sort columns: {}", e));
                return;
            }
        }
    };

    let reverse_flags: Vec<bool> = if reverse_sorts.is_null() {
        vec![]
    } else {
        match convert_java_list_to_bool_vec(&mut env, reverse_sorts) {
            Ok(v) => v,
            Err(e) => {
                log_error!("Failed to convert reverse sorts list: {}", e);
                let _ = env.throw_new("java/io/IOException", &format!("Failed to read reverse sorts: {}", e));
                return;
            }
        }
    };

    if let Err(e) = NativeParquetWriter::create_writer(filename, index_name, schema_address as i64, sort_cols, reverse_flags) {
        log_error!("ERROR: Failed to create writer: {:?}", e);
        let _ = env.throw_new("java/io/IOException", &format!("Failed to create writer: {}", e));
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_write(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
    array_address: jlong,
    schema_address: jlong,
) {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    if let Err(e) = NativeParquetWriter::write_data(filename, array_address as i64, schema_address as i64) {
        log_error!("ERROR: Failed to write data: {:?}", e);
        let _ = env.throw_new("java/io/IOException", &format!("Failed to write data: {}", e));
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_finalizeWriter(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
) -> jobject {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    match NativeParquetWriter::finalize_writer(filename) {
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
            log_error!("ERROR: Failed to finalize writer: {:?}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to finalize writer: {}", e));
            JObject::null().into_raw()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_syncToDisk(
    mut env: JNIEnv,
    _class: JClass,
    file: JString,
) {
    let filename: String = env.get_string(&file).expect("Couldn't get java string!").into();
    if let Err(e) = NativeParquetWriter::sync_to_disk(filename) {
        log_error!("ERROR: Failed to sync to disk: {:?}", e);
        let _ = env.throw_new("java/io/IOException", &format!("Failed to sync to disk: {}", e));
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
            match create_java_metadata(&mut env, &file_metadata) {
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

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_onSettingsUpdate(
    mut env: JNIEnv,
    _class: JClass,
    settings: JObject,
) {
    match read_native_settings(&mut env, &settings) {
        Ok(config) => {
            let index_name = match &config.index_name {
                Some(name) => name.clone(),
                None => {
                    log_error!("onSettingsUpdate: missing index_name, cannot store settings");
                    let _ = env.throw_new("java/io/IOException", "NativeSettings missing indexName");
                    return;
                }
            };
            log_info!(
                "onSettingsUpdate: storing settings for index '{}' - compression_type={:?}, compression_level={:?}, \
                 page_size_bytes={:?}, page_row_limit={:?}, dict_size_bytes={:?}, row_group_size_bytes={:?}",
                index_name,
                config.compression_type,
                config.compression_level,
                config.page_size_bytes,
                config.page_row_limit,
                config.dict_size_bytes,
                config.row_group_size_bytes
            );
            SETTINGS_STORE.insert(index_name, config);
        }
        Err(e) => {
            log_error!("onSettingsUpdate: failed to read NativeSettings object: {}", e);
            let _ = env.throw_new("java/io/IOException", &format!("Failed to read NativeSettings: {}", e));
        }
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_opensearch_parquet_bridge_RustBridge_removeSettings(
    mut env: JNIEnv,
    _class: JClass,
    index_name: JString,
) {
    match env.get_string(&index_name) {
        Ok(name) => {
            let name: String = name.into();
            SETTINGS_STORE.remove(&name);
            log_info!("removeSettings: removed settings for index '{}'", name);
        }
        Err(e) => {
            log_error!("removeSettings: failed to read index name: {}", e);
        }
    }
}

fn create_java_metadata<'local>(env: &mut JNIEnv<'local>, metadata: &FileMetaData) -> Result<JObject<'local>, Box<dyn std::error::Error>> {
    let class = env.find_class("org/opensearch/parquet/bridge/ParquetFileMetadata")?;
    let created_by_jstring = match metadata.created_by() {
        Some(created_by) => env.new_string(created_by)?,
        None => JObject::null().into(),
    };
    let java_metadata = env.new_object(
        &class,
        "(IJLjava/lang/String;)V",
        &[
            (metadata.version()).into(),
            (metadata.num_rows()).into(),
            (&created_by_jstring).into(),
        ],
    )?;
    Ok(java_metadata)
}

/// Reads a NativeSettings Java object into a Rust NativeSettings struct via JNI getters.
fn read_native_settings(env: &mut JNIEnv, obj: &JObject) -> Result<NativeSettings, Box<dyn std::error::Error>> {
    macro_rules! get_boxed_int {
        ($method:expr) => {{
            let jobj = env.call_method(obj, $method, "()Ljava/lang/Integer;", &[])?.l()?;
            if jobj.is_null() {
                None
            } else {
                Some(env.call_method(&jobj, "intValue", "()I", &[])?.i()?)
            }
        }};
    }

    macro_rules! get_boxed_long_as_usize {
        ($method:expr) => {{
            let jobj = env.call_method(obj, $method, "()Ljava/lang/Long;", &[])?.l()?;
            if jobj.is_null() {
                None
            } else {
                Some(env.call_method(&jobj, "longValue", "()J", &[])?.j()? as usize)
            }
        }};
    }

    macro_rules! get_string {
        ($method:expr) => {{
            let jobj = env.call_method(obj, $method, "()Ljava/lang/String;", &[])?.l()?;
            if jobj.is_null() {
                None
            } else {
                let s: String = env.get_string(&jobj.into())?.into();
                Some(s)
            }
        }};
    }

    Ok(NativeSettings {
        index_name:           get_string!("getIndexName"),
        compression_type:     get_string!("getCompressionType"),
        compression_level:    get_boxed_int!("getCompressionLevel"),
        page_size_bytes:      get_boxed_long_as_usize!("getPageSizeBytes"),
        page_row_limit:       get_boxed_int!("getPageRowLimit").map(|v| v as usize),
        dict_size_bytes:      get_boxed_long_as_usize!("getDictSizeBytes"),
        row_group_size_bytes: get_boxed_long_as_usize!("getRowGroupSizeBytes"),
        field_configs:        None,
        custom_settings:      None,
        sort_columns:         vec![],
        reverse_sorts:        vec![],
    })
}

/// Converts a Java `List<String>` to a Rust `Vec<String>` via JNI.
fn convert_java_list_to_string_vec(
    env: &mut JNIEnv,
    list: JObject,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let size = env.call_method(&list, "size", "()I", &[])?.i()? as usize;
    let mut result = Vec::with_capacity(size);
    for i in 0..size {
        let obj = env.call_method(&list, "get", "(I)Ljava/lang/Object;", &[(i as i32).into()])?.l()?;
        let jstr: JString = obj.into();
        let rust_string: String = env.get_string(&jstr)?.into();
        result.push(rust_string);
    }
    Ok(result)
}

/// Converts a Java `List<Boolean>` to a Rust `Vec<bool>` via JNI.
fn convert_java_list_to_bool_vec(
    env: &mut JNIEnv,
    list: JObject,
) -> Result<Vec<bool>, Box<dyn std::error::Error>> {
    let size = env.call_method(&list, "size", "()I", &[])?.i()? as usize;
    let mut result = Vec::with_capacity(size);
    for i in 0..size {
        let obj = env.call_method(&list, "get", "(I)Ljava/lang/Object;", &[(i as i32).into()])?.l()?;
        let val = env.call_method(&obj, "booleanValue", "()Z", &[])?.z()?;
        result.push(val);
    }
    Ok(result)
}
