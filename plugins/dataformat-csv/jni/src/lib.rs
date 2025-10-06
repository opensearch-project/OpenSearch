/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! OpenSearch DataFusion Csv JNI Library
//!
//! This library provides JNI bindings for DataFusion query execution,

use jni::JNIEnv;
use jni::objects::{JClass, JString, JObjectArray, JByteArray};
use jni::sys::{jlong, jstring};
use std::ptr;
use std::collections::HashMap;

mod context;
mod runtime;
mod stream;
mod substrait;
mod util;
mod csv_exec;

use context::SessionContextManager;
use runtime::RuntimeManager;
use stream::RecordBatchStreamWrapper;
use substrait::SubstraitExecutor;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;

/**
TODO : Put more thought into this
**/
static mut RUNTIME_MANAGER: Option<RuntimeManager> = None;

static mut SESSION_MANAGER: Option<SessionContextManager> = None;

/// Initialize the managers (call once)
fn init_managers() {
    unsafe {
        if RUNTIME_MANAGER.is_none() {
            RUNTIME_MANAGER = Some(RuntimeManager::new());
        }
        if SESSION_MANAGER.is_none() {
            SESSION_MANAGER = Some(SessionContextManager::new());
        }
    }
}
static mut RUNTIME_ENVIRONMENTS: Option<HashMap<u64, String>> = None;


/// Register a directory as a table in the global context and return runtime environment ID
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_CsvDataSourceCodec_nativeRegisterDirectory(
    mut env: JNIEnv,
    _class: JClass,
    table_name: JString,
    directory_path: JString,
    files: JObjectArray,
    runtime_id: jlong
) {
    let runtimeEnv = unsafe { &mut *(runtime_id as *mut RuntimeEnv) };
    // placeholder
}

/// Create a new session context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_CsvDataSourceCodec_nativeCreateSessionContext(
    mut env: JNIEnv,
    _class: JClass,
    config_keys: JObjectArray,
    config_values: JObjectArray,
) -> jlong {
    // Initialize managers if not already done
    init_managers();

    // PLACEHOLDER
    // Parse configuration from JNI arrays
    let config = match util::parse_string_map(&mut env, config_keys, config_values) {
        Ok(cfg) => cfg,
        Err(e) => {
            util::throw_exception(&mut env, &format!("Failed to parse config: {}", e));
            return 0;
        }
    };

    // Create session context
    match unsafe {
        RUNTIME_MANAGER.as_ref().unwrap().block_on(async {
            SESSION_MANAGER.as_mut().unwrap().create_session_context(config).await
        })
    } {
        Ok(context_ptr) => context_ptr as jlong,
        Err(e) => {
            util::throw_exception(&mut env, &format!("Failed to create session context: {}", e));
            0
        }
    }
}

/// Execute a Substrait query plan
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_CsvDataSourceCodec_nativeExecuteSubstraitQuery(
    mut env: JNIEnv,
    _class: JClass,
    session_context_ptr: jlong,
    substrait_plan: JByteArray,
) -> jlong {

    // Convert JByteArray to Vec<u8>
    let substrait_plan_bytes = match env.convert_byte_array(substrait_plan) {
        Ok(bytes) => bytes,
        Err(e) => {
            util::throw_exception(&mut env, &format!("Failed to convert substrait plan: {}", e));
            return 0;
        }
    };

    // Execute the query
    match unsafe {
        RUNTIME_MANAGER.as_ref().unwrap().block_on(async {
            let executor = SubstraitExecutor::new();
            executor.execute_plan(session_context_ptr as *mut SessionContext, &substrait_plan_bytes).await
        })
    } {
        Ok(stream_ptr) => stream_ptr as jlong,
        Err(e) => {
            util::throw_exception(&mut env, &format!("Failed to execute query: {}", e));
            0
        }
    }
}

/// Close a session context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_CsvDataSourceCodec_nativeCloseSessionContext(
    mut env: JNIEnv,
    _class: JClass,
    session_context_ptr: jlong,
) {

    if let Err(e) = unsafe {
        RUNTIME_MANAGER.as_ref().unwrap().block_on(async {
            SESSION_MANAGER.as_mut().unwrap()
                .close_session_context(session_context_ptr as *mut SessionContext)
                .await
        })
    } {
        util::throw_exception(&mut env, &format!("Failed to close session context: {}", e));
    }
}

/// Get the next record batch from a stream
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_CsvRecordBatchStream_nativeNextBatch(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
) -> jstring {

    let stream = unsafe { &mut *(stream_ptr as *mut RecordBatchStreamWrapper) };

    match unsafe {
        RUNTIME_MANAGER.as_ref().unwrap().block_on(async {
            stream.next_batch().await
        })
    } {
        Ok(Some(batch_json)) => {
            match env.new_string(&batch_json) {
                Ok(jstr) => jstr.into_raw(),
                Err(e) => {
                    util::throw_exception(&mut env, &format!("Failed to create Java string: {}", e));
                    ptr::null_mut()
                }
            }
        }
        Ok(None) => ptr::null_mut(), // End of stream
        Err(e) => {
            util::throw_exception(&mut env, &format!("Failed to get next batch: {}", e));
            ptr::null_mut()
        }
    }
}

/// Close a record batch stream
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_csv_CsvRecordBatchStream_nativeCloseStream(
    _env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
) {
    if stream_ptr != 0 {
        let stream = unsafe { Box::from_raw(stream_ptr as *mut RecordBatchStreamWrapper) };
        drop(stream);
    }
}
