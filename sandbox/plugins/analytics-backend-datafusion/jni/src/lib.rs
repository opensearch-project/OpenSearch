/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! JNI bridge layer.
//!
//! This module is a thin adapter between Java's JNI types and the bridge-agnostic
//! API in [`api`]. All core logic lives in `api.rs` and `query_executor.rs`.
//! When migrating to JDK FFM, replace this file with an `extern "C"` bridge
//! that calls the same `api::*` functions.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::cell::RefCell;
use std::sync::{Arc, OnceLock};

use datafusion::common::DataFusionError;
use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JString};
use jni::sys::{jint, jlong};
use jni::{JNIEnv, JavaVM};
use log::error;

pub mod api;
pub mod cross_rt_stream;
pub mod executor;
pub mod io;
pub mod query_executor;
pub mod runtime_manager;
pub mod util;

use jni_macros::jni_safe;

use crate::runtime_manager::RuntimeManager;
use crate::util::*;

// Global state
static TOKIO_RUNTIME_MANAGER: OnceLock<Arc<RuntimeManager>> = OnceLock::new();
static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

thread_local! {
    static THREAD_JNIENV: RefCell<Option<JNIEnv<'static>>> = RefCell::new(None);
}

fn with_jni_env<F, R>(f: F) -> R
where
    F: FnOnce(&mut JNIEnv) -> R,
{
    THREAD_JNIENV.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            let jvm = JAVA_VM.get().expect("JavaVM not initialized");
            let env = jvm
                .attach_current_thread_permanently()
                .expect("Failed to attach thread to JVM");
            *opt = Some(env);
        }
        f(opt.as_mut().unwrap())
    })
}

fn get_manager() -> Result<&'static Arc<RuntimeManager>, DataFusionError> {
    TOKIO_RUNTIME_MANAGER
        .get()
        .ok_or_else(|| DataFusionError::Execution("Runtime manager not initialized".to_string()))
}

// ---------------------------------------------------------------------------
// Tokio runtime management
// ---------------------------------------------------------------------------

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_initTokioRuntimeManager(
    mut env: JNIEnv,
    _class: JClass,
    cpu_threads: jint,
) {
    JAVA_VM.get_or_init(|| env.get_java_vm().expect("Failed to get JavaVM"));
    TOKIO_RUNTIME_MANAGER.get_or_init(|| Arc::new(RuntimeManager::new(cpu_threads as usize)));
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_shutdownTokioRuntimeManager(
    mut env: JNIEnv,
    _class: JClass,
) {
    if let Some(mgr) = TOKIO_RUNTIME_MANAGER.get() {
        mgr.shutdown();
    }
}

// ---------------------------------------------------------------------------
// DataFusion runtime
// ---------------------------------------------------------------------------

#[jni_safe(default = 0)]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_createGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    memory_pool_limit: jlong,
    _cache_manager_ptr: jlong,
    spill_dir: JString,
    spill_limit: jlong,
) -> jlong {
    let spill_dir: String = match env.get_string(&spill_dir) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", format!("Invalid spill dir: {:?}", e));
            return 0;
        }
    };

    match api::create_global_runtime(memory_pool_limit, &spill_dir, spill_limit) {
        Ok(ptr) => ptr as jlong,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            0
        }
    }
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_closeGlobalRuntime(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    unsafe { api::close_global_runtime(ptr as i64) };
}

// ---------------------------------------------------------------------------
// Reader management
// ---------------------------------------------------------------------------

#[jni_safe(default = 0)]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_createDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    table_path: JString,
    files: JObjectArray,
) -> jlong {
    let table_path: String = match env.get_string(&table_path) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", format!("Invalid table path: {:?}", e));
            return 0;
        }
    };
    let filenames = match parse_string_arr(env, files) {
        Ok(f) => f,
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalArgumentException", format!("Invalid file list: {}", e));
            return 0;
        }
    };
    let manager = match get_manager() {
        Ok(m) => m,
        Err(e) => {
            let _ = env.throw_new("java/lang/IllegalStateException", e.to_string());
            return 0;
        }
    };

    match api::create_reader(&table_path, filenames, manager) {
        Ok(ptr) => ptr as jlong,
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            0
        }
    }
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_closeDatafusionReader(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    unsafe { api::close_reader(ptr as i64) };
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_executeQueryAsync(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    substrait_bytes: JObject,
    runtime_ptr: jlong,
    listener: JObject,
) {
    let manager = match get_manager() {
        Ok(m) => m,
        Err(e) => {
            set_action_listener_error(env, listener, &e);
            return;
        }
    };

    let table_name: String = match env.get_string(&JString::from(table_name)) {
        Ok(s) => s.into(),
        Err(e) => {
            set_action_listener_error(env, listener, &DataFusionError::Execution(format!("Invalid table name: {}", e)));
            return;
        }
    };
    let plan_bytes_obj = unsafe { JByteArray::from_raw(substrait_bytes.as_raw()) };
    let plan_bytes = match env.convert_byte_array(plan_bytes_obj) {
        Ok(b) => b,
        Err(e) => {
            set_action_listener_error(env, listener, &DataFusionError::Execution(format!("Failed to convert plan bytes: {}", e)));
            return;
        }
    };
    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            set_action_listener_error(env, listener, &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    // Delegate to bridge-agnostic API — bridge does the block_on
    let result = manager.io_runtime.block_on(unsafe {
        api::execute_query(shard_view_ptr as i64, &table_name, &plan_bytes, runtime_ptr as i64, manager)
    });

    with_jni_env(|env| match result {
        Ok(stream_ptr) => set_action_listener_ok_global(env, &listener_ref, stream_ptr as jlong),
        Err(e) => {
            error!("Query execution failed: {}", e);
            set_action_listener_error_global(env, &listener_ref, &e);
        }
    });
}

// ---------------------------------------------------------------------------
// Stream operations
// ---------------------------------------------------------------------------

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_streamGetSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
    listener: JObject,
) {
    if stream_ptr == 0 {
        set_action_listener_error(env, listener, &DataFusionError::Execution("Invalid stream pointer".to_string()));
        return;
    }
    match unsafe { api::stream_get_schema(stream_ptr as i64) } {
        Ok(schema_ptr) => set_action_listener_ok(env, listener, schema_ptr as jlong),
        Err(e) => set_action_listener_error(env, listener, &e),
    }
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_streamNext(
    mut env: JNIEnv,
    _class: JClass,
    _runtime_ptr: jlong,
    stream_ptr: jlong,
    listener: JObject,
) {
    let manager = match get_manager() {
        Ok(m) => m,
        Err(e) => {
            set_action_listener_error(env, listener, &e);
            return;
        }
    };

    let listener_ref = match env.new_global_ref(&listener) {
        Ok(r) => r,
        Err(e) => {
            set_action_listener_error(env, listener, &DataFusionError::Execution(format!("Failed to create global ref: {}", e)));
            return;
        }
    };

    let result = manager.io_runtime.block_on(unsafe { api::stream_next(stream_ptr as i64) });

    with_jni_env(|env| match result {
        Ok(array_ptr) => set_action_listener_ok_global(env, &listener_ref, array_ptr as jlong),
        Err(e) => {
            error!("Stream next failed: {}", e);
            set_action_listener_error_global(env, &listener_ref, &e);
        }
    });
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_streamClose(
    mut env: JNIEnv,
    _class: JClass,
    stream_ptr: jlong,
) {
    unsafe { api::stream_close(stream_ptr as i64) };
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

#[jni_safe(default = std::ptr::null_mut())]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_sqlToSubstrait(
    mut env: JNIEnv,
    _class: JClass,
    shard_view_ptr: jlong,
    table_name: JString,
    sql: JString,
    runtime_ptr: jlong,
) -> jni::sys::jbyteArray {
    let manager = TOKIO_RUNTIME_MANAGER.get().expect("Runtime manager not initialized");
    let table_name: String = env.get_string(&table_name).expect("Invalid table name").into();
    let sql: String = env.get_string(&sql).expect("Invalid SQL").into();

    let result = unsafe {
        api::sql_to_substrait(shard_view_ptr as i64, &table_name, &sql, runtime_ptr as i64, manager)
    };

    match result {
        Ok(bytes) => env.byte_array_from_slice(&bytes).expect("byte array").into_raw(),
        Err(e) => {
            let _ = env.throw_new("java/lang/RuntimeException", e.to_string());
            std::ptr::null_mut()
        }
    }
}

// ---------------------------------------------------------------------------
// Cache management (stubs)
// ---------------------------------------------------------------------------

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_testPanic(
    mut env: JNIEnv,
    _class: JClass,
    message: JString,
) {
    let msg: String = env.get_string(&message).expect("Invalid message").into();
    panic!("{}", msg);
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_cacheManagerAddFiles(
    mut env: JNIEnv,
    _class: JClass,
    _runtime_ptr: jlong,
    _file_paths: JObjectArray,
) {
    // TODO: wire to native cache manager
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_cacheManagerRemoveFiles(
    mut env: JNIEnv,
    _class: JClass,
    _runtime_ptr: jlong,
    _file_paths: JObjectArray,
) {
    // TODO: wire to native cache manager
}

#[jni_safe]
#[no_mangle]
pub extern "system" fn Java_org_opensearch_be_datafusion_jni_NativeBridge_initLogger(
    mut env: JNIEnv,
    _class: JClass,
) {
    // TODO: wire Rust→Java logging bridge
}
