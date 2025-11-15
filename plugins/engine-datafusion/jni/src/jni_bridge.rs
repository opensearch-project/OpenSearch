/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! JNI bridge implementations matching Java NativeBridge class

use jni::objects::{JByteArray, JClass, JLongArray, JObject, JObjectArray, JString};
use jni::sys::{jbyteArray, jint, jlong, jstring};
use jni::JNIEnv;

use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionConfig;
use tokio::runtime::Runtime;

use crate::util::set_object_result_error;

/// Create global runtime environment
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    match RuntimeEnvBuilder::default().build() {
        Ok(runtime_env) => Box::into_raw(Box::new(runtime_env)) as jlong,
        Err(_) => 0,
    }
}

/// Create Tokio runtime
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createTokioRuntime(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    match Runtime::new() {
        Ok(rt) => Box::into_raw(Box::new(rt)) as jlong,
        Err(_) => 0,
    }
}

/// Close global runtime
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeGlobalRuntime(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        let _ = unsafe { Box::from_raw(ptr as *mut RuntimeEnv) };
    }
}

/// Create session context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_createSessionContext(
    _env: JNIEnv,
    _class: JClass,
    runtime_id: jlong,
) -> jlong {
    if runtime_id == 0 {
        return 0;
    }
    
    let runtime_env = unsafe { &*(runtime_id as *const RuntimeEnv) };
    let config = SessionConfig::new().with_repartition_aggregations(true);
    let context = SessionContext::new_with_config_rt(config, std::sync::Arc::new(runtime_env.clone()));
    Box::into_raw(Box::new(context)) as jlong
}

/// Close session context
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeSessionContext(
    _env: JNIEnv,
    _class: JClass,
    context_id: jlong,
) {
    if context_id != 0 {
        let _ = unsafe { Box::from_raw(context_id as *mut SessionContext) };
    }
}

/// Close stream
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_streamClose(
    _env: JNIEnv,
    _class: JClass,
    stream: jlong,
) {
    if stream != 0 {
        let _ = unsafe { Box::from_raw(stream as *mut SendableRecordBatchStream) };
    }
}

/// Close DataFusion reader
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_closeDatafusionReader(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        let _ = unsafe { Box::from_raw(ptr as *mut crate::ShardView) };
    }
}

/// Get version info
#[no_mangle]
pub extern "system" fn Java_org_opensearch_datafusion_jni_NativeBridge_getVersionInfo(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version_info = format!(
        r#"{{"version": "{}", "codecs": ["CsvDataSourceCodec"]}}"#,
        datafusion::DATAFUSION_VERSION
    );
    env.new_string(version_info)
        .expect("Couldn't create Java string")
        .as_raw()
}
