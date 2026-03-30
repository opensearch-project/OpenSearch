 /*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use datafusion::error::DataFusionError;
use jni::objects::{GlobalRef, JObject, JObjectArray, JString, JValue};
use jni::sys::jlong;
use jni::JNIEnv;
use object_store::ObjectMeta;
// JNI utility functions.
/// Parse a Java String[] into Vec<String>.
pub fn parse_string_arr(env: &mut JNIEnv, arr: JObjectArray) -> Result<Vec<String>, DataFusionError> {
    let len = env.get_array_length(&arr).map_err(|e| DataFusionError::Execution(e.to_string()))?;
    let mut result = Vec::with_capacity(len as usize);
    for i in 0..len {
        let obj = env.get_object_array_element(&arr, i).map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let jstr = JString::from(obj);
        let s: String = env.get_string(&jstr).map_err(|e| DataFusionError::Execution(e.to_string()))?.into();
        result.push(s);
    }
    Ok(result)
}

/// Build ObjectMeta for each file using the given object store.
pub async fn create_object_metas(
    store: &dyn object_store::ObjectStore,
    base_path: &str,
    filenames: Vec<String>,
) -> Result<Vec<ObjectMeta>, DataFusionError> {
    let mut metas = Vec::with_capacity(filenames.len());
    for filename in filenames {
        let full_path = if filename.starts_with('/') || filename.contains(base_path) {
            filename
        } else {
            format!("{}/{}", base_path.trim_end_matches('/'), filename)
        };
        let path = object_store::path::Path::from(full_path.as_str());
        let meta = store.head(&path).await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to get object meta for {}: {}", full_path, e))
        })?;
        metas.push(meta);
    }
    Ok(metas)
}

/// Call ActionListener.onResponse(Long) via JNI.
pub fn set_action_listener_ok(env: &mut JNIEnv, listener: JObject, value: jlong) {
    let boxed = env
        .call_static_method("java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", &[value.into()])
        .expect("Long.valueOf failed");
    env.call_method(
        listener,
        "onResponse",
        "(Ljava/lang/Object;)V",
        &[(&boxed).into()],
    )
    .expect("onResponse failed");
}

/// Call ActionListener.onResponse(Long) via GlobalRef.
pub fn set_action_listener_ok_global(env: &mut JNIEnv, listener: &GlobalRef, value: jlong) {
    let boxed = env
        .call_static_method("java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", &[value.into()])
        .expect("Long.valueOf failed");
    env.call_method(
        listener.as_obj(),
        "onResponse",
        "(Ljava/lang/Object;)V",
        &[(&boxed).into()],
    )
    .expect("onResponse failed");
}

/// Call ActionListener.onFailure(Exception) via JNI.
pub fn set_action_listener_error(
    env: &mut JNIEnv,
    listener: JObject,
    error: &DataFusionError,
) {
    let msg = env
        .new_string(error.to_string())
        .expect("new_string failed");
    let exception = env
        .new_object(
            "java/lang/RuntimeException",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&msg)],
        )
        .expect("new RuntimeException failed");
    env.call_method(
        listener,
        "onFailure",
        "(Ljava/lang/Exception;)V",
        &[JValue::Object(&exception)],
    )
    .expect("onFailure failed");
}

/// Call ActionListener.onFailure(Exception) via GlobalRef.
pub fn set_action_listener_error_global(
    env: &mut JNIEnv,
    listener: &GlobalRef,
    error: &DataFusionError,
) {
    let msg = env
        .new_string(error.to_string())
        .expect("new_string failed");
    let exception = env
        .new_object(
            "java/lang/RuntimeException",
            "(Ljava/lang/String;)V",
            &[JValue::Object(&msg)],
        )
        .expect("new RuntimeException failed");
    env.call_method(
        listener.as_obj(),
        "onFailure",
        "(Ljava/lang/Exception;)V",
        &[JValue::Object(&exception)],
    )
    .expect("onFailure failed");
}
