/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! JNI utility helpers: string array parsing and ActionListener callbacks.

use datafusion::common::DataFusionError;
use jni::objects::{GlobalRef, JObject, JObjectArray, JString};
use jni::sys::jlong;
use jni::JNIEnv;

/// Parse a Java String[] into a Vec<String>.
pub fn parse_string_arr(env: &mut JNIEnv, files: JObjectArray) -> Result<Vec<String>, String> {
    let length = env.get_array_length(&files)
        .map_err(|e| format!("get_array_length: {}", e))?;
    let mut out = Vec::with_capacity(length as usize);
    for i in 0..length {
        let obj = env.get_object_array_element(&files, i)
            .map_err(|e| format!("get_object_array_element: {}", e))?;
        let jstr = JString::from(obj);
        let s: String = env.get_string(&jstr)
            .map_err(|e| format!("get_string: {}", e))?
            .into();
        out.push(s);
    }
    Ok(out)
}

/// Call `ActionListener.onResponse(Long.valueOf(value))`.
pub fn set_action_listener_ok(mut env: JNIEnv, listener: JObject, value: jlong) {
    let long_obj = env.new_object("java/lang/Long", "(J)V", &[value.into()])
        .expect("Failed to create Long");
    env.call_method(listener, "onResponse", "(Ljava/lang/Object;)V", &[(&long_obj).into()])
        .expect("onResponse failed");
}

/// Call `ActionListener.onFailure(new RuntimeException(error))`.
pub fn set_action_listener_error(mut env: JNIEnv, listener: JObject, error: &DataFusionError) {
    let msg = env.new_string(error.to_string()).expect("new_string");
    let exc = env.new_object("java/lang/RuntimeException", "(Ljava/lang/String;)V", &[(&msg).into()])
        .expect("new RuntimeException");
    env.call_method(listener, "onFailure", "(Ljava/lang/Exception;)V", &[(&exc).into()])
        .expect("onFailure failed");
}

/// `set_action_listener_ok` with a `GlobalRef` listener.
pub fn set_action_listener_ok_global(env: &mut JNIEnv, listener: &GlobalRef, value: jlong) {
    let long_obj = env.new_object("java/lang/Long", "(J)V", &[value.into()])
        .expect("Failed to create Long");
    env.call_method(listener.as_obj(), "onResponse", "(Ljava/lang/Object;)V", &[(&long_obj).into()])
        .expect("onResponse failed");
}

/// `set_action_listener_error` with a `GlobalRef` listener.
pub fn set_action_listener_error_global(env: &mut JNIEnv, listener: &GlobalRef, error: &DataFusionError) {
    let msg = env.new_string(error.to_string()).expect("new_string");
    let exc = env.new_object("java/lang/RuntimeException", "(Ljava/lang/String;)V", &[(&msg).into()])
        .expect("new RuntimeException");
    env.call_method(listener.as_obj(), "onFailure", "(Ljava/lang/Exception;)V", &[(&exc).into()])
        .expect("onFailure failed");
}
