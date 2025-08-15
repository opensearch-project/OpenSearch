/*
 * SPDX-License-Identifier: Apache-2.0
 */

use jni::JNIEnv;
use jni::objects::{JObjectArray, JString};
use std::collections::HashMap;
use anyhow::Result;

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

/// Throw a Java exception
pub fn throw_exception(env: &mut JNIEnv, message: &str) {
    let _ = env.throw_new("java/lang/RuntimeException", message);
}
