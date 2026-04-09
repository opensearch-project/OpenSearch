/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
// Macro to wrap JNI entry points with panic-catching and exception conversion.

/// Wraps a JNI function body with panic catching. On panic, throws a Java RuntimeException
/// and returns `$default`. Usage:
///
/// ```ignore
/// jni_entry!(env, 0, {
///     // body that may panic — `env` is &mut JNIEnv
///     42 as jlong
/// })
/// ```
#[macro_export]
macro_rules! jni_entry {
    ($env:expr, $default:expr, $body:block) => {{
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let env_ptr = $env as *mut jni::JNIEnv;
        match catch_unwind(AssertUnwindSafe(|| {
            let env = unsafe { &mut *env_ptr };
            (|| -> _ { $body })()
        })) {
            Ok(result) => result,
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else {
                    "unknown panic".to_string()
                };
                let env = unsafe { &mut *env_ptr };
                let _ = env.throw_new(
                    "java/lang/RuntimeException",
                    format!("Native panic: {}", msg),
                );
                $default
            }
        }
    }};
}
