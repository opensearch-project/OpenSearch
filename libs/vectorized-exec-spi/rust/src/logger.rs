/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use jni::objects::JValue;
use jni::{JNIEnv, JavaVM};
use std::sync::OnceLock;

static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

/// Log level enum (must match Java RustLoggerBridge.LogLevel ordinals)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Error = 2,
}

impl LogLevel {
    fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Initialize the logger from a JNIEnv reference.
/// This is a convenience function that extracts the JVM from the env.
pub fn init_logger_from_env(env: &JNIEnv) {
    if let Ok(jvm) = env.get_java_vm() {
        JAVA_VM.set(jvm).ok();
    }
}

// Private log function - only used by macros
fn log(level: LogLevel, message: &str) {
    if let Some(jvm) = JAVA_VM.get() {
        if let Ok(mut env) = jvm.attach_current_thread() {
            let result = (|| -> Result<(), Box<dyn std::error::Error>> {
                let class =
                    env.find_class("org/opensearch/vectorized/execution/jni/RustLoggerBridge")?;
                let java_message = env.new_string(message)?;
                env.call_static_method(
                    class,
                    "log",
                    "(ILjava/lang/String;)V",
                    &[JValue::Int(level.as_i32()), (&java_message).into()],
                )?;
                Ok(())
            })();

            if result.is_err() {
                // Fallback to stderr if JNI call fails
                eprintln!("[RUST_LOG_FALLBACK] {:?}: {}", level, message);
            }
        }
    }
}

/// Log at DEBUG level with format! syntax
/// Usage: log_debug!("message") or log_debug!("value: {}", x)
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        $crate::logger::__internal_log($crate::logger::LogLevel::Debug, &format!($($arg)*))
    };
}

/// Log at INFO level with format! syntax
/// Usage: log_info!("message") or log_info!("value: {}", x)
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::logger::__internal_log($crate::logger::LogLevel::Info, &format!($($arg)*))
    };
}

/// Log at ERROR level with format! syntax
/// Usage: log_error!("message") or log_error!("value: {}", x)
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::logger::__internal_log($crate::logger::LogLevel::Error, &format!($($arg)*))
    };
}

// Internal function used by macros - must be public for macro expansion but not intended for direct use
#[doc(hidden)]
pub fn __internal_log(level: LogLevel, message: &str) {
    log(level, message);
}
