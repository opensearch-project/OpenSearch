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

// Log level constants (must match Java RustLoggerBridge constants)
pub const LEVEL_TRACE: i32 = 0;
pub const LEVEL_DEBUG: i32 = 1;
pub const LEVEL_INFO: i32 = 2;
pub const LEVEL_WARN: i32 = 3;
pub const LEVEL_ERROR: i32 = 4;

/// Initialize the logger with a JVM reference.
/// This can be called multiple times safely - only the first call takes effect.
pub fn init_logger(jvm: JavaVM) {
    JAVA_VM.set(jvm).ok();
}

/// Initialize the logger from a JNIEnv reference.
/// This is a convenience function that extracts the JVM from the env.
pub fn init_logger_from_env(env: &JNIEnv) {
    if let Ok(jvm) = env.get_java_vm() {
        init_logger(jvm);
    }
}

pub fn log(level: i32, message: &str) {
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
                    &[JValue::Int(level), (&java_message).into()],
                )?;
                Ok(())
            })();

            if result.is_err() {
                // Fallback to stdout if JNI call fails
                eprintln!("[RUST_LOG_FALLBACK] level={}: {}", level, message);
            }
        }
    }
}

/// Convenience function for logging at INFO level
pub fn log_info(message: &str) {
    log(LEVEL_INFO, message);
}

/// Convenience function for logging at WARN level
pub fn log_warn(message: &str) {
    log(LEVEL_WARN, message);
}

/// Convenience function for logging at ERROR level
pub fn log_error(message: &str) {
    log(LEVEL_ERROR, message);
}

/// Convenience function for logging at DEBUG level
pub fn log_debug(message: &str) {
    log(LEVEL_DEBUG, message);
}

/// Convenience function for logging at TRACE level
pub fn log_trace(message: &str) {
    log(LEVEL_TRACE, message);
}

// Macros for convenient logging with format! syntax

/// Log at INFO level with format! syntax
#[macro_export]
macro_rules! rust_log_info {
    ($($arg:tt)*) => {
        $crate::logger::log_info(&format!($($arg)*))
    };
}

/// Log at WARN level with format! syntax
#[macro_export]
macro_rules! rust_log_warn {
    ($($arg:tt)*) => {
        $crate::logger::log_warn(&format!($($arg)*))
    };
}

/// Log at ERROR level with format! syntax
#[macro_export]
macro_rules! rust_log_error {
    ($($arg:tt)*) => {
        $crate::logger::log_error(&format!($($arg)*))
    };
}

/// Log at DEBUG level with format! syntax
#[macro_export]
macro_rules! rust_log_debug {
    ($($arg:tt)*) => {
        $crate::logger::log_debug(&format!($($arg)*))
    };
}

/// Log at TRACE level with format! syntax
#[macro_export]
macro_rules! rust_log_trace {
    ($($arg:tt)*) => {
        $crate::logger::log_trace(&format!($($arg)*))
    };
}
