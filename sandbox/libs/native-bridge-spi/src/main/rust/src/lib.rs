/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared Rust utilities for OpenSearch sandbox native plugins.

use jni::objects::JValue;
use jni::{JNIEnv, JavaVM};
use std::sync::OnceLock;

static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

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
pub fn init_logger_from_env(env: &JNIEnv) {
    if JAVA_VM.get().is_some() {
        // return early as env has already been initialized
        return;
    }
    match env.get_java_vm() {
        Ok(jvm) => {
            if let Err(_) = JAVA_VM.set(jvm) {
                // Another thread initialized it between our check and set; that's fine.
            } else {
                // Log through the Java bridge to confirm end-to-end setup;
                // absence of this message indicates a logger initialization failure.
                log(LogLevel::Info, "Native logger initialized successfully");
            }
        }
        Err(e) => eprintln!("[RUST_LOG_FALLBACK] Failed to get JavaVM: {}", e),
    }
}

fn log(level: LogLevel, message: &str) {
    if let Some(jvm) = JAVA_VM.get() {
        if let Ok(mut env) = jvm.attach_current_thread() {
            let result = (|| -> Result<(), Box<dyn std::error::Error>> {
                let class = env.find_class("org/opensearch/nativebridge/spi/RustLoggerBridge")?;
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
                eprintln!("[RUST_LOG_FALLBACK] {:?}: {}", level, message);
            }
        }
    }
}

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        $crate::__internal_log($crate::LogLevel::Debug, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::__internal_log($crate::LogLevel::Info, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::__internal_log($crate::LogLevel::Error, &format!($($arg)*))
    };
}

#[doc(hidden)]
pub fn __internal_log(level: LogLevel, message: &str) {
    log(level, message);
}
