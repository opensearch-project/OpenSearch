/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Rust→Java logging via FFM callback.
//!
//! Java registers a function pointer at startup via `native_logger_init`.
//! Rust calls that pointer to log. No JNI.
//!
//! Level short-circuit: the `log_debug!`/`log_info!` macros gate on a Rust-side
//! atomic level BEFORE `format!`, so a suppressed log costs only one relaxed
//! atomic load + branch — no string allocation, no FFM crossing. Java keeps
//! this atomic in sync via `native_logger_set_level`.

use std::sync::atomic::{AtomicI32, AtomicPtr, Ordering};

/// Callback signature: `void log(int level, const char* msg, long msg_len)`
type LogCallback = unsafe extern "C" fn(i32, *const u8, i64);

static LOG_CALLBACK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Minimum level that will be formatted + forwarded to Java. Defaults to Info
/// so `log_debug!` is suppressed until Java enables DEBUG.
static MAX_LEVEL: AtomicI32 = AtomicI32::new(LogLevel::Info as i32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum LogLevel {
    Debug = 0,
    Info = 1,
    Error = 2,
}

/// True if a message at `level` would be emitted. Cheap (one relaxed load).
#[inline]
pub fn enabled(level: LogLevel) -> bool {
    (level as i32) >= MAX_LEVEL.load(Ordering::Relaxed)
}

/// Set the minimum forwarded log level. Called by Java at init and whenever the
/// logger level changes dynamically.
#[no_mangle]
pub extern "C" fn native_logger_set_level(level: i32) {
    let clamped = level.clamp(LogLevel::Debug as i32, LogLevel::Error as i32);
    MAX_LEVEL.store(clamped, Ordering::Relaxed);
}

/// Called by Java at startup to register the log callback.
///
/// # Safety
///
/// `callback` must be a valid function pointer matching the [`LogCallback`]
/// signature (`void log(int, const char*, long)`) and must remain valid for the
/// entire lifetime of the process, since it is stored globally and invoked from
/// arbitrary threads on every subsequent log call.
#[no_mangle]
pub unsafe extern "C" fn native_logger_init(callback: LogCallback) {
    LOG_CALLBACK.store(callback as *mut (), Ordering::Release);
    log(LogLevel::Info, "Native logger initialized successfully");
}

pub fn log(level: LogLevel, message: &str) {
    let ptr = LOG_CALLBACK.load(Ordering::Acquire);
    if ptr.is_null() {
        eprintln!("[RUST_LOG_FALLBACK] {:?}: {}", level, message);
        return;
    }
    let callback: LogCallback = unsafe { std::mem::transmute(ptr) };
    unsafe { callback(level as i32, message.as_ptr(), message.len() as i64) };
}

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        if $crate::logger::enabled($crate::logger::LogLevel::Debug) {
            $crate::logger::log($crate::logger::LogLevel::Debug, &format!($($arg)*))
        }
    };
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        if $crate::logger::enabled($crate::logger::LogLevel::Info) {
            $crate::logger::log($crate::logger::LogLevel::Info, &format!($($arg)*))
        }
    };
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        if $crate::logger::enabled($crate::logger::LogLevel::Error) {
            $crate::logger::log($crate::logger::LogLevel::Error, &format!($($arg)*))
        }
    };
}
