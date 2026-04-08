/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM error handling.
//!
//! Convention: FFM functions return `i64`.
//!   - `>= 0` → success (value or pointer)
//!   - `< 0`  → error. Negate to get a pointer to a heap-allocated error.
//!              Call `native_error_message` to read, `native_error_free` to free.

use std::ffi::CString;
use std::os::raw::c_char;

/// Heap-allocate the error message and return its pointer as a negative i64.
pub fn into_error_ptr(msg: String) -> i64 {
    let c = CString::new(msg).unwrap_or_else(|_| CString::new("error contained null byte").unwrap());
    let ptr = c.into_raw();
    -(ptr as i64)
}

/// Returns a pointer to the null-terminated error message.
#[no_mangle]
pub unsafe extern "C" fn native_error_message(ptr: i64) -> *const c_char {
    ptr as *const c_char
}

/// Frees a heap-allocated error string.
#[no_mangle]
pub unsafe extern "C" fn native_error_free(ptr: i64) {
    if ptr != 0 {
        let _ = CString::from_raw(ptr as *mut c_char);
    }
}

/// Deliberately panics with the given message. For testing panic handling.
#[no_mangle]
pub unsafe extern "C" fn native_test_panic(msg_ptr: *const u8, msg_len: i64) -> i64 {
    match ::std::panic::catch_unwind(::std::panic::AssertUnwindSafe(|| -> Result<i64, String> {
        let msg = std::str::from_utf8_unchecked(std::slice::from_raw_parts(msg_ptr, msg_len as usize));
        panic!("{}", msg);
    })) {
        Ok(Ok(v)) => v,
        Ok(Err(msg)) => into_error_ptr(msg),
        Err(panic) => {
            let msg = if let Some(s) = panic.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = panic.downcast_ref::<&str>() {
                s.to_string()
            } else {
                "unknown panic".to_string()
            };
            into_error_ptr(msg)
        }
    }
}

/// Returns an error (not a panic) with the given message. For testing error handling.
#[no_mangle]
pub unsafe extern "C" fn native_test_error(msg_ptr: *const u8, msg_len: i64) -> i64 {
    let msg = std::str::from_utf8_unchecked(std::slice::from_raw_parts(msg_ptr, msg_len as usize));
    into_error_ptr(msg.to_string())
}
