/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Shared test harness for the async-completion FFM entry points.
//!
//! The `df_*_async` entries (`df_stream_next_async`, `df_sender_send_async`)
//! initiate work on the IO runtime and deliver the result later through the
//! completion upcall registered via `df_register_completion_callback`. These
//! helpers install a process-global callback that parks each call id on a
//! condvar and provides blocking `*_sync` wrappers so integration tests can
//! drive the async API as if it were synchronous — mirroring what the Java
//! `DatafusionResultStream` / `DatafusionPartitionSender` do with a virtual
//! thread + `CompletableFuture.join`.

#![allow(dead_code)]

use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};

use opensearch_datafusion::completion::df_register_completion_callback;
use opensearch_datafusion::ffm::{df_sender_send_async, df_stream_next_async};

/// MUST match `ffm.rs` (and `NativeBridge`).
pub const SENDER_SEND_RECEIVER_DROPPED: i64 = 1;
/// MUST match `ffm.rs` (and `NativeBridge`).
pub const SENDER_SEND_PENDING: i64 = 2;

struct Completions {
    results: Mutex<HashMap<i64, Result<i64, String>>>,
    cv: Condvar,
}

static COMPLETIONS: OnceLock<Completions> = OnceLock::new();
static NEXT_CALL_ID: AtomicI64 = AtomicI64::new(1);
static INSTALLED: OnceLock<()> = OnceLock::new();

fn completions() -> &'static Completions {
    COMPLETIONS.get_or_init(|| Completions {
        results: Mutex::new(HashMap::new()),
        cv: Condvar::new(),
    })
}

/// Completion upcall. Stores the result (taking ownership of any batch pointer,
/// which the waiter then drains) and wakes the waiter. Returns 1 = accepted.
unsafe extern "C" fn on_complete(call_id: i64, value: i64, err_ptr: *const u8, err_len: i64) -> i32 {
    let result = if err_len > 0 {
        let bytes = std::slice::from_raw_parts(err_ptr, err_len as usize);
        Err(String::from_utf8_lossy(bytes).into_owned())
    } else {
        Ok(value)
    };
    let c = completions();
    c.results.lock().unwrap().insert(call_id, result);
    c.cv.notify_all();
    1
}

/// Installs the global completion callback exactly once.
pub fn install_completion_callback() {
    INSTALLED.get_or_init(|| {
        unsafe { df_register_completion_callback(on_complete) };
    });
}

fn alloc_call_id() -> i64 {
    NEXT_CALL_ID.fetch_add(1, Ordering::Relaxed)
}

fn wait(call_id: i64) -> Result<i64, String> {
    let c = completions();
    let mut guard = c.results.lock().unwrap();
    loop {
        if let Some(r) = guard.remove(&call_id) {
            return r;
        }
        guard = c.cv.wait(guard).unwrap();
    }
}

/// Decode (and free) the heap-allocated error string behind a negative FFM rc.
pub fn decode_error(rc: i64) -> String {
    assert!(rc < 0, "expected negative rc, got {}", rc);
    let ptr = (-rc) as *mut c_char;
    let cstring = unsafe { CString::from_raw(ptr) };
    cstring.to_string_lossy().into_owned()
}

/// Blocking wrapper over `df_stream_next_async`. Returns the batch pointer
/// (0 = EOS) or the error message.
pub fn stream_next_sync(stream_ptr: i64) -> Result<i64, String> {
    install_completion_callback();
    let call_id = alloc_call_id();
    let rc = unsafe { df_stream_next_async(stream_ptr, call_id) };
    if rc < 0 {
        return Err(decode_error(rc));
    }
    // rc == 0: initiation succeeded; the batch/error arrives via the completion.
    wait(call_id)
}

/// Blocking wrapper over `df_sender_send_async`. Returns the synchronous-or-deferred
/// outcome code (0 = sent, [`SENDER_SEND_RECEIVER_DROPPED`]) or an error message.
pub fn sender_send_sync(sender_ptr: i64, array_ptr: i64, schema_ptr: i64) -> Result<i64, String> {
    install_completion_callback();
    let call_id = alloc_call_id();
    let rc = unsafe { df_sender_send_async(sender_ptr, array_ptr, schema_ptr, call_id) };
    if rc < 0 {
        return Err(decode_error(rc));
    }
    if rc == SENDER_SEND_PENDING {
        return wait(call_id);
    }
    Ok(rc)
}
