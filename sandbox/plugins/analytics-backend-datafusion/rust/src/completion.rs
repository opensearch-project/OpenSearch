/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Generic async-completion upcall to Java.
//!
//! One static slot, registered once at startup via
//! `df_register_completion_callback` (FilterTreeCallbacks-style — see
//! `indexed_table::ffm_callbacks`). Spawned IO-runtime tasks deliver their
//! result back to the Java-side per-call listener through this callback,
//! turning the previously-blocking `df_stream_next` / `df_sender_send`
//! data-flow waits into initiate-now, complete-via-upcall operations.
//!
//! Contract of `on_complete(call_id, value, err_ptr, err_len) -> consumed`:
//!   err_len > 0  => failure. `value` is 0. err bytes valid only for the call duration.
//!   err_len == 0 => success. `value` is the payload (meaning is per-operation).
//!   return 1     => Java accepted ownership of `value`.
//!   return 0     => Java did not (listener gone / threw): Rust reclaims if owned.

use std::sync::atomic::{AtomicPtr, Ordering};

pub(crate) type OnNativeCompleteFn = unsafe extern "C" fn(i64, i64, *const u8, i64) -> i32;
static ON_COMPLETE: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Registered by Java at startup. Stores the completion function pointer into
/// the atomic slot. Not annotated `#[ffm_safe]` (that macro is specific to the
/// `-> i64` error-pointer convention); a manual `catch_unwind` guards the
/// boundary even though an atomic store can't realistically panic.
#[no_mangle]
pub unsafe extern "C" fn df_register_completion_callback(cb: OnNativeCompleteFn) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ON_COMPLETE.store(cb as *mut (), Ordering::Release);
    }));
}

pub(crate) fn load_on_complete() -> Result<OnNativeCompleteFn, String> {
    let p = ON_COMPLETE.load(Ordering::Acquire);
    if p.is_null() {
        return Err("completion callback not registered".into());
    }
    Ok(unsafe { std::mem::transmute::<*mut (), OnNativeCompleteFn>(p) })
}

/// Deliver a completion. `value_is_owned_batch`: when true and Java declines
/// (consumed == 0), the value is a boxed `FFI_ArrowArray` Rust must drop here —
/// its release callback frees the underlying Arrow buffers.
pub(crate) fn complete(
    cb: OnNativeCompleteFn,
    call_id: i64,
    result: Result<i64, String>,
    value_is_owned_batch: bool,
) {
    let consumed = unsafe {
        match &result {
            Ok(v) => cb(call_id, *v, std::ptr::null(), 0),
            Err(m) => cb(call_id, 0, m.as_ptr(), m.len() as i64),
        }
    };
    if consumed == 0 && value_is_owned_batch {
        if let Ok(ptr) = result {
            if ptr != 0 {
                unsafe { drop(Box::from_raw(ptr as *mut arrow_array::ffi::FFI_ArrowArray)); }
            }
        }
    }
}
