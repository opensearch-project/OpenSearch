/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Process-global IO runtime [`Handle`], shared across all native crates.
//!
//! # Why this lives here
//!
//! The dedicated IO runtime is owned by the analytics engine's `RuntimeManager`
//! (in the `opensearch-datafusion` crate). But the object stores that should
//! dispatch their network IO onto it — `AmazonS3`, `GoogleCloudStorage`,
//! `MicrosoftAzure` — are built in the separate `native-repository-*` crates,
//! which do NOT depend on `opensearch-datafusion`. The one crate they all share
//! is `native-bridge-common`, so the handle slot lives here as the single source
//! of truth every crate can reach.
//!
//! The analytics `RuntimeManager` calls [`set_io_handle`] when it builds the IO
//! runtime, keeps the [`HandleRegistration`] it gets back, and presents that to
//! [`clear_io_handle_if_current`] on shutdown. Each remote object-store
//! builder calls [`io_handle`] and, if present, installs a
//! `SpawnedReqwestConnector` so HTTP requests + response-body streaming run on
//! the IO runtime instead of the CPU runtime — DataFusion's `thread_pools`
//! example mechanism. If no handle is installed (e.g. a unit test, or a process
//! with no IO runtime), the builders leave their default connector untouched.
//!
//! A swappable [`RwLock`] slot (not a first-wins `OnceLock`) is used on purpose:
//! `DataFusionService.doStop()` tears the IO runtime down and `doStart()` builds
//! a fresh one (node restarts in tests, service recycling). A stale handle would
//! point at a dead runtime; last-writer-wins keeps it live across restarts.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use tokio::runtime::Handle;

/// Identifies one installation of the handle. [`set_io_handle`] hands one back and
/// [`clear_io_handle_if_current`] takes it, so an owner that outlives its own
/// replacement can only clear a slot it still owns.
///
/// A monotonic counter rather than [`Handle::id`] on purpose: tokio documents that
/// a runtime's id may be reused once that runtime has completed, which is exactly
/// when a stale owner's `Drop` runs — so ids can collide in precisely the case
/// this type exists to distinguish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HandleRegistration(u64);

static NEXT_REGISTRATION: AtomicU64 = AtomicU64::new(1);
static GLOBAL_IO_HANDLE: RwLock<Option<(HandleRegistration, Handle)>> = RwLock::new(None);

/// Install (or replace) the process-global IO runtime handle. Most recent writer
/// wins, so the handle always points at the live IO runtime. The returned
/// registration is what lets the caller clear only its own installation later.
pub fn set_io_handle(handle: Handle) -> HandleRegistration {
    let registration = HandleRegistration(NEXT_REGISTRATION.fetch_add(1, Ordering::Relaxed));
    *GLOBAL_IO_HANDLE.write().unwrap() = Some((registration, handle));
    registration
}

/// Clear the process-global IO runtime handle unconditionally, for an explicit
/// teardown that means "this process has no IO runtime any more".
///
/// Prefer [`clear_io_handle_if_current`] when clearing because one particular
/// runtime went away: an unconditional clear from a late-dropping owner strands
/// every store built afterwards on its default connector.
pub fn clear_io_handle() {
    *GLOBAL_IO_HANDLE.write().unwrap() = None;
}

/// Clear the handle only if `registration` is the one currently installed.
/// Returns whether it cleared, which a caller can log but does not have to act on.
pub fn clear_io_handle_if_current(registration: HandleRegistration) -> bool {
    let mut slot = GLOBAL_IO_HANDLE.write().unwrap();
    match *slot {
        Some((installed, _)) if installed == registration => {
            *slot = None;
            true
        }
        _ => false,
    }
}

/// Returns the process-global IO runtime handle, if one is currently installed.
pub fn io_handle() -> Option<Handle> {
    GLOBAL_IO_HANDLE
        .read()
        .unwrap()
        .as_ref()
        .map(|(_, handle)| handle.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The slot is process-global and `cargo test` is parallel by default, so every test here
    /// holds this for its whole body. Poison is ignored deliberately: the guarded data is `()`,
    /// so a panicking test leaves nothing half-written, and propagating poison would turn one
    /// real failure into `PoisonError` in the others and hide it.
    static SLOT_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn lock_slot() -> std::sync::MutexGuard<'static, ()> {
        SLOT_GUARD
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // Covers the full lifecycle the remote object-store builders and RuntimeManager
    // rely on: install → read → last-writer-wins → clear.
    #[test]
    fn set_get_replace_clear_lifecycle() {
        let _slot = lock_slot();
        // Clean slate (a prior test in this binary may have left a handle).
        clear_io_handle();
        assert!(io_handle().is_none(), "handle must start cleared");

        let rt_a = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let rt_b = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        // Install A → io_handle() hands out A.
        set_io_handle(rt_a.handle().clone());
        assert_eq!(
            io_handle().as_ref().map(|h| h.id()),
            Some(rt_a.handle().id()),
            "io_handle() must return the installed handle (A)"
        );

        // Install B → last writer wins (this is why it's an RwLock slot, not a
        // first-wins OnceLock: doStop/doStart rebuilds the runtime).
        set_io_handle(rt_b.handle().clone());
        assert_eq!(
            io_handle().as_ref().map(|h| h.id()),
            Some(rt_b.handle().id()),
            "the most recent set_io_handle must win (B replaces A)"
        );

        // Clear → builders fall back to the default connector.
        clear_io_handle();
        assert!(
            io_handle().is_none(),
            "clear_io_handle must empty the slot so no stale handle is handed out"
        );
    }

    // Covers the ownership check the owning runtime relies on when it is torn down
    // out of order.
    #[test]
    fn clear_if_current_only_clears_its_own_registration() {
        let _slot = lock_slot();
        clear_io_handle();

        let rt_a = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let rt_b = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let reg_a = set_io_handle(rt_a.handle().clone());
        let reg_b = set_io_handle(rt_b.handle().clone());
        assert_ne!(
            reg_a, reg_b,
            "each installation needs its own registration, or ownership cannot be told apart"
        );

        // A's owner is torn down after B replaced it — the common case when an in-flight
        // caller held the old owner across a re-install. B's handle must survive.
        assert!(
            !clear_io_handle_if_current(reg_a),
            "a superseded registration must not clear the slot"
        );
        assert_eq!(
            io_handle().as_ref().map(|h| h.id()),
            Some(rt_b.handle().id()),
            "the live handle (B) must still be installed after A's late teardown"
        );

        // B is the installed one, so it does clear.
        assert!(
            clear_io_handle_if_current(reg_b),
            "the installed registration must clear the slot"
        );
        assert!(
            io_handle().is_none(),
            "slot must be empty after B cleared it"
        );

        // Clearing twice is a no-op rather than a panic: teardown paths can double-fire.
        assert!(!clear_io_handle_if_current(reg_b));
    }
}
