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
//! runtime (and [`clear_io_handle`] on shutdown). Each remote object-store
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

use std::sync::RwLock;
use tokio::runtime::Handle;

static GLOBAL_IO_HANDLE: RwLock<Option<Handle>> = RwLock::new(None);

/// Install (or replace) the process-global IO runtime handle. Most recent writer
/// wins, so the handle always points at the live IO runtime.
pub fn set_io_handle(handle: Handle) {
    *GLOBAL_IO_HANDLE.write().unwrap() = Some(handle);
}

/// Clear the process-global IO runtime handle (called when the owning runtime is
/// shut down) so a stale, dead-runtime handle is never handed out.
pub fn clear_io_handle() {
    *GLOBAL_IO_HANDLE.write().unwrap() = None;
}

/// Returns the process-global IO runtime handle, if one is currently installed.
pub fn io_handle() -> Option<Handle> {
    GLOBAL_IO_HANDLE.read().unwrap().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    // One test on purpose: the slot is process-global, so parallel tests would
    // race on it. Covers the full lifecycle the remote object-store builders and
    // RuntimeManager rely on: install → read → last-writer-wins → clear.
    #[test]
    fn set_get_replace_clear_lifecycle() {
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
}
