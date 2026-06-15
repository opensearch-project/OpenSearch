/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::sync::RwLock;
use tokio::runtime::Handle;

/// Process-global IO runtime handle.
///
/// Reachable from ANY thread — including the bare Java/FFM threads that register
/// object stores and plan queries. [`SpawnIoStore`](crate::spawn_io_store) uses
/// it to dispatch every object-store read onto the IO runtime regardless of
/// which thread polls the store.
///
/// This MUST track the *current* `RuntimeManager`'s IO runtime, not the first
/// one ever installed: `DataFusionService.doStop()` shuts the IO runtime down
/// and `doStart()` builds a fresh one (node restarts in tests, service
/// recycling). A first-wins `OnceLock` would leave `SpawnIoStore` dispatching
/// onto a dead runtime, so every spawned read joins as *cancelled* and fails
/// the query ("object-store read was cancelled"). A swappable slot keeps the
/// handle live across restarts.
static GLOBAL_IO_HANDLE: RwLock<Option<Handle>> = RwLock::new(None);

/// Install (or replace) the process-global IO runtime handle. The most recent
/// writer wins, so the handle always points at the live IO runtime — see the
/// note on [`GLOBAL_IO_HANDLE`].
pub fn set_global_io_handle(handle: Handle) {
     *GLOBAL_IO_HANDLE.write().unwrap() = Some(handle);
}

/// Clear the process-global IO runtime handle (called when the owning
/// `RuntimeManager` is shut down) so a stale, dead-runtime handle is never
/// handed out. `SpawnIoStore::wrap` then falls back to the unwrapped store.
pub fn clear_global_io_handle() {
    *GLOBAL_IO_HANDLE.write().unwrap() = None;
}

/// Returns the process-global IO runtime handle, if one is currently installed.
pub fn global_io_handle() -> Option<Handle> {
    GLOBAL_IO_HANDLE.read().unwrap().clone()
}
