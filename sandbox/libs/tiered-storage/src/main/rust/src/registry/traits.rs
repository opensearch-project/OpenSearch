/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`FileRegistry`] trait — interface for file tracking with RAII ref counting.
//!
//! The trait is specific to [`TieredFileEntry`] and returns [`ReadGuard`] from
//! `get()` for automatic acquire/release semantics.

use std::collections::HashSet;

use crate::types::{ReadGuard, TieredFileEntry};

/// File registry trait for tiered storage.
///
/// Implementations must be thread-safe (`Send + Sync`). Ref counting is
/// managed by [`ReadGuard`] — `get()` returns a guard that auto-acquires
/// on creation and auto-releases on drop.
pub trait FileRegistry: Send + Sync {
    /// Register or update an entry.
    fn register(&self, key: &str, value: TieredFileEntry);

    /// Get an entry with auto-acquired ref count. Guard releases on drop.
    fn get(&self, key: &str) -> Option<ReadGuard<'_>>;

    /// Update an entry in-place via a closure.
    fn update(&self, key: &str, f: impl FnOnce(&mut TieredFileEntry));

    /// Remove an entry. If `force=false`, only remove if `ref_count == 0`.
    /// Returns `true` if the entry was removed.
    fn remove(&self, key: &str, force: bool) -> bool;

    /// Remove entries whose key starts with `prefix`.
    /// If `force=false`, only remove entries with `ref_count == 0`.
    /// Returns count of entries removed.
    fn remove_by_prefix(&self, prefix: &str, force: bool) -> usize;

    /// Remove entries not in `valid_keys`. Returns count removed.
    fn purge_stale(&self, valid_keys: &HashSet<String>) -> usize;

    /// Number of entries.
    fn len(&self) -> usize;

    /// Check if empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
