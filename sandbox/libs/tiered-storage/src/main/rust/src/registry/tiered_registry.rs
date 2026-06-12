/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`TieredStorageRegistry`] — concrete registry for tiered storage.
//!
//! # Thread Safety
//!
//! All operations are safe for concurrent use. DashMap entry API is used for
//! check-then-act patterns to prevent TOCTOU races. Atomic operations
//! (SeqCst) are used for `active_reads`.

use std::collections::HashSet;
use std::fmt;

use dashmap::DashMap;

use super::traits::FileRegistry;
use crate::types::{FileLocation, ReadGuard, TieredFileEntry};

// ---------------------------------------------------------------------------
// TieredStorageRegistry
// ---------------------------------------------------------------------------

/// Production file registry backed by [`DashMap`].
///
/// Tracks per-file metadata and provides RAII-based ref counting via
/// [`ReadGuard`].
pub struct TieredStorageRegistry {
    /// Per-file metadata. Key is the file path.
    files: DashMap<String, TieredFileEntry>,
}

// TODO: Add PendingAction (EvictLocal/RemoveFull) and pinned fields to
// TieredFileEntry when eviction lifecycle is needed for the write path.

impl TieredStorageRegistry {
    /// Create a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        native_bridge_common::log_info!("TieredStorageRegistry: created");
        Self {
            files: DashMap::new(),
        }
    }

    /// List entries matching `prefix`. Returns `(key, location)`.
    ///
    /// If `prefix` is empty or `"/"`, returns all entries.
    #[must_use]
    pub fn entries_matching(&self, prefix: &str) -> Vec<(String, FileLocation, u64)> {
        // Strip leading "/" — keys are stored without leading slash
        let prefix = prefix.strip_prefix('/').unwrap_or(prefix);
        let match_all = prefix.is_empty();
        self.files
            .iter()
            .filter(|e| match_all || e.key().starts_with(prefix))
            .map(|e| (e.key().clone(), e.value().location(), e.value().size()))
            .collect()
    }
}

impl Default for TieredStorageRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TieredStorageRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TieredStorageRegistry")
            .field("file_count", &self.files.len())
            .finish()
    }
}

impl fmt::Display for TieredStorageRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TieredStorageRegistry(files={})", self.files.len())
    }
}

// -- FileRegistry trait impl ------------------------------------------------

impl FileRegistry for TieredStorageRegistry {
    fn register(&self, key: &str, value: TieredFileEntry) {
        // Strip leading "/" — keys are stored without leading slash
        let key = key.strip_prefix('/').unwrap_or(key);
        self.files.insert(key.to_string(), value);
    }

    fn get(&self, key: &str) -> Option<ReadGuard<'_>> {
        // Strip leading "/" — keys are stored without leading slash
        let key = key.strip_prefix('/').unwrap_or(key);
        let entry = self.files.get(key)?;
        Some(ReadGuard::new(entry))
    }

    fn update(&self, key: &str, f: impl FnOnce(&mut TieredFileEntry)) {
        // Strip leading "/" — keys are stored without leading slash
        let key = key.strip_prefix('/').unwrap_or(key);
        if let Some(mut entry) = self.files.get_mut(key) {
            f(entry.value_mut());
        }
    }

    fn remove(&self, key: &str, force: bool) -> bool {
        // Strip leading "/" — keys are stored without leading slash
        let key = key.strip_prefix('/').unwrap_or(key);
        if force {
            self.files.remove(key).is_some()
        } else {
            // Only remove if ref_count == 0.
            match self.files.entry(key.to_string()) {
                dashmap::mapref::entry::Entry::Occupied(entry) => {
                    if entry.get().ref_count() == 0 {
                        entry.remove();
                        true
                    } else {
                        false
                    }
                }
                dashmap::mapref::entry::Entry::Vacant(_) => false,
            }
        }
    }

    fn remove_by_prefix(&self, prefix: &str, force: bool) -> usize {
        // Strip leading "/" — keys are stored without leading slash
        let prefix = prefix.strip_prefix('/').unwrap_or(prefix);
        if force {
            let mut removed = 0usize;
            self.files.retain(|key, _| {
                if key.starts_with(prefix) {
                    removed += 1;
                    false
                } else {
                    true
                }
            });
            removed
        } else {
            let matching: Vec<String> = self
                .files
                .iter()
                .filter(|e| e.key().starts_with(prefix))
                .map(|e| e.key().clone())
                .collect();
            let mut removed = 0;
            for key in &matching {
                if self.remove(key, false) {
                    removed += 1;
                }
            }
            removed
        }
    }

    fn purge_stale(&self, valid_keys: &HashSet<String>) -> usize {
        let before = self.files.len();
        self.files.retain(|key, _| {
            // Check both with and without leading "/" since callers may pass either form
            valid_keys.contains(key.as_str())
                || valid_keys.contains(&format!("/{}", key))
        });
        let removed = before.saturating_sub(self.files.len());
        if removed > 0 {
            native_bridge_common::log_info!(
                "TieredStorageRegistry: purge_stale removed {} entries",
                removed
            );
        }
        removed
    }

    fn len(&self) -> usize {
        self.files.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::thread;

    fn make_registry() -> TieredStorageRegistry {
        TieredStorageRegistry::new()
    }

    fn local_entry() -> TieredFileEntry {
        TieredFileEntry::new(FileLocation::Local, None)
    }

    fn remote_entry() -> TieredFileEntry {
        TieredFileEntry::new(
            FileLocation::Remote,
            Some(Arc::from("remote/a.parquet")),
        )
    }

    fn both_entry() -> TieredFileEntry {
        TieredFileEntry::new(
            FileLocation::Remote,
            Some(Arc::from("remote/a.parquet")),
        )
    }

    // -- Register -----------------------------------------------------------

    #[test]
    fn test_register_local() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_register_remote() {
        let reg = make_registry();
        reg.register("/a.parquet", remote_entry());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_register_both() {
        let reg = make_registry();
        reg.register("/a.parquet", both_entry());
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_register_overwrites() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        reg.register("/a.parquet", remote_entry());
        assert_eq!(reg.len(), 1);
        let guard = reg.get("/a.parquet").unwrap();
        assert_eq!(guard.location(), FileLocation::Remote);
    }

    // -- Get / ReadGuard ----------------------------------------------------

    #[test]
    fn test_get_returns_guard_with_correct_data() {
        let reg = make_registry();
        reg.register("/a.parquet", remote_entry());
        let guard = reg.get("/a.parquet").unwrap();
        assert_eq!(guard.location(), FileLocation::Remote);
        assert_eq!(guard.remote_path(), Some("remote/a.parquet"));
        assert_eq!(guard.ref_count(), 1);
    }

    #[test]
    fn test_get_nonexistent_returns_none() {
        let reg = make_registry();
        assert!(reg.get("/nonexistent").is_none());
    }

    #[test]
    fn test_read_guard_auto_releases_on_drop() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        {
            let guard = reg.get("/a.parquet").unwrap();
            assert_eq!(guard.ref_count(), 1);
        }
        // After guard dropped, ref count should be 0.
        let guard2 = reg.get("/a.parquet").unwrap();
        // This guard is the only one, so ref_count == 1.
        assert_eq!(guard2.ref_count(), 1);
        drop(guard2);
    }

    #[test]
    fn test_multiple_guards_accumulate() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        let g1 = reg.get("/a.parquet").unwrap();
        let g2 = reg.get("/a.parquet").unwrap();
        assert_eq!(g2.ref_count(), 2);
        drop(g1);
        assert_eq!(g2.ref_count(), 1);
        drop(g2);
    }

    // -- Update -------------------------------------------------------------

    #[test]
    fn test_update_modifies_entry() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        reg.update("/a.parquet", |e| {
            e.location = FileLocation::Remote;
        });
        let guard = reg.get("/a.parquet").unwrap();
        assert_eq!(guard.location(), FileLocation::Remote);
    }

    #[test]
    fn test_update_nonexistent_is_noop() {
        let reg = make_registry();
        reg.update("/nonexistent", |e| {
            e.location = FileLocation::Remote;
        });
        assert_eq!(reg.len(), 0);
    }

    // -- Remove -------------------------------------------------------------

    #[test]
    fn test_remove_force_true() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        assert!(reg.remove("/a.parquet", true));
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_remove_force_false_zero_refs() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        assert!(reg.remove("/a.parquet", false));
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_remove_force_false_active_refs() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        let _guard = reg.get("/a.parquet").unwrap();
        // Can't remove while guard is held — but guard holds a Ref which
        // prevents entry-level operations. We need to test differently.
        // Drop the guard first, then acquire manually.
        drop(_guard);

        // Manually acquire to simulate active reads without holding DashMap Ref.
        reg.update("/a.parquet", |e| {
            e.acquire();
        });
        assert!(!reg.remove("/a.parquet", false));
        assert_eq!(reg.len(), 1);
        // Clean up.
        reg.update("/a.parquet", |e| {
            e.release();
        });
    }

    #[test]
    fn test_remove_force_true_with_active_refs() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        reg.update("/a.parquet", |e| {
            e.acquire();
        });
        assert!(reg.remove("/a.parquet", true));
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_remove_nonexistent() {
        let reg = make_registry();
        assert!(!reg.remove("/nonexistent", true));
        assert!(!reg.remove("/nonexistent", false));
    }

    // -- Remove by prefix ---------------------------------------------------

    #[test]
    fn test_remove_by_prefix_force() {
        let reg = make_registry();
        reg.register("/data/a.parquet", local_entry());
        reg.register("/data/b.parquet", local_entry());
        reg.register("/other/c.parquet", local_entry());
        let removed = reg.remove_by_prefix("/data/", true);
        assert_eq!(removed, 2);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_remove_by_prefix_no_force_skips_active() {
        let reg = make_registry();
        reg.register("/data/a.parquet", local_entry());
        reg.register("/data/b.parquet", local_entry());
        reg.update("/data/b.parquet", |e| {
            e.acquire();
        });
        let removed = reg.remove_by_prefix("/data/", false);
        assert_eq!(removed, 1);
        assert_eq!(reg.len(), 1);
        // Clean up.
        reg.update("/data/b.parquet", |e| {
            e.release();
        });
    }

    // -- Purge stale --------------------------------------------------------

    #[test]
    fn test_purge_stale_removes_unknown() {
        let reg = make_registry();
        reg.register("/a.parquet", local_entry());
        reg.register("/b.parquet", local_entry());
        reg.register("/c.parquet", local_entry());
        let valid: HashSet<String> = ["/a.parquet".to_string()].into_iter().collect();
        let removed = reg.purge_stale(&valid);
        assert_eq!(removed, 2);
        assert_eq!(reg.len(), 1);
    }

    // -- entries_matching ---------------------------------------------------

    #[test]
    fn test_entries_matching_prefix() {
        let reg = make_registry();
        reg.register("data/a.parquet", local_entry());
        reg.register(
            "data/b.parquet",
            TieredFileEntry::new(FileLocation::Remote, None),
        );
        reg.register("other/c.parquet", local_entry());

        let entries = reg.entries_matching("data/");
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_entries_matching_all() {
        let reg = make_registry();
        reg.register("a", local_entry());
        reg.register("b", local_entry());
        let entries = reg.entries_matching("");
        assert_eq!(entries.len(), 2);
        let entries2 = reg.entries_matching("/");
        assert_eq!(entries2.len(), 2);
    }

    // -- Concurrency --------------------------------------------------------

    #[test]
    fn test_concurrent_acquire_release_returns_to_zero() {
        let reg = Arc::new(make_registry());
        reg.register("/a.parquet", local_entry());

        let num_threads = 16;
        let ops_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let reg = Arc::clone(&reg);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..ops_per_thread {
                        if let Some(guard) = reg.get("/a.parquet") {
                            drop(guard);
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread should not panic");
        }

        // All guards dropped, ref count should be 0.
        let guard = reg.get("/a.parquet").unwrap();
        assert_eq!(guard.ref_count(), 1); // Only this guard.
    }

    #[test]
    fn test_concurrent_register_different_paths() {
        let reg = Arc::new(make_registry());
        let num_threads = 16;
        let barrier = Arc::new(Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let reg = Arc::clone(&reg);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for j in 0..100 {
                        reg.register(&format!("/t{}/f{}.parquet", i, j), local_entry());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread should not panic");
        }

        assert_eq!(reg.len(), num_threads * 100);
    }
}
