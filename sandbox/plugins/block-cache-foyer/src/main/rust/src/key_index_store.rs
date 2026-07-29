/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Persistence helpers for the [`FoyerCache`] key_index.
//!
//! Provides atomic save/load/delete for a JSON snapshot of the
//! `DashMap<String, HashSet<String>>` key_index. The file lives inside the
//! Foyer cache directory alongside the Foyer segment files.
//!
//! ## File format
//! ```json
//! {"version":1,"index":{"data/shard/_0.parquet":["data/shard/_0.parquet\u001f0-4096"]}}
//! ```
//!
//! ## Crash safety
//! `save()` writes to `.key_index.json.tmp` then renames to `key_index.json`.
//! The rename is atomic on POSIX filesystems. A crash between the two steps
//! leaves the `.tmp` file behind; `load()` uses it as a fallback and then
//! deletes it.
//!
//! [`FoyerCache`]: crate::foyer::foyer_cache::FoyerCache

use std::collections::{HashMap, HashSet};
use std::io::{self, ErrorKind};
use std::path::Path;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Format version embedded in every snapshot. Bump when the schema changes
/// incompatibly; old files with a different version are rejected on load.
pub const SNAPSHOT_VERSION: u32 = 1;

/// Name of the committed snapshot file inside the cache directory.
pub const KEY_INDEX_FILENAME: &str = "key_index.json";

/// Name of the in-progress temp file used during an atomic write.
pub const KEY_INDEX_TMP_FILENAME: &str = ".key_index.json.tmp";

// ── Types ─────────────────────────────────────────────────────────────────────

/// Deserialized representation of the key_index snapshot.
///
/// Used exclusively by `load()` / `parse_and_validate()`. `save()` serializes
/// the live `DashMap` directly via `serde_json::json!` — no conversion to this
/// struct on the write path.
///
/// The `index` field deserializes from the JSON object produced by DashMap's
/// `Serialize` impl: both produce `{"prefix": ["key1", "key2", ...], ...}`.
#[derive(Debug, Serialize, Deserialize)]
pub struct KeyIndexSnapshot {
    /// Format version. Must equal [`SNAPSHOT_VERSION`] to be accepted.
    pub version: u32,
    /// Deserialized key_index: prefix → set of Foyer keys.
    pub index: HashMap<String, HashSet<String>>,
}

impl KeyIndexSnapshot {
    /// Return an empty snapshot with the current version.
    /// Used as a fallback when the snapshot file is missing or unparseable.
    pub fn empty() -> Self {
        Self {
            version: SNAPSHOT_VERSION,
            index: HashMap::new(),
        }
    }

    /// Return `true` when there are no prefix buckets in this snapshot.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Atomically write the key_index snapshot to `<dir>/key_index.json`.
///
/// Serializes the DashMap directly (no intermediate HashMap copy) using
/// DashMap's own `Serialize` impl (enabled via the `serde` workspace feature).
/// A concurrent `put()` racing with this call may or may not appear in the
/// snapshot — both outcomes are acceptable; the next periodic persist or
/// graceful-shutdown `Drop` will capture any missed keys.
///
/// # Error handling
/// Returns `Err` on I/O failure. Callers (the persist task and `Drop`) log
/// the error but do not propagate it — a failed persist is not fatal.
pub fn save(dir: &Path, key_index: &DashMap<String, HashSet<String>>) -> io::Result<()> {
    // DashMap implements Serialize (workspace feature "serde" enabled for dashmap).
    // Serialize directly — no intermediate HashMap copy needed.
    let json = serde_json::to_string(&serde_json::json!({
        "version": SNAPSHOT_VERSION,
        "index": key_index
    }))
    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

    let tmp_path = dir.join(KEY_INDEX_TMP_FILENAME);
    let final_path = dir.join(KEY_INDEX_FILENAME);

    // Atomic write: write to .tmp then rename.
    // If the process crashes between write and rename, .tmp is left behind;
    // load() will fall back to it on the next startup.
    std::fs::write(&tmp_path, json.as_bytes())?;
    std::fs::rename(&tmp_path, &final_path)?;

    Ok(())
}

/// Load the key_index snapshot from `<dir>/key_index.json`.
///
/// # Load strategy
/// 1. Try `key_index.json` (primary). Success → return snapshot.
/// 2. If `key_index.json` is absent, try `.key_index.json.tmp` (fallback for
///    the crash-during-rename edge case). Success → log WARN, return snapshot.
/// 3. Both absent → return `Err(NotFound)`.
///
/// `key_index.json` is always preferred: it is the last *successfully committed*
/// snapshot. If both files exist, the rename completed and `.tmp` is stale.
///
/// # Cleanup
/// Deletes `.key_index.json.tmp` after every load attempt to prevent stale
/// temp files from accumulating across restarts.
pub fn load(dir: &Path) -> io::Result<KeyIndexSnapshot> {
    let result = load_inner(dir);

    let tmp_path = dir.join(KEY_INDEX_TMP_FILENAME);
    if tmp_path.exists() {
        let _ = std::fs::remove_file(&tmp_path);
    }

    result
}

fn load_inner(dir: &Path) -> io::Result<KeyIndexSnapshot> {
    let final_path = dir.join(KEY_INDEX_FILENAME);
    let tmp_path = dir.join(KEY_INDEX_TMP_FILENAME);

    // 1. Try the committed file.
    match std::fs::read_to_string(&final_path) {
        Ok(contents) => return parse_and_validate(&contents),
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    // 2. Fallback: .tmp (crash-during-rename recovery).
    match std::fs::read_to_string(&tmp_path) {
        Ok(contents) => {
            native_bridge_common::log_info!(
                "[block-cache] key_index: using .tmp fallback (crash during rename?), dir={}",
                dir.display()
            );
            parse_and_validate(&contents)
        }
        Err(e) if e.kind() == ErrorKind::NotFound => Err(io::Error::new(
            ErrorKind::NotFound,
            "key_index.json not found",
        )),
        Err(e) => Err(e),
    }
}

fn parse_and_validate(contents: &str) -> io::Result<KeyIndexSnapshot> {
    let snapshot: KeyIndexSnapshot =
        serde_json::from_str(contents).map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

    if snapshot.version != SNAPSHOT_VERSION {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "key_index version mismatch: expected {}, got {}",
                SNAPSHOT_VERSION, snapshot.version
            ),
        ));
    }

    Ok(snapshot)
}

/// Load the snapshot, returning an empty snapshot on any error.
///
/// - `NotFound` → empty, silently. Normal for first-ever startup or after a
///   crash where graceful shutdown (`Drop`) was not called.
/// - Any other error → logs WARN, returns empty. Never fails startup.
pub fn load_or_empty(dir: &Path) -> KeyIndexSnapshot {
    match load(dir) {
        Ok(snapshot) => snapshot,
        Err(e) if e.kind() == ErrorKind::NotFound => KeyIndexSnapshot::empty(),
        Err(e) => {
            native_bridge_common::log_info!(
                "[block-cache] WARNING: key_index load failed, starting empty: {} — {}",
                dir.display(),
                e
            );
            KeyIndexSnapshot::empty()
        }
    }
}

/// Delete both `key_index.json` and `.key_index.json.tmp` from `dir`.
///
/// `NotFound` is treated as success for both files.
/// Called by `FoyerCache::clear()` so the next startup does not bulk-load
/// stale keys into a freshly-cleared cache.
pub fn delete(dir: &Path) -> io::Result<()> {
    for name in [KEY_INDEX_FILENAME, KEY_INDEX_TMP_FILENAME] {
        match std::fs::remove_file(dir.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tempfile::TempDir;

    fn make_map(entries: Vec<(&str, Vec<&str>)>) -> DashMap<String, HashSet<String>> {
        let map = DashMap::new();
        for (prefix, keys) in entries {
            let set: HashSet<String> = keys.into_iter().map(String::from).collect();
            map.insert(prefix.to_string(), set);
        }
        map
    }

    // ── save + load roundtrip ─────────────────────────────────────────────────

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let map = make_map(vec![
            (
                "data/a.parquet",
                vec!["data/a.parquet\x1F0-100", "data/a.parquet\x1F100-200"],
            ),
            (
                "data/b.parquet",
                vec!["data/b.parquet\x1F0-512", "data/b.parquet\x1F512-1024"],
            ),
            (
                "data/c.parquet",
                vec!["data/c.parquet\x1F0-4096", "data/c.parquet\x1F4096-8192"],
            ),
        ]);

        save(dir.path(), &map).expect("save must succeed");
        assert!(dir.path().join(KEY_INDEX_FILENAME).exists());

        let snapshot = load(dir.path()).expect("load must succeed");
        assert_eq!(snapshot.version, SNAPSHOT_VERSION);
        assert_eq!(snapshot.index.len(), 3);
        let total_keys: usize = snapshot.index.values().map(|s| s.len()).sum();
        assert_eq!(total_keys, 6);
    }

    #[test]
    fn test_separator_in_keys_roundtrip() {
        let dir = TempDir::new().unwrap();
        let key = "data/nodes/0/file.parquet\x1F99999-200000";
        let map = make_map(vec![("data/nodes/0/file.parquet", vec![key])]);

        save(dir.path(), &map).unwrap();
        let snapshot = load(dir.path()).unwrap();
        let keys = snapshot.index.get("data/nodes/0/file.parquet").unwrap();
        assert!(
            keys.contains(key),
            "\\x1F separator must survive serde roundtrip"
        );
    }

    #[test]
    fn test_save_empty_map_produces_valid_file() {
        let dir = TempDir::new().unwrap();
        let map: DashMap<String, HashSet<String>> = DashMap::new();
        save(dir.path(), &map).unwrap();
        let snapshot = load(dir.path()).unwrap();
        assert!(snapshot.index.is_empty());
    }

    // ── load error cases ──────────────────────────────────────────────────────

    #[test]
    fn test_load_returns_not_found_for_missing_file() {
        let dir = TempDir::new().unwrap();
        assert_eq!(load(dir.path()).unwrap_err().kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_load_or_empty_returns_empty_for_missing_file() {
        let dir = TempDir::new().unwrap();
        assert!(load_or_empty(dir.path()).is_empty());
    }

    #[test]
    fn test_load_or_empty_returns_empty_for_corrupt_json() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(KEY_INDEX_FILENAME), b"{{not valid json}}").unwrap();
        assert!(load_or_empty(dir.path()).is_empty());
    }

    #[test]
    fn test_load_rejects_wrong_version() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join(KEY_INDEX_FILENAME),
            br#"{"version":999,"index":{}}"#,
        )
        .unwrap();
        assert_eq!(load(dir.path()).unwrap_err().kind(), ErrorKind::InvalidData);
    }

    // ── atomic write ──────────────────────────────────────────────────────────

    #[test]
    fn test_save_is_atomic_no_tmp_file_on_success() {
        let dir = TempDir::new().unwrap();
        save(dir.path(), &make_map(vec![("p", vec!["p\x1F0-1"])])).unwrap();
        assert!(dir.path().join(KEY_INDEX_FILENAME).exists());
        assert!(!dir.path().join(KEY_INDEX_TMP_FILENAME).exists());
    }

    // ── .tmp fallback ─────────────────────────────────────────────────────────

    #[test]
    fn test_load_uses_tmp_fallback_when_main_file_absent() {
        let dir = TempDir::new().unwrap();
        let snap = KeyIndexSnapshot {
            version: SNAPSHOT_VERSION,
            index: {
                let mut m = HashMap::new();
                m.insert(
                    "data/x.parquet".to_string(),
                    ["data/x.parquet\x1F0-100".to_string()].into(),
                );
                m
            },
        };
        let json = serde_json::to_string(&snap).unwrap();
        std::fs::write(dir.path().join(KEY_INDEX_TMP_FILENAME), json.as_bytes()).unwrap();
        assert!(!dir.path().join(KEY_INDEX_FILENAME).exists());

        let loaded = load(dir.path()).expect(".tmp fallback must succeed");
        assert_eq!(loaded.index.len(), 1);
        assert!(loaded.index.contains_key("data/x.parquet"));
        assert!(
            !dir.path().join(KEY_INDEX_TMP_FILENAME).exists(),
            ".tmp must be deleted"
        );
    }

    #[test]
    fn test_load_prefers_main_file_over_tmp() {
        let dir = TempDir::new().unwrap();
        let main_snap = KeyIndexSnapshot {
            version: SNAPSHOT_VERSION,
            index: {
                let mut m = HashMap::new();
                m.insert("main".to_string(), HashSet::new());
                m
            },
        };
        let tmp_snap = KeyIndexSnapshot {
            version: SNAPSHOT_VERSION,
            index: {
                let mut m = HashMap::new();
                m.insert("tmp".to_string(), HashSet::new());
                m
            },
        };
        std::fs::write(
            dir.path().join(KEY_INDEX_FILENAME),
            serde_json::to_string(&main_snap).unwrap().as_bytes(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(KEY_INDEX_TMP_FILENAME),
            serde_json::to_string(&tmp_snap).unwrap().as_bytes(),
        )
        .unwrap();

        let loaded = load(dir.path()).unwrap();
        assert!(loaded.index.contains_key("main"));
        assert!(!loaded.index.contains_key("tmp"));
        assert!(!dir.path().join(KEY_INDEX_TMP_FILENAME).exists());
    }

    #[test]
    fn test_load_tmp_fallback_also_fails_returns_not_found() {
        let dir = TempDir::new().unwrap();
        assert_eq!(load(dir.path()).unwrap_err().kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_load_cleans_up_tmp_even_when_main_succeeds() {
        let dir = TempDir::new().unwrap();
        let snap = KeyIndexSnapshot {
            version: SNAPSHOT_VERSION,
            index: HashMap::new(),
        };
        std::fs::write(
            dir.path().join(KEY_INDEX_FILENAME),
            serde_json::to_string(&snap).unwrap().as_bytes(),
        )
        .unwrap();
        std::fs::write(dir.path().join(KEY_INDEX_TMP_FILENAME), b"stale").unwrap();

        load(dir.path()).unwrap();
        assert!(!dir.path().join(KEY_INDEX_TMP_FILENAME).exists());
    }

    // ── delete ────────────────────────────────────────────────────────────────

    #[test]
    fn test_delete_removes_both_files() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(KEY_INDEX_FILENAME), b"x").unwrap();
        std::fs::write(dir.path().join(KEY_INDEX_TMP_FILENAME), b"y").unwrap();
        delete(dir.path()).unwrap();
        assert!(!dir.path().join(KEY_INDEX_FILENAME).exists());
        assert!(!dir.path().join(KEY_INDEX_TMP_FILENAME).exists());
    }

    #[test]
    fn test_delete_is_noop_for_missing_files() {
        let dir = TempDir::new().unwrap();
        delete(dir.path()).unwrap();
    }
}
