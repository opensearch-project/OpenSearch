/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`TieredObjectStore`] — routes reads between local and remote stores
//! based on [`TieredStorageRegistry`] metadata.
//!
//! Owns the per-repository remote store backends and the registry. On every
//! read, it checks the registry:
//! - **Remote** → delegates to the remote backend via cached store in entry
//! - **Local / Both / not registered** → falls through to the local store
//!
//! # Thread Safety
//!
//! `TieredObjectStore` is `Send + Sync`. All mutable state lives in the
//! registry's atomics and DashMap — no locks are held during I/O.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

use crate::registry::traits::FileRegistry;
use crate::registry::TieredStorageRegistry;
use crate::types::{FileLocation, TieredFileEntry};

// ---------------------------------------------------------------------------
// TieredObjectStore
// ---------------------------------------------------------------------------

/// ObjectStore implementation that routes reads between local and remote
/// stores based on [`TieredStorageRegistry`] metadata.
///
/// Owns the per-repository remote store backends and delegates registration
/// and file tracking to the registry.
pub struct TieredObjectStore {
    registry: Arc<TieredStorageRegistry>,
    remote_stores: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
    local: Arc<dyn ObjectStore>,
}

impl TieredObjectStore {
    /// Create a new tiered store routing between `local` and remote backends.
    #[must_use]
    pub fn new(registry: Arc<TieredStorageRegistry>, local: Arc<dyn ObjectStore>) -> Self {
        native_bridge_common::log_info!("TieredObjectStore: created");
        Self {
            registry,
            remote_stores: RwLock::new(HashMap::new()),
            local,
        }
    }

    /// Reference to the underlying registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<TieredStorageRegistry> {
        &self.registry
    }

    /// Register a remote object-store backend under a repository key.
    pub fn add_store(&self, repo_key: String, store: Arc<dyn ObjectStore>) {
        native_bridge_common::log_info!("TieredObjectStore: add_store repo_key='{}'", repo_key);
        self.remote_stores
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(repo_key, store);
    }

    /// Number of registered remote stores.
    #[must_use]
    pub fn store_count(&self) -> usize {
        self.remote_stores
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Register a file in the registry, resolving the cached store from
    /// `remote_stores` for Remote/Both locations.
    pub fn register_file(
        &self,
        path: &str,
        location: FileLocation,
        remote_path: Option<String>,
        repo_key: Option<String>,
    ) -> Result<(), crate::types::FileRegistryError> {
        // Validate: Remote/Both requires remote_path + repo_key.
        if matches!(location, FileLocation::Remote | FileLocation::Both) {
            if remote_path.is_none() {
                return Err(crate::types::FileRegistryError::InvalidRegistration {
                    path: path.to_string(),
                    reason: format!("remote_path required for location={}", location),
                });
            }
            if repo_key.is_none() {
                return Err(crate::types::FileRegistryError::InvalidRegistration {
                    path: path.to_string(),
                    reason: format!("repo_key required for location={}", location),
                });
            }
        }

        // Resolve cached store at registration time for Remote/Both.
        let cached_store = if matches!(location, FileLocation::Remote | FileLocation::Both) {
            let rk = repo_key.as_ref().unwrap();
            let stores = self.remote_stores.read().unwrap_or_else(|e| e.into_inner());
            let store = stores.get(rk.as_str()).ok_or_else(|| {
                crate::types::FileRegistryError::InvalidRegistration {
                    path: path.to_string(),
                    reason: format!("remote store not registered for repo_key='{}'", rk),
                }
            })?;
            Some(Arc::clone(store))
        } else {
            None
        };

        let remote_arc: Option<Arc<str>> = remote_path.map(Arc::from);

        let entry = TieredFileEntry::new(location, remote_arc, repo_key, cached_store, None);
        self.registry.register(path, entry);

        native_bridge_common::log_debug!(
            "TieredObjectStore: register_file path='{}', location={}",
            path,
            location
        );
        Ok(())
    }

    /// Transition a file's location and metadata via `registry.update()`.
    pub fn transition(
        &self,
        path: &str,
        location: FileLocation,
        remote_path: Option<String>,
        repo_key: Option<String>,
    ) {
        let cached_store = if matches!(location, FileLocation::Remote | FileLocation::Both) {
            if let Some(rk) = repo_key.as_ref() {
                let stores = self.remote_stores.read().unwrap_or_else(|e| e.into_inner());
                stores.get(rk.as_str()).map(Arc::clone)
            } else {
                None
            }
        } else {
            None
        };

        let remote_arc: Option<Arc<str>> = remote_path.map(Arc::from);
        let repo_arc: Option<Arc<str>> = repo_key.map(Arc::from);

        self.registry.update(path, move |e| {
            e.location = location;
            e.remote_path = remote_arc;
            e.repo_key = repo_arc;
            e.cached_store = cached_store;
        });

        native_bridge_common::log_debug!(
            "TieredObjectStore: transition path='{}', location={}",
            path,
            location
        );
    }

    // TODO: Add pin(path)/unpin(path) methods for write-path eviction protection.
    // TODO: Add schedule_eviction(path) and sweep() for deferred eviction lifecycle.
}

impl fmt::Debug for TieredObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TieredObjectStore")
            .field("file_count", &self.registry.len())
            .field("store_count", &self.store_count())
            .finish()
    }
}

impl fmt::Display for TieredObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TieredObjectStore(files={}, stores={})",
            self.registry.len(),
            self.store_count()
        )
    }
}

// ---------------------------------------------------------------------------
// ObjectStore impl
// ---------------------------------------------------------------------------

#[async_trait]
impl ObjectStore for TieredObjectStore {
    /// Write to local store and register the file as [`FileLocation::Local`].
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let size = payload.content_length() as u64;
        let result = self.local.put_opts(location, payload, opts).await?;

        let path_str = location.as_ref();
        let entry = TieredFileEntry::new(FileLocation::Local, None, None, None, Some(size));
        self.registry.register(path_str, entry);

        native_bridge_common::log_debug!(
            "TieredObjectStore: put_opts registered LOCAL path='{}', size={}",
            path_str,
            size
        );
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: "TieredObjectStore does not support put_multipart_opts".into(),
        })
    }

    /// Primary read path: check registry for remote routing, otherwise local.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let path_str = location.as_ref();

        if let Some(guard) = self.registry.get(path_str) {
            if guard.location() == FileLocation::Remote {
                if let (Some(remote_path), Some(store)) =
                    (guard.remote_path(), guard.cached_store())
                {
                    native_bridge_common::log_debug!(
                        "TieredObjectStore: get_opts routing REMOTE path='{}'",
                        path_str
                    );
                    let rp = Path::from(remote_path);
                    let store: Arc<dyn ObjectStore> = Arc::clone(store);
                    // Drop guard before I/O — release happens on drop.
                    drop(guard);
                    return store.get_opts(&rp, options).await;
                }
            }
            // Local or Both — drop guard, fall through to local.
        }

        native_bridge_common::log_debug!(
            "TieredObjectStore: get_opts routing LOCAL path='{}'",
            path_str
        );
        self.local.get_opts(location, options).await
    }

    /// Range read: same routing as `get_opts`.
    async fn get_range(&self, location: &Path, range: Range<u64>) -> OsResult<Bytes> {
        let path_str = location.as_ref();

        if let Some(guard) = self.registry.get(path_str) {
            if guard.location() == FileLocation::Remote {
                if let (Some(remote_path), Some(store)) =
                    (guard.remote_path(), guard.cached_store())
                {
                    let rp = Path::from(remote_path);
                    let store: Arc<dyn ObjectStore> = Arc::clone(store);
                    drop(guard);
                    return store.get_range(&rp, range).await;
                }
            }
        }

        self.local.get_range(location, range).await
    }

    /// Multi-range read: same routing as `get_opts` for the entire batch.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        let path_str = location.as_ref();

        if let Some(guard) = self.registry.get(path_str) {
            if guard.location() == FileLocation::Remote {
                if let (Some(remote_path), Some(store)) =
                    (guard.remote_path(), guard.cached_store())
                {
                    let rp = Path::from(remote_path);
                    let store: Arc<dyn ObjectStore> = Arc::clone(store);
                    drop(guard);
                    return store.get_ranges(&rp, ranges).await;
                }
            }
        }

        self.local.get_ranges(location, ranges).await
    }

    /// Head: try local first, fall back to remote if not found locally.
    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        let path_str = location.as_ref();

        match self.local.head(location).await {
            Ok(meta) => return Ok(meta),
            Err(object_store::Error::NotFound { .. }) => {}
            Err(other) => return Err(other),
        }

        if let Some(guard) = self.registry.get(path_str) {
            if guard.location() == FileLocation::Remote {
                if let (Some(remote_path), Some(store)) =
                    (guard.remote_path(), guard.cached_store())
                {
                    let rp = Path::from(remote_path);
                    let store: Arc<dyn ObjectStore> = Arc::clone(store);
                    drop(guard);
                    return store.head(&rp).await;
                }
            }
        }

        Err(object_store::Error::NotFound {
            path: path_str.to_string(),
            source: "TieredObjectStore: not found locally or in registry".into(),
        })
    }

    /// Delete: remove from registry only, NO local delete.
    /// Local file deletion is handled by the Java layer (CompositeDirectory).
    // TODO: Consider deferred removal (schedule + sweep) instead of force-remove
    // when eviction lifecycle is added.
    async fn delete(&self, location: &Path) -> OsResult<()> {
        let path_str = location.as_ref();
        self.registry.remove(path_str, true);
        Ok(())
    }

    /// List: local entries first, then remote-only entries from registry.
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();
        let registry = Arc::clone(&self.registry);
        let local_stream = self.local.list(prefix);

        let remote_entries: Vec<OsResult<ObjectMeta>> = registry
            .entries_matching(&prefix_str)
            .into_iter()
            .filter(|(_, loc, _)| *loc == FileLocation::Remote)
            .map(|(path, _, size)| {
                Ok(ObjectMeta {
                    location: Path::from(path),
                    last_modified: chrono::DateTime::<chrono::Utc>::default(),
                    size: size.unwrap_or(0),
                    e_tag: None,
                    version: None,
                })
            })
            .collect();

        let remote_stream = futures::stream::iter(remote_entries);
        Box::pin(local_stream.chain(remote_stream))
    }

    /// List with delimiter: local entries first, then merge remote-only entries.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        let mut result = self.local.list_with_delimiter(prefix).await?;

        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();

        let local_paths: std::collections::HashSet<String> = result
            .objects
            .iter()
            .map(|m| m.location.as_ref().to_string())
            .collect();

        for (path, location, size) in self.registry.entries_matching(&prefix_str) {
            if location == FileLocation::Remote && !local_paths.contains(&path) {
                result.objects.push(ObjectMeta {
                    location: Path::from(path),
                    last_modified: chrono::DateTime::<chrono::Utc>::default(),
                    size: size.unwrap_or(0),
                    e_tag: None,
                    version: None,
                });
            }
        }

        Ok(result)
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> OsResult<()> {
        Err(object_store::Error::NotSupported {
            source: "TieredObjectStore does not support copy".into(),
        })
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> OsResult<()> {
        Err(object_store::Error::NotSupported {
            source: "TieredObjectStore does not support copy_if_not_exists".into(),
        })
    }

    async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> OsResult<()> {
        Err(object_store::Error::NotSupported {
            source: "TieredObjectStore does not support rename_if_not_exists".into(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::PutPayload;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    /// Helper: create a registry + tiered store backed by in-memory stores.
    fn setup() -> (
        Arc<TieredStorageRegistry>,
        Arc<InMemory>,
        Arc<InMemory>,
        TieredObjectStore,
    ) {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(InMemory::new());
        let remote = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(Arc::clone(&registry), Arc::clone(&local) as _);
        (registry, local, remote, tiered)
    }

    // -- Routing tests ------------------------------------------------------

    #[tokio::test]
    async fn test_get_opts_routes_to_remote_for_remote_file() {
        let (_registry, _local, remote, tiered) = setup();

        let remote_path = Path::from("remote/a.parquet");
        remote
            .put(&remote_path, PutPayload::from_static(b"remote-data"))
            .await
            .unwrap();

        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let result = tiered
            .get_opts(&Path::from("a.parquet"), GetOptions::default())
            .await
            .unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"remote-data");
    }

    #[tokio::test]
    async fn test_get_opts_routes_to_local_when_not_in_registry() {
        let (_registry, local, _remote, tiered) = setup();

        local
            .put(
                &Path::from("local.parquet"),
                PutPayload::from_static(b"local-data"),
            )
            .await
            .unwrap();

        let result = tiered
            .get_opts(&Path::from("local.parquet"), GetOptions::default())
            .await
            .unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"local-data");
    }

    #[tokio::test]
    async fn test_get_opts_routes_to_local_for_both_file() {
        let (_registry, local, remote, tiered) = setup();

        local
            .put(
                &Path::from("a.parquet"),
                PutPayload::from_static(b"local-data"),
            )
            .await
            .unwrap();
        remote
            .put(
                &Path::from("remote/a.parquet"),
                PutPayload::from_static(b"remote-data"),
            )
            .await
            .unwrap();

        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Both,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let result = tiered
            .get_opts(&Path::from("a.parquet"), GetOptions::default())
            .await
            .unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(
            bytes.as_ref(),
            b"local-data",
            "Both files should route to local"
        );
    }

    #[tokio::test]
    async fn test_get_opts_routes_to_local_for_local_file() {
        let (_registry, local, _remote, tiered) = setup();

        local
            .put(
                &Path::from("a.parquet"),
                PutPayload::from_static(b"local-data"),
            )
            .await
            .unwrap();

        tiered
            .register_file("a.parquet", FileLocation::Local, None, None)
            .unwrap();

        let result = tiered
            .get_opts(&Path::from("a.parquet"), GetOptions::default())
            .await
            .unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"local-data");
    }

    // -- Ref count balance --------------------------------------------------

    #[tokio::test]
    async fn test_successful_remote_read_releases_ref_count() {
        let (registry, _local, remote, tiered) = setup();

        remote
            .put(
                &Path::from("remote/a.parquet"),
                PutPayload::from_static(b"data"),
            )
            .await
            .unwrap();

        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let _ = tiered
            .get_opts(&Path::from("a.parquet"), GetOptions::default())
            .await
            .unwrap();

        // Guard was dropped, ref count should be 0.
        let guard = registry.get("a.parquet").unwrap();
        // This guard adds 1, so underlying should have been 0 before.
        assert_eq!(guard.ref_count(), 1);
    }

    // -- Head ---------------------------------------------------------------

    #[tokio::test]
    async fn test_head_returns_local_metadata() {
        let (_registry, local, _remote, tiered) = setup();

        local
            .put(&Path::from("a.parquet"), PutPayload::from_static(b"data"))
            .await
            .unwrap();

        let meta = tiered.head(&Path::from("a.parquet")).await.unwrap();
        assert_eq!(meta.size, 4);
    }

    #[tokio::test]
    async fn test_head_falls_back_to_remote() {
        let (_registry, _local, remote, tiered) = setup();

        remote
            .put(
                &Path::from("remote/a.parquet"),
                PutPayload::from_static(b"remote-data"),
            )
            .await
            .unwrap();

        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let meta = tiered.head(&Path::from("a.parquet")).await.unwrap();
        assert_eq!(meta.size, 11);
    }

    #[tokio::test]
    async fn test_head_not_found() {
        let (_registry, _local, _remote, tiered) = setup();
        let result = tiered.head(&Path::from("nonexistent")).await;
        assert!(result.is_err());
    }

    // -- Put ----------------------------------------------------------------

    #[tokio::test]
    async fn test_put_writes_local_and_registers() {
        let (registry, local, _remote, tiered) = setup();

        tiered
            .put_opts(
                &Path::from("new.parquet"),
                PutPayload::from_static(b"new-data"),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let result = local.get(&Path::from("new.parquet")).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"new-data");
        assert_eq!(registry.len(), 1);
    }

    #[tokio::test]
    async fn test_put_opts_caches_file_size() {
        let (registry, _local, _remote, tiered) = setup();

        tiered
            .put_opts(
                &Path::from("sized.parquet"),
                PutPayload::from_static(b"hello world"),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let entries = registry.entries_matching("sized.parquet");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].2, Some(11));
    }

    // -- Delete -------------------------------------------------------------

    #[tokio::test]
    async fn test_delete_removes_registry_entry_only() {
        let (registry, local, _remote, tiered) = setup();

        local
            .put(&Path::from("a.parquet"), PutPayload::from_static(b"data"))
            .await
            .unwrap();
        tiered
            .register_file("a.parquet", FileLocation::Local, None, None)
            .unwrap();

        tiered.delete(&Path::from("a.parquet")).await.unwrap();
        assert_eq!(registry.len(), 0, "registry entry should be removed");

        // Local file should still exist (delete only removes registry entry).
        let result = local.get(&Path::from("a.parquet")).await;
        assert!(result.is_ok(), "local file should still exist");
    }

    // -- put_multipart_opts -------------------------------------------------

    #[tokio::test]
    async fn test_put_multipart_opts_not_supported() {
        let (_registry, _local, _remote, tiered) = setup();
        let result = tiered
            .put_multipart_opts(&Path::from("a"), PutMultipartOptions::default())
            .await;
        assert!(matches!(
            result,
            Err(object_store::Error::NotSupported { .. })
        ));
    }

    // -- Range reads --------------------------------------------------------

    #[tokio::test]
    async fn test_get_range_from_remote() {
        let (_registry, _local, remote, tiered) = setup();

        remote
            .put(
                &Path::from("remote/a.parquet"),
                PutPayload::from_static(b"0123456789"),
            )
            .await
            .unwrap();

        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let bytes = tiered
            .get_range(&Path::from("a.parquet"), 2..5)
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), b"234");
    }

    #[tokio::test]
    async fn test_get_ranges_empty_returns_empty() {
        let (_registry, local, _remote, tiered) = setup();

        local
            .put(&Path::from("a.parquet"), PutPayload::from_static(b"data"))
            .await
            .unwrap();

        let result = tiered
            .get_ranges(&Path::from("a.parquet"), &[])
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_get_ranges_multiple_from_remote() {
        let (_registry, _local, remote, tiered) = setup();

        remote
            .put(
                &Path::from("remote/a.parquet"),
                PutPayload::from_static(b"0123456789"),
            )
            .await
            .unwrap();

        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let results = tiered
            .get_ranges(&Path::from("a.parquet"), &[0..3, 5..8])
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref(), b"012");
        assert_eq!(results[1].as_ref(), b"567");
    }

    // -- Copy/rename not supported ------------------------------------------

    #[tokio::test]
    async fn test_copy_returns_not_supported() {
        let (_registry, _local, _remote, tiered) = setup();
        let result = tiered.copy(&Path::from("a"), &Path::from("b")).await;
        assert!(matches!(
            result,
            Err(object_store::Error::NotSupported { .. })
        ));
    }

    #[tokio::test]
    async fn test_rename_returns_not_supported() {
        let (_registry, _local, _remote, tiered) = setup();
        let result = tiered
            .rename_if_not_exists(&Path::from("a"), &Path::from("b"))
            .await;
        assert!(matches!(
            result,
            Err(object_store::Error::NotSupported { .. })
        ));
    }

    // -- List tests ---------------------------------------------------------

    #[tokio::test]
    async fn test_list_includes_remote_only_files() {
        let (registry, local, remote, tiered) = setup();

        local
            .put(
                &Path::from("data/local.parquet"),
                PutPayload::from_static(b"local"),
            )
            .await
            .unwrap();

        remote
            .put(
                &Path::from("remote/evicted.parquet"),
                PutPayload::from_static(b"remote-data"),
            )
            .await
            .unwrap();
        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "data/evicted.parquet",
                FileLocation::Remote,
                Some("remote/evicted.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();
        registry.update("data/evicted.parquet", |e| e.size = Some(11));

        let results: Vec<ObjectMeta> = tiered
            .list(Some(&Path::from("data")))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let paths: Vec<String> = results.iter().map(|m| m.location.to_string()).collect();
        assert!(paths.contains(&"data/local.parquet".to_string()));
        assert!(paths.contains(&"data/evicted.parquet".to_string()));

        let evicted_meta = results
            .iter()
            .find(|m| m.location.as_ref() == "data/evicted.parquet")
            .unwrap();
        assert_eq!(evicted_meta.size, 11);
    }

    #[tokio::test]
    async fn test_list_no_duplicates_for_local_files() {
        let (_registry, local, _remote, tiered) = setup();

        local
            .put(
                &Path::from("data/a.parquet"),
                PutPayload::from_static(b"data"),
            )
            .await
            .unwrap();
        tiered
            .register_file("data/a.parquet", FileLocation::Local, None, None)
            .unwrap();

        let results: Vec<ObjectMeta> = tiered
            .list(Some(&Path::from("data")))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let count = results
            .iter()
            .filter(|m| m.location.as_ref() == "data/a.parquet")
            .count();
        assert_eq!(count, 1, "local file should appear exactly once");
    }

    #[tokio::test]
    async fn test_list_with_delimiter_includes_remote() {
        let (_registry, local, remote, tiered) = setup();

        local
            .put(
                &Path::from("data/local.parquet"),
                PutPayload::from_static(b"local"),
            )
            .await
            .unwrap();

        remote
            .put(
                &Path::from("remote/evicted.parquet"),
                PutPayload::from_static(b"remote-data"),
            )
            .await
            .unwrap();
        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "data/evicted.parquet",
                FileLocation::Remote,
                Some("remote/evicted.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let result = tiered
            .list_with_delimiter(Some(&Path::from("data")))
            .await
            .unwrap();

        let paths: Vec<String> = result
            .objects
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        assert!(paths.contains(&"data/local.parquet".to_string()));
        assert!(paths.contains(&"data/evicted.parquet".to_string()));
    }

    // -- Concurrency --------------------------------------------------------

    #[tokio::test]
    async fn test_concurrent_get_opts_on_same_remote_file() {
        let (registry, _local, remote, tiered) = setup();
        let tiered = Arc::new(tiered);

        remote
            .put(
                &Path::from("remote/a.parquet"),
                PutPayload::from_static(b"data"),
            )
            .await
            .unwrap();

        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let mut handles = Vec::new();
        for _ in 0..16 {
            let t = Arc::clone(&tiered);
            handles.push(tokio::spawn(async move {
                t.get_opts(&Path::from("a.parquet"), GetOptions::default())
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
            }));
        }

        for h in handles {
            let bytes = h.await.unwrap();
            assert_eq!(bytes.as_ref(), b"data");
        }

        // All reads done, ref count should be 0.
        let guard = registry.get("a.parquet").unwrap();
        assert_eq!(guard.ref_count(), 1); // Only this guard.
    }

    // -- Mock store for call tracking ---------------------------------------

    #[derive(Debug)]
    struct CallCountingStore {
        inner: InMemory,
        get_count: AtomicUsize,
    }

    impl CallCountingStore {
        fn new() -> Self {
            Self {
                inner: InMemory::new(),
                get_count: AtomicUsize::new(0),
            }
        }
    }

    impl fmt::Display for CallCountingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CallCountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CallCountingStore {
        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.get_count.fetch_add(1, AtomicOrdering::SeqCst);
            self.inner.get_opts(location, options).await
        }

        async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
            self.inner.head(location).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn delete(&self, location: &Path) -> OsResult<()> {
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }

        async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
            self.inner.rename_if_not_exists(from, to).await
        }
    }

    #[tokio::test]
    async fn test_mock_store_exactly_one_call_per_get_opts() {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(InMemory::new());
        let mock_remote = Arc::new(CallCountingStore::new());

        mock_remote
            .inner
            .put(
                &Path::from("remote/a.parquet"),
                PutPayload::from_static(b"data"),
            )
            .await
            .unwrap();

        let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
        tiered.add_store("repo1".into(), Arc::clone(&mock_remote) as _);
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let _ = tiered
            .get_opts(&Path::from("a.parquet"), GetOptions::default())
            .await
            .unwrap();

        assert_eq!(
            mock_remote.get_count.load(AtomicOrdering::SeqCst),
            1,
            "exactly 1 call to remote get_opts"
        );
    }

    // -- Error store --------------------------------------------------------

    #[derive(Debug)]
    struct ErrorStore;

    impl fmt::Display for ErrorStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "ErrorStore")
        }
    }

    #[async_trait]
    impl ObjectStore for ErrorStore {
        async fn get_opts(&self, _location: &Path, _options: GetOptions) -> OsResult<GetResult> {
            Err(object_store::Error::Generic {
                store: "ErrorStore",
                source: "simulated error".into(),
            })
        }

        async fn head(&self, _location: &Path) -> OsResult<ObjectMeta> {
            Err(object_store::Error::Generic {
                store: "ErrorStore",
                source: "simulated error".into(),
            })
        }

        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> OsResult<PutResult> {
            Err(object_store::Error::Generic {
                store: "ErrorStore",
                source: "simulated error".into(),
            })
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn MultipartUpload>> {
            Err(object_store::Error::Generic {
                store: "ErrorStore",
                source: "simulated error".into(),
            })
        }

        async fn delete(&self, _location: &Path) -> OsResult<()> {
            Err(object_store::Error::Generic {
                store: "ErrorStore",
                source: "simulated error".into(),
            })
        }

        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            futures::stream::empty().boxed()
        }

        async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> OsResult<ListResult> {
            Ok(ListResult {
                common_prefixes: vec![],
                objects: vec![],
            })
        }

        async fn copy(&self, _from: &Path, _to: &Path) -> OsResult<()> {
            Err(object_store::Error::NotSupported {
                source: "not supported".into(),
            })
        }

        async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> OsResult<()> {
            Err(object_store::Error::NotSupported {
                source: "not supported".into(),
            })
        }

        async fn rename_if_not_exists(&self, _from: &Path, _to: &Path) -> OsResult<()> {
            Err(object_store::Error::NotSupported {
                source: "not supported".into(),
            })
        }
    }

    #[tokio::test]
    async fn test_error_store_guard_still_releases() {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(InMemory::new());
        let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);

        let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
        tiered.add_store("repo1".into(), Arc::clone(&error_remote));
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let result = tiered
            .get_opts(&Path::from("a.parquet"), GetOptions::default())
            .await;
        assert!(result.is_err());

        // Guard was dropped before the remote call, so ref count should be 0.
        let guard = registry.get("a.parquet").unwrap();
        assert_eq!(guard.ref_count(), 1); // Only this guard.
    }

    // -- register_file validation -------------------------------------------

    #[test]
    fn test_register_file_remote_without_remote_path_returns_err() {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(registry, local as _);
        let result = tiered.register_file(
            "/a.parquet",
            FileLocation::Remote,
            None,
            Some("repo1".into()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_register_file_remote_without_repo_key_returns_err() {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(registry, local as _);
        let result = tiered.register_file(
            "/a.parquet",
            FileLocation::Remote,
            Some("remote/a".into()),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_register_file_remote_without_store_returns_err() {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(InMemory::new());
        let tiered = TieredObjectStore::new(registry, local as _);
        let result = tiered.register_file(
            "/a.parquet",
            FileLocation::Remote,
            Some("remote/a".into()),
            Some("missing_repo".into()),
        );
        assert!(result.is_err());
    }

    // -- Error / edge-case tests --------------------------------------------

    #[tokio::test]
    async fn test_failed_remote_read_not_found_still_completes() {
        let (registry, _local, remote, tiered) = setup();

        // Register a Remote file pointing to a path that doesn't exist on the remote store.
        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "missing.parquet",
                FileLocation::Remote,
                Some("remote/nonexistent.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let result = tiered
            .get_opts(&Path::from("missing.parquet"), GetOptions::default())
            .await;
        assert!(result.is_err(), "should error for non-existent remote path");

        // Registry entry still exists (guard was dropped before I/O, entry not removed).
        assert_eq!(registry.len(), 1);
        assert!(registry.get("missing.parquet").is_some());
    }

    #[tokio::test]
    async fn test_get_range_error_from_remote_still_completes() {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(InMemory::new());
        let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);

        let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
        tiered.add_store("repo1".into(), Arc::clone(&error_remote));
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let result = tiered.get_range(&Path::from("a.parquet"), 0..10).await;
        assert!(result.is_err(), "ErrorStore should return an error");

        // Registry entry still exists.
        assert_eq!(registry.len(), 1);
        assert!(registry.get("a.parquet").is_some());
    }

    #[tokio::test]
    async fn test_get_ranges_error_from_remote_still_completes() {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(InMemory::new());
        let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);

        let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
        tiered.add_store("repo1".into(), Arc::clone(&error_remote));
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let result = tiered
            .get_ranges(&Path::from("a.parquet"), &[0..5, 5..10])
            .await;
        assert!(result.is_err(), "ErrorStore should return an error");

        // Registry entry still exists.
        assert_eq!(registry.len(), 1);
        assert!(registry.get("a.parquet").is_some());
    }

    #[tokio::test]
    async fn test_head_remote_fallback_error_still_completes() {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(InMemory::new());
        let error_remote: Arc<dyn ObjectStore> = Arc::new(ErrorStore);

        // File not found locally. Register as Remote with ErrorStore.
        let tiered = TieredObjectStore::new(Arc::clone(&registry), local as _);
        tiered.add_store("repo1".into(), Arc::clone(&error_remote));
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        let result = tiered.head(&Path::from("a.parquet")).await;
        assert!(result.is_err(), "head should fail when remote errors");

        // Registry entry still exists.
        assert_eq!(registry.len(), 1);
        assert!(registry.get("a.parquet").is_some());
    }

    #[tokio::test]
    async fn test_concurrent_read_and_delete() {
        let (registry, _local, remote, tiered) = setup();
        let tiered = Arc::new(tiered);

        remote
            .put(
                &Path::from("remote/a.parquet"),
                PutPayload::from_static(b"data"),
            )
            .await
            .unwrap();

        tiered.add_store("repo1".into(), Arc::clone(&remote) as _);
        tiered
            .register_file(
                "a.parquet",
                FileLocation::Remote,
                Some("remote/a.parquet".into()),
                Some("repo1".into()),
            )
            .unwrap();

        // Spawn 16 concurrent reads.
        let mut handles = Vec::new();
        for _ in 0..16 {
            let t = Arc::clone(&tiered);
            handles.push(tokio::spawn(async move {
                // Each read may succeed or fail depending on timing with delete;
                // the important thing is no panic.
                let _ = t
                    .get_opts(&Path::from("a.parquet"), GetOptions::default())
                    .await;
            }));
        }

        // While reads are in flight, delete the file.
        tiered.delete(&Path::from("a.parquet")).await.unwrap();

        // All reads should complete (Arc keeps store alive). No panics.
        for h in handles {
            h.await.expect("task should not panic");
        }

        // After all reads finish, registry should have 0 entries.
        assert_eq!(registry.len(), 0);
    }

    // -- ReadGuard scope / ref-count tests ----------------------------------

    #[test]
    fn test_guard_releases_on_scope_exit() {
        let registry = TieredStorageRegistry::new();
        registry.register("a.parquet", local_entry());

        {
            let guard = registry.get("a.parquet").unwrap();
            assert_eq!(guard.ref_count(), 1);
            // guard drops here
        }

        // Get another guard — ref_count should be 1, not 2.
        let guard2 = registry.get("a.parquet").unwrap();
        assert_eq!(guard2.ref_count(), 1, "previous guard should have released");
    }

    #[test]
    fn test_delete_during_active_guard() {
        let registry = TieredStorageRegistry::new();
        registry.register("a.parquet", local_entry());

        // Simulate an active reader via manual acquire (doesn't hold DashMap Ref).
        registry.update("a.parquet", |e| {
            e.acquire();
        });

        // Force-remove while ref_count > 0.
        let removed = registry.remove("a.parquet", true);
        assert!(
            removed,
            "force remove should succeed even with active ref count"
        );

        // Entry is gone.
        assert_eq!(registry.len(), 0);

        // A subsequent get returns None — no crash.
        assert!(registry.get("a.parquet").is_none());
    }

    // Helper: create a local entry (reused by guard tests above).
    fn local_entry() -> TieredFileEntry {
        TieredFileEntry::new(FileLocation::Local, None, None, None, None)
    }
}
