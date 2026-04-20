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
//! On every read, it checks the file registry:
//! - **Remote** → delegates to the remote backend via the store in the entry
//! - **Local / Both / not registered** → falls through to the local store
//!
//! # Thread Safety
//!
//! `TieredObjectStore` is `Send + Sync`. All mutable state lives in the
//! registry's atomics and DashMap — no locks are held during I/O.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

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
/// File tracking is delegated to the registry. Remote stores are passed
/// directly when registering files.
pub struct TieredObjectStore {
    registry: Arc<TieredStorageRegistry>,
    local: Arc<dyn ObjectStore>,
}

impl TieredObjectStore {
    /// Create a new tiered store routing between `local` and remote backends.
    #[must_use]
    pub fn new(registry: Arc<TieredStorageRegistry>, local: Arc<dyn ObjectStore>) -> Self {
        native_bridge_common::log_info!("TieredObjectStore: created");
        Self { registry, local }
    }

    /// Reference to the underlying registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<TieredStorageRegistry> {
        &self.registry
    }

    /// Register a file in the registry. For Remote/Both locations, the caller
    /// must provide the resolved `store` directly.
    pub fn register_file(
        &self,
        path: &str,
        location: FileLocation,
        remote_path: Option<String>,
        repo_key: Option<String>,
        store: Option<Arc<dyn ObjectStore>>,
    ) -> Result<(), crate::types::FileRegistryError> {
        // Validate: Remote/Both requires remote_path + repo_key + store.
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
            if store.is_none() {
                return Err(crate::types::FileRegistryError::InvalidRegistration {
                    path: path.to_string(),
                    reason: format!("store required for location={}", location),
                });
            }
        }

        let remote_arc: Option<Arc<str>> = remote_path.map(Arc::from);

        let entry = TieredFileEntry::new(location, remote_arc, repo_key, store, None);
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
        store: Option<Arc<dyn ObjectStore>>,
    ) {
        let remote_arc: Option<Arc<str>> = remote_path.map(Arc::from);
        let repo_arc: Option<Arc<str>> = repo_key.map(Arc::from);

        self.registry.update(path, move |e| {
            e.location = location;
            e.remote_path = remote_arc;
            e.repo_key = repo_arc;
            e.remote_store = store;
        });

        native_bridge_common::log_debug!(
            "TieredObjectStore: transition path='{}', location={}",
            path,
            location
        );
    }

    // TODO: Add pin(path)/unpin(path) methods for write-path eviction protection.
    // TODO: Add schedule_eviction(path) and sweep() for deferred eviction lifecycle.

    // NOTE: The guard is intentionally dropped before I/O. The Arc<dyn ObjectStore>
    // keeps the store alive independently. If eviction lifecycle is added in the future,
    // this method should return the guard alongside the resolved path/store to pin the
    // entry for the duration of the I/O operation.
    fn resolve_remote(&self, path: &str) -> Option<(Path, Arc<dyn ObjectStore>)> {
        let guard = self.registry.get(path)?;
        if guard.location() != FileLocation::Remote {
            return None;
        }
        let remote_path = guard.remote_path()?;
        let store = Arc::clone(guard.remote_store()?);
        let rp = Path::from(remote_path);
        drop(guard); // release before I/O — Arc keeps store alive
        Some((rp, store))
    }
}

impl fmt::Debug for TieredObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TieredObjectStore")
            .field("file_count", &self.registry.len())
            .finish()
    }
}

impl fmt::Display for TieredObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TieredObjectStore(files={})", self.registry.len())
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

        if let Some((rp, store)) = self.resolve_remote(path_str) {
            native_bridge_common::log_debug!(
                "TieredObjectStore: get_opts routing REMOTE path='{}'",
                path_str
            );
            return store.get_opts(&rp, options).await;
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

        if let Some((rp, store)) = self.resolve_remote(path_str) {
            return store.get_range(&rp, range).await;
        }

        self.local.get_range(location, range).await
    }

    /// Multi-range read: same routing as `get_opts` for the entire batch.
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        let path_str = location.as_ref();

        if let Some((rp, store)) = self.resolve_remote(path_str) {
            return store.get_ranges(&rp, ranges).await;
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

        if let Some((rp, store)) = self.resolve_remote(path_str) {
            return store.head(&rp).await;
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
#[path = "tiered_object_store_tests.rs"]
mod tests;
