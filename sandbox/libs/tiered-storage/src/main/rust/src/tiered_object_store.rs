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
//! - **Remote** → delegates to the store-level remote backend
//! - **Local / Both / not registered** → falls through to the local store
//!
//! # Thread Safety
//!
//! `TieredObjectStore` is `Send + Sync`. All mutable state lives in the
//! registry's atomics and DashMap — no locks are held during I/O.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
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
/// Per-shard model: one remote store is set once via [`set_remote()`] and
/// shared across all entries.
pub struct TieredObjectStore {
    registry: Arc<TieredStorageRegistry>,
    local: Arc<dyn ObjectStore>,
    remote: std::sync::OnceLock<Arc<dyn ObjectStore>>,
}

impl TieredObjectStore {
    /// Create a new tiered store routing between `local` and remote backends.
    #[must_use]
    pub fn new(registry: Arc<TieredStorageRegistry>, local: Arc<dyn ObjectStore>) -> Self {
        native_bridge_common::log_info!("TieredObjectStore: created");
        Self {
            registry,
            local,
            remote: std::sync::OnceLock::new(),
        }
    }

    /// Reference to the underlying registry.
    #[must_use]
    pub fn registry(&self) -> &Arc<TieredStorageRegistry> {
        &self.registry
    }

    /// Set the remote store (once). Subsequent calls are ignored.
    pub fn set_remote(&self, store: Arc<dyn ObjectStore>) {
        self.remote.set(store).ok(); // ignore if already set
    }

    /// Register a file in the registry. For Remote/Both locations, the caller
    /// must provide a `remote_path`.
    pub fn register_file(
        &self,
        path: &str,
        location: FileLocation,
        remote_path: Option<String>,
    ) -> Result<(), crate::types::FileRegistryError> {
        if matches!(location, FileLocation::Remote) && remote_path.is_none() {
            return Err(crate::types::FileRegistryError::InvalidRegistration {
                path: path.to_string(),
                reason: format!("remote_path required for location={}", location),
            });
        }

        let entry = TieredFileEntry::new(location, remote_path.map(Arc::from));
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
    ) -> Result<(), crate::types::FileRegistryError> {
        if matches!(location, FileLocation::Remote) && remote_path.is_none() {
            return Err(crate::types::FileRegistryError::InvalidRegistration {
                path: path.to_string(),
                reason: format!("remote_path required for location={}", location),
            });
        }

        let remote_arc: Option<Arc<str>> = remote_path.map(Arc::from);

        self.registry.update(path, move |e| {
            e.location = location;
            e.remote_path = remote_arc;
        });

        native_bridge_common::log_debug!(
            "TieredObjectStore: transition path='{}', location={}",
            path,
            location
        );
        Ok(())
    }

    // TODO: Add pin(path)/unpin(path) methods for write-path eviction protection.
    // TODO: Add schedule_eviction(path) and sweep() for deferred eviction lifecycle.

    // NOTE: The guard is intentionally dropped before I/O. The Arc<dyn ObjectStore>
    // keeps the store alive independently. On writable warm, the guard must be held
    // during I/O to prevent eviction race — resolve_remote should return the guard
    // alongside the resolved path/store to pin the entry for the I/O duration.
    fn resolve_remote(&self, path: &str) -> Option<(Path, Arc<dyn ObjectStore>)> {
        let guard = self.registry.get(path)?;
        if guard.location() != FileLocation::Remote {
            return None;
        }
        let remote_path = guard.remote_path()?;
        let store = Arc::clone(self.remote.get()?); // use store-level remote
        let rp = Path::from(remote_path);
        drop(guard); // release before I/O — Arc keeps store alive
        Some((rp, store))
    }

    /// Checks if a local read error is NotFound and the file has since transitioned
    /// to REMOTE in the registry (e.g., afterSyncToRemote deleted the local copy).
    /// Returns the remote path + store if retry is possible, None otherwise.
    fn should_retry_remote(&self, path_str: &str, err: &object_store::Error) -> Option<(Path, Arc<dyn ObjectStore>)> {
        if matches!(err, object_store::Error::NotFound { .. }) {
            let resolved = self.resolve_remote(path_str);
            if resolved.is_some() {
                native_bridge_common::log_info!(
                    "TieredObjectStore: LOCAL NotFound, file transitioned to REMOTE — retrying path='{}'",
                    path_str
                );
            }
            resolved
        } else {
            None
        }
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
    /// On writable warm, caller must pin the file to prevent eviction before
    /// sync completes.
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        let result = self.local.put_opts(location, payload, opts).await?;

        let path_str = location.as_ref();
        let entry = TieredFileEntry::new(FileLocation::Local, None);
        self.registry.register(path_str, entry);

        native_bridge_common::log_debug!(
            "TieredObjectStore: put_opts registered LOCAL path='{}'",
            path_str,
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
    /// If local read fails with NotFound and file transitioned to REMOTE, retries from remote.
    ///
    /// Also handles head requests (options.head == true) by returning cached
    /// size from the registry when available — avoids I/O for the common case.
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        let path_str = location.as_ref();

        // Fast path for head: return cached size from registry if available
        if options.head {
            if let Some(guard) = self.registry.get(path_str) {
                let size = guard.size();
                if size > 0 {
                    let meta = ObjectMeta {
                        location: location.clone(),
                        last_modified: chrono::DateTime::<chrono::Utc>::default(),
                        size,
                        e_tag: None,
                        version: None,
                    };
                    return Ok(GetResult {
                        payload: object_store::GetResultPayload::Stream(
                            futures::stream::empty().boxed(),
                        ),
                        meta,
                        range: 0..size,
                        attributes: Default::default(),
                    });
                }
            }
        }

        if let Some((rp, store)) = self.resolve_remote(path_str) {
            native_bridge_common::log_debug!(
                "TieredObjectStore: get_opts REMOTE path='{}'",
                path_str
            );
            return store.get_opts(&rp, options).await;
        }

        let result = self.local.get_opts(location, options.clone()).await;
        if let Err(ref e) = result {
            if let Some((rp, store)) = self.should_retry_remote(path_str, e) {
                return store.get_opts(&rp, options).await;
            }
        }
        result
    }

    /// Delete stream: remove each path from registry only, NO local delete.
    /// Local file deletion is handled by the Java layer.
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        let registry = Arc::clone(&self.registry);
        let mapped = locations.map(move |result| {
            if let Ok(ref path) = result {
                registry.remove(path.as_ref(), true);
            }
            result
        });
        Box::pin(mapped)
    }

    /// List: local entries first, then remote-only entries from registry (deduplicated).
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
                    size,
                    e_tag: None,
                    version: None,
                })
            })
            .collect();

        let remote_stream = futures::stream::iter(remote_entries);
        Box::pin(local_stream.chain(remote_stream))
    }

    /// List with delimiter: local entries first, then merge remote-only entries (deduplicated).
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
                    size,
                    e_tag: None,
                    version: None,
                });
            }
        }

        Ok(result)
    }

    async fn copy_opts(&self, _from: &Path, _to: &Path, _options: CopyOptions) -> OsResult<()> {
        Err(object_store::Error::NotSupported {
            source: "TieredObjectStore does not support copy".into(),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tiered_object_store_tests.rs"]
mod tests;
