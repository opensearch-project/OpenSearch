/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! All shared types for tiered storage — file entries, locations, errors,
//! and RAII read guards.

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use dashmap::mapref::one::Ref;
use object_store::ObjectStore;

// ---------------------------------------------------------------------------
// FileRegistryError
// ---------------------------------------------------------------------------

/// Errors from [`TieredStorageRegistry`](crate::registry::TieredStorageRegistry) operations.
#[derive(Debug, thiserror::Error)]
pub enum FileRegistryError {
    /// A required field was missing for the given location.
    #[error("FileRegistry: register_file failed for path='{path}': {reason}")]
    InvalidRegistration {
        /// The file path that failed registration.
        path: String,
        /// Human-readable reason.
        reason: String,
    },

    /// The requested operation is invalid for the current file state.
    #[error("FileRegistry: invalid state transition for path='{path}': {reason}")]
    InvalidStateTransition {
        /// The file path.
        path: String,
        /// Human-readable reason.
        reason: String,
    },
}

// ---------------------------------------------------------------------------
// FileLocation
// ---------------------------------------------------------------------------

/// Where a file's data currently resides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FileLocation {
    /// File exists only on local disk.
    Local = 0,
    /// File exists only on a remote object store.
    Remote = 1,
    /// File exists on both local disk and remote store.
    Both = 2,
}

impl fmt::Display for FileLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local => write!(f, "Local"),
            Self::Remote => write!(f, "Remote"),
            Self::Both => write!(f, "Both"),
        }
    }
}

impl FileLocation {
    /// Convert from a raw `u8` (used in the native bridge).
    ///
    /// Returns `None` for unrecognised values.
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Local),
            1 => Some(Self::Remote),
            2 => Some(Self::Both),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// TieredFileEntry
// ---------------------------------------------------------------------------

/// Per-file metadata stored in the registry.
///
/// Fields are ordered by alignment to minimise struct padding.
/// Ref counting is managed directly on the entry via `acquire()` / `release()`.
pub struct TieredFileEntry {
    /// Number of active readers. Atomic for lock-free concurrent access.
    pub(crate) active_reads: AtomicI64,
    /// Path on the remote store. Stored as `Arc<str>` for cheap cloning.
    pub(crate) remote_path: Option<Arc<str>>,
    /// Repository key for looking up the remote [`ObjectStore`].
    pub(crate) repo_key: Option<Arc<str>>,
    /// Remote [`ObjectStore`] reference, resolved at registration time.
    pub(crate) remote_store: Option<Arc<dyn ObjectStore>>,
    /// Cached file size in bytes (from head or put).
    pub(crate) size: Option<u64>,
    /// Current location of the file data.
    pub(crate) location: FileLocation,
}

impl fmt::Debug for TieredFileEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TieredFileEntry")
            .field("location", &self.location)
            .field("remote_path", &self.remote_path)
            .field("repo_key", &self.repo_key)
            .field(
                "remote_store",
                if self.remote_store.is_some() {
                    &"Some(...)" as &dyn fmt::Debug
                } else {
                    &"None" as &dyn fmt::Debug
                },
            )
            .field("active_reads", &self.active_reads.load(Ordering::SeqCst))
            .field("size", &self.size)
            .finish()
    }
}

impl TieredFileEntry {
    /// Create a new entry with the given location and zero active readers.
    pub fn new(
        location: FileLocation,
        remote_path: Option<Arc<str>>,
        repo_key: Option<String>,
        remote_store: Option<Arc<dyn ObjectStore>>,
        size: Option<u64>,
    ) -> Self {
        Self {
            active_reads: AtomicI64::new(0),
            remote_path,
            repo_key: repo_key.map(Arc::from),
            remote_store,
            size,
            location,
        }
    }

    /// Atomically increment the active reader count.
    pub fn acquire(&self) {
        self.active_reads.fetch_add(1, Ordering::SeqCst);
    }

    /// Atomically decrement the active reader count, clamped at zero.
    pub fn release(&self) {
        let result = self
            .active_reads
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                if current <= 0 {
                    None
                } else {
                    Some(current - 1)
                }
            });
        debug_assert!(
            result.is_ok(),
            "release() called with active_reads <= 0, mismatched acquire/release"
        );
    }

    /// Current active reader count.
    #[must_use]
    pub fn ref_count(&self) -> i64 {
        self.active_reads.load(Ordering::SeqCst)
    }

    /// Current location.
    #[must_use]
    pub fn location(&self) -> FileLocation {
        self.location
    }

    /// Remote path, if any.
    #[must_use]
    pub fn remote_path(&self) -> Option<&str> {
        self.remote_path.as_deref()
    }

    /// Repository key, if any.
    #[must_use]
    pub fn repo_key(&self) -> Option<&str> {
        self.repo_key.as_deref()
    }

    /// Remote [`ObjectStore`] reference, if any.
    #[must_use]
    pub fn remote_store(&self) -> Option<&Arc<dyn ObjectStore>> {
        self.remote_store.as_ref()
    }

    /// Cached file size.
    #[must_use]
    pub fn file_size(&self) -> Option<u64> {
        self.size
    }
}

// ---------------------------------------------------------------------------
// ReadGuard — RAII acquire/release via DashMap Ref
// ---------------------------------------------------------------------------

/// RAII guard that auto-acquires on creation and auto-releases on drop.
///
/// Holds a DashMap [`Ref`] to keep the entry alive for the duration of the
/// read. When the guard is dropped, the ref count is decremented.
pub struct ReadGuard<'a> {
    entry: Ref<'a, String, TieredFileEntry>,
}

impl<'a> ReadGuard<'a> {
    /// Create a guard, incrementing the ref count.
    pub(crate) fn new(entry: Ref<'a, String, TieredFileEntry>) -> Self {
        entry.value().acquire();
        Self { entry }
    }

    /// Access the underlying entry.
    pub fn value(&self) -> &TieredFileEntry {
        self.entry.value()
    }

    /// Current location of the file.
    pub fn location(&self) -> FileLocation {
        self.entry.value().location()
    }

    /// Remote path, if any.
    pub fn remote_path(&self) -> Option<&str> {
        self.entry.value().remote_path()
    }

    /// Remote [`ObjectStore`] reference, if any.
    pub fn remote_store(&self) -> Option<&Arc<dyn ObjectStore>> {
        self.entry.value().remote_store()
    }

    /// Current reference count (including this guard).
    pub fn ref_count(&self) -> i64 {
        self.entry.value().ref_count()
    }
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        self.entry.value().release();
    }
}

impl fmt::Debug for ReadGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadGuard")
            .field("location", &self.location())
            .field("remote_path", &self.remote_path())
            .field("ref_count", &self.ref_count())
            .finish()
    }
}
