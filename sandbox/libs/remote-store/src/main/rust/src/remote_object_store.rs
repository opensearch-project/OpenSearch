/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Logging and error-normalizing wrapper around any [`ObjectStore`] backend.
//!
//! Every remote backend created by the [`factory`](crate::factory) is wrapped
//! in this before being returned. Adds debug logging with repo_key context
//! on every operation.

use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

/// Wrapper that adds debug logging and error context to any [`ObjectStore`].
pub struct RemoteObjectStore {
    inner: Arc<dyn ObjectStore>,
    repo_key: String,
}

impl RemoteObjectStore {
    /// Wrap an existing store with logging and error normalisation.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, repo_key: String) -> Self {
        Self { inner, repo_key }
    }

    /// The repository key for this store.
    #[must_use]
    pub fn repo_key(&self) -> &str {
        &self.repo_key
    }
}

impl fmt::Debug for RemoteObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteObjectStore")
            .field("repo_key", &self.repo_key)
            .finish()
    }
}

impl fmt::Display for RemoteObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RemoteObjectStore(repo_key={})", self.repo_key)
    }
}

#[async_trait]
impl ObjectStore for RemoteObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: put_opts path='{}'",
            self.repo_key,
            location
        );
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: put_multipart_opts path='{}'",
            self.repo_key,
            location
        );
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: get_opts path='{}'",
            self.repo_key,
            location
        );
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> OsResult<Bytes> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: get_range path='{}' range={}..{}",
            self.repo_key,
            location,
            range.start,
            range.end
        );
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OsResult<Vec<Bytes>> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: get_ranges path='{}' count={}",
            self.repo_key,
            location,
            ranges.len()
        );
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> OsResult<ObjectMeta> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: head path='{}'",
            self.repo_key,
            location
        );
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> OsResult<()> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: delete path='{}'",
            self.repo_key,
            location
        );
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: list prefix='{:?}'",
            self.repo_key,
            prefix
        );
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: list_with_delimiter prefix='{:?}'",
            self.repo_key,
            prefix
        );
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> OsResult<()> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: copy from='{}' to='{}'",
            self.repo_key,
            from,
            to
        );
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: copy_if_not_exists from='{}' to='{}'",
            self.repo_key,
            from,
            to
        );
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> OsResult<()> {
        native_bridge_common::log_debug!(
            "RemoteObjectStore[{}]: rename_if_not_exists from='{}' to='{}'",
            self.repo_key,
            from,
            to
        );
        self.inner.rename_if_not_exists(from, to).await
    }
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    fn make_store(repo_key: &str) -> RemoteObjectStore {
        RemoteObjectStore::new(Arc::new(InMemory::new()), repo_key.to_string())
    }

    #[test]
    fn test_repo_key_accessor() {
        let store = make_store("my-repo");
        assert_eq!(store.repo_key(), "my-repo");
    }

    #[test]
    fn test_debug_includes_repo_key() {
        let store = make_store("debug-repo");
        let debug = format!("{:?}", store);
        assert!(debug.contains("debug-repo"));
    }

    #[test]
    fn test_display_includes_repo_key() {
        let store = make_store("display-repo");
        let display = format!("{}", store);
        assert!(display.contains("display-repo"));
    }

    #[tokio::test]
    async fn test_put_and_get_delegates_to_inner() {
        let store = make_store("test-repo");
        let path = Path::from("test.parquet");

        store
            .put_opts(&path, PutPayload::from_static(b"hello"), PutOptions::default())
            .await
            .unwrap();

        let result = store
            .get_opts(&path, GetOptions::default())
            .await
            .unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn test_head_delegates_to_inner() {
        let store = make_store("test-repo");
        let path = Path::from("test.parquet");

        store
            .put_opts(&path, PutPayload::from_static(b"data"), PutOptions::default())
            .await
            .unwrap();

        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 4);
    }

    #[tokio::test]
    async fn test_delete_delegates_to_inner() {
        let store = make_store("test-repo");
        let path = Path::from("test.parquet");

        store
            .put_opts(&path, PutPayload::from_static(b"data"), PutOptions::default())
            .await
            .unwrap();

        store.delete(&path).await.unwrap();
        assert!(store.head(&path).await.is_err());
    }

    #[tokio::test]
    async fn test_get_range_delegates_to_inner() {
        let store = make_store("test-repo");
        let path = Path::from("test.parquet");

        store
            .put_opts(
                &path,
                PutPayload::from_static(b"0123456789"),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let bytes = store.get_range(&path, 2..5).await.unwrap();
        assert_eq!(bytes.as_ref(), b"234");
    }

    #[tokio::test]
    async fn test_get_ranges_delegates_to_inner() {
        let store = make_store("test-repo");
        let path = Path::from("test.parquet");

        store
            .put_opts(
                &path,
                PutPayload::from_static(b"0123456789"),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let results = store.get_ranges(&path, &[0..3, 7..10]).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref(), b"012");
        assert_eq!(results[1].as_ref(), b"789");
    }

    #[tokio::test]
    async fn test_list_delegates_to_inner() {
        use futures::StreamExt;

        let store = make_store("test-repo");
        store
            .put_opts(
                &Path::from("a.parquet"),
                PutPayload::from_static(b"a"),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let results: Vec<_> = store
            .list(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(results.len(), 1);
    }
}
