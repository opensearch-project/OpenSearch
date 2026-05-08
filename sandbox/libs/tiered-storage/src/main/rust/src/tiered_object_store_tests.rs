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
    tiered.set_remote(Arc::clone(&remote) as _);
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

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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
        .register_file("a.parquet", FileLocation::Local, None)
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

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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

// -- Delete -------------------------------------------------------------

#[tokio::test]
async fn test_delete_removes_registry_entry_only() {
    let (registry, local, _remote, tiered) = setup();

    local
        .put(&Path::from("a.parquet"), PutPayload::from_static(b"data"))
        .await
        .unwrap();
    tiered
        .register_file("a.parquet", FileLocation::Local, None)
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

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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
    tiered
        .register_file(
            "data/evicted.parquet",
            FileLocation::Remote,
            Some("remote/evicted.parquet".into()),
        )
        .unwrap();

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
        .register_file("data/a.parquet", FileLocation::Local, None)
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
    tiered
        .register_file(
            "data/evicted.parquet",
            FileLocation::Remote,
            Some("remote/evicted.parquet".into()),
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

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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
    tiered.set_remote(Arc::clone(&mock_remote) as _);
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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
    tiered.set_remote(Arc::clone(&error_remote));
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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
    );
    assert!(result.is_err());
}

// -- Error / edge-case tests --------------------------------------------

#[tokio::test]
async fn test_failed_remote_read_not_found_still_completes() {
    let (registry, _local, _remote, tiered) = setup();

    // Register a Remote file pointing to a path that doesn't exist on the remote store.
    tiered
        .register_file(
            "missing.parquet",
            FileLocation::Remote,
            Some("remote/nonexistent.parquet".into()),
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
    tiered.set_remote(Arc::clone(&error_remote));
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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
    tiered.set_remote(Arc::clone(&error_remote));
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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
    tiered.set_remote(Arc::clone(&error_remote));
    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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

    tiered
        .register_file(
            "a.parquet",
            FileLocation::Remote,
            Some("remote/a.parquet".into()),
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
    TieredFileEntry::new(FileLocation::Local, None)
}
