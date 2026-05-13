# Directory Routing for DFA Shards

## 1. CLASS RELATIONSHIPS

```
RemoteSegmentStoreDirectory extends FilterDirectory
  └── wraps: RemoteDirectory remoteDataDirectory  (composition)
              ↑ polymorphic — either RemoteDirectory OR DataFormatAwareRemoteDirectory

DataFormatAwareRemoteDirectory extends RemoteDirectory
  └── owns: FormatBlobRouter formatBlobRouter  (composition)

DataFormatAwareStoreDirectory extends FilterDirectory
  └── wraps: SubdirectoryAwareDirectory  (which wraps the FSDirectory)

FormatBlobRouter — standalone routing class
  └── owns: BlobStore, BlobPath basePath, ConcurrentHashMap<format, BlobContainer>
```

**Key constructor params:**

- `RemoteSegmentStoreDirectory(RemoteDirectory remoteDataDirectory, RemoteDirectory remoteMetadataDirectory, ...)`
  - `RemoteSegmentStoreDirectoryFactory.java:175-188` — creates `DataFormatAwareRemoteDirectory` when `isPluggableDataFormatEnabled()`
  - RSSD extracts `formatBlobRouter` via `remoteDataDirectory.getFormatBlobRouter().orElse(null)` (line 179)

- `DataFormatAwareRemoteDirectory(BlobStore, BlobPath baseBlobPath, ..., DataFormatRegistry, IndexSettings)`
  - Creates `FormatBlobRouter(blobStore, baseBlobPath)` internally (line ~97)
  - Pre-registers format containers from `DataFormatRegistry`

- `DataFormatAwareStoreDirectory(Directory delegate, ShardPath, Map<String, FormatChecksumStrategy>)`
  - Wraps delegate in `SubdirectoryAwareDirectory` for local path routing

- `FormatBlobRouter(BlobStore blobStore, BlobPath basePath)`
  - "lucene"/"metadata" → baseContainer (same path)
  - Other formats → `basePath/formatName/` sub-path (lazy creation)

## 2. FILE FLOW on Refresh/Flush

### Upload path (single path sees BOTH formats):

```
RemoteStoreRefreshListener.uploadNewSegments()
  → RemoteStoreUploaderService.uploadSegments(filteredFiles, ...)
    → for each file: remoteDirectory.copyFrom(storeDirectory, localSegment, ...)
```
`remoteDirectory` = `RemoteSegmentStoreDirectory` (RSSD)

**RSSD.copyFrom** (line 636-663):
```java
String remoteFileName = getNewRemoteSegmentFilename(src);  // strips "parquet/" prefix, appends __UUID
uploaded = remoteDataDirectory.copyFrom(from, src, remoteFileName, context, postUploadRunner, ...);
```

**DataFormatAwareRemoteDirectory.copyFrom** (line 248-268):
```java
FileMetadata fileMetadata = new FileMetadata(src);  // parses "parquet/_0.pqt" → format="parquet"
BlobContainer container = getBlobContainerForFormat(fileMetadata.dataFormat());
// lucene → basePath, parquet → basePath/parquet/
```

### Parquet file: `"parquet/_0_1.parquet"`
1. `DataFormatAwareStoreDirectory` resolves local path: `<shard>/parquet/_0_1.parquet`
2. RSSD strips prefix → remote blob key: `_0_1.parquet__UUID`
3. `DataFormatAwareRemoteDirectory` routes to `basePath/parquet/` container

### Lucene file: `"_0.cfs"`
1. `DataFormatAwareStoreDirectory` resolves local path: `<shard>/index/_0.cfs`
2. RSSD → remote blob key: `_0.cfs__UUID`
3. `DataFormatAwareRemoteDirectory` routes to `basePath/` (base container)

### Single upload path?
**YES** — both parquet and lucene files flow through the same `RSSD.copyFrom → DataFormatAwareRemoteDirectory.copyFrom` path. Format routing is determined by parsing the `src` string's prefix.

## 3. Who invokes `uploadMetadata`?

`RemoteStoreRefreshListener.uploadMetadata()` (line 480-503) is the **only** caller:

```java
// RemoteStoreRefreshListener.java:496
remoteDirectory.uploadMetadata(localSegmentsPostRefresh, catalogSnapshotCloned, storeDirectory, ...)
```

`remoteDirectory` here is `RemoteSegmentStoreDirectory`. Neither `DataFormatAwareRemoteDirectory` nor `DataFormatAwareStoreDirectory` invokes `uploadMetadata` — they are downstream of it.

## 4. Read/Download: DataFormatAwareRemoteDirectory.openInput

**YES, format-aware dispatch exists** (line 439-462):

```java
public IndexInput openInput(String name, long fileLength, IOContext context) throws IOException {
    String format = resolveFormat(name);  // looks up blobFormatCache: blobKey → format
    BlobContainer container = getBlobContainerForFormat(format);
    InputStream inputStream = container.readBlob(name);
    return new RemoteIndexInput(name, rateLimiter.apply(inputStream), fileLength);
}
```

The `resolveFormat(name)` delegates to `FormatBlobRouter.resolveFormat()` which uses the `blobFormatCache` (a `Map<blobKey, format>` populated during upload via `registerBlobFormat`). If not found, defaults to "lucene".

Same pattern for `openBlockInput` (line 474-494) and `fileLength` (line 283-295).

## 5. CatalogSnapshot Type During uploadMetadata

**For DFA shards: `DataformatAwareCatalogSnapshot`**

Evidence:
- `IndexShard.getCatalogSnapshot()` → `getIndexer().acquireSnapshot()` (IndexShard.java:6090)
- For DFA engines, indexer is backed by `DataFormatAwareEngine` which uses `CatalogSnapshotManager`
- `CatalogSnapshotManager` line 221: `newSnapshot = new DataformatAwareCatalogSnapshot(...)`
- The manager's `latestCatalogSnapshot` is always a `DataformatAwareCatalogSnapshot` for DFA shards

**How determined:** The engine type (`DataFormatAwareEngine` vs `InternalEngine`) determines which `CatalogSnapshotManager` is used. DFA engines always produce `DataformatAwareCatalogSnapshot`. Standard engines produce `SegmentInfosCatalogSnapshot`.

**Note on `getFormatVersionForFile`:** Currently returns `Version.LATEST` unconditionally (DataformatAwareCatalogSnapshot.java:241-244) with a TODO comment: "return the per-file format version once per-segment tracking is available."
