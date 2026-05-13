# DFA ↔ Snapshot V2 Integration Analysis

## Part 1: DFA Remote Storage Layout

### 1.1 Blob Path Structure (FormatBlobRouter)

**File**: `server/src/main/java/org/opensearch/index/store/remote/FormatBlobRouter.java`

The `FormatBlobRouter` routes blob operations to format-specific `BlobContainer`s:

```
<repo-base>/<index-uuid>/<shard>/segments/data/       ← basePath (Lucene files)
<repo-base>/<index-uuid>/<shard>/segments/parquet/    ← sibling of data/
<repo-base>/<index-uuid>/<shard>/segments/<format>/   ← one sibling per non-Lucene format
```

Key routing logic (`createFormatContainer`, line ~220):
```java
BlobPath formatPath = Objects.requireNonNull(basePath.parent()).add(format.toLowerCase(Locale.ROOT));
```
- `basePath` = `segments/data` → `parent()` = `segments/` → appends format name
- "lucene" and "metadata" formats route to `baseContainer` (segments/data/)
- All other formats get sibling containers under `segments/`

### 1.2 Upload Pipeline

**Upload flow**: `RemoteStoreRefreshListener.syncSegments()` (line 223) →
1. Gets `CatalogSnapshot` via `indexShard.getCatalogSnapshot()`
2. Calls `catalogSnapshot.getFiles(true)` → returns format-prefixed filenames (e.g., `"parquet/_0.pqt"`)
3. Calls `uploadNewSegments(localSegmentsPostRefresh, ...)` → delegates to `remoteStoreUploader.uploadSegments()`
4. On success, calls `uploadMetadata(localSegmentsPostRefresh, catalogSnapshot, checkpoint)`

**DataformatAwareCatalogSnapshot.getFiles()** (line ~240 of that file):
```java
public Collection<String> getFiles(boolean includeSegmentsFile) throws IOException {
    for (Segment segment : segments) {
        for (Map.Entry<String, WriterFileSet> entry : segment.dfGroupedSearchableFiles().entrySet()) {
            String formatName = entry.getKey();
            for (String file : entry.getValue().files()) {
                fileNames.add(FileMetadata.serialize(formatName, file));
            }
        }
    }
}
```
This returns format-prefixed names like `"parquet/_0.pqt"` for non-lucene files.

**DataFormatAwareRemoteDirectory.copyFrom()** (line ~230 of that file):
- Parses `src` (e.g., `"parquet/_0.pqt"`) via `FileMetadata` to extract format
- Routes upload to `getBlobContainerForFormat(fileMetadata.dataFormat())` → correct sibling container

**RemoteSegmentStoreDirectory.getNewRemoteSegmentFilename()** (line ~1080):
```java
String plainFilename = FileMetadata.parseFile(localFilename);
return plainFilename + SEGMENT_NAME_UUID_SEPARATOR + UUIDs.base64UUID();
```
Strips format prefix before creating blob key: `"parquet/_0.pqt"` → `"_0.pqt__UUID"`

### 1.3 Metadata Tracking

**YES — DFA files ARE included in the remote metadata file.**

`RemoteSegmentStoreDirectory.uploadMetadata()` (CatalogSnapshot variant, line 853):
- Iterates over ALL `segmentFiles` (which includes format-prefixed names like `"parquet/_0.pqt"`)
- Looks up each in `segmentsUploadedToRemoteStore` map
- Writes `UploadedSegmentMetadata` (originalFilename::uploadedFilename::checksum::length::writtenByMajor)

The `UploadedSegmentMetadata.originalFilename` stores the format-prefixed name (e.g., `"parquet/_0.pqt"`).
The `uploadedFilename` is the plain blob key (e.g., `"_0.pqt__UUID"`).

**Format cache sync** (`syncBlobFormatCache`, line ~1059):
```java
for (UploadedSegmentMetadata metadata : segmentsUploadedToRemoteStore.values()) {
    blobKeyToFormat.put(metadata.getUploadedFilename(), extractFormat(metadata.getOriginalFilename()));
}
```
This rebuilds the reverse lookup (blob key → format) from metadata, enabling download/delete routing.

---

## Part 2: Snapshot V2 Integration Points (Lucene Reference)

### 2.1 initializeToSpecificTimestamp (line 232)

```java
public RemoteSegmentMetadata initializeToSpecificTimestamp(long timestamp) throws IOException
```
1. Lists ALL metadata files from `remoteMetadataDirectory` (prefix `metadata_`)
2. Calls `RemoteStoreUtils.getPinnedTimestampLockedFiles(metadataFiles, Set.of(timestamp), ...)` to find the metadata file whose timestamp is ≤ the pinned timestamp
3. Reads that metadata file → `RemoteSegmentMetadata` containing the `Map<String, UploadedSegmentMetadata>`
4. Calls `replaceUploadedSegments(remoteSegmentMetadata.getMetadata())` which also calls `syncBlobFormatCache()`

**Key insight**: The returned `RemoteSegmentMetadata` contains ALL files (including DFA format-prefixed ones) because `uploadMetadata` includes them. The `syncBlobFormatCache()` call rebuilds the format routing cache from `originalFilename` → format.

### 2.2 getPinnedTimestampLockedFiles (line 459)

Protects **metadata files** (not individual blobs). For each pinned timestamp, finds the metadata file with the closest preceding timestamp. The metadata file implicitly protects all blobs it references (because GC only deletes blobs not referenced by any active/locked metadata file).

### 2.3 recoverShallowSnapshotV2 (StoreRecovery.java, line 465)

1. Creates `RemoteSegmentStoreDirectory` for the source index
2. Calls `sourceRemoteDirectory.initializeToSpecificTimestamp(recoverySource.pinnedTimestamp())`
3. Calls `indexShard.syncSegmentsFromGivenRemoteSegmentStore(true, sourceRemoteDirectory, remoteSegmentMetadata, true)`
4. `syncSegmentsFromGivenRemoteSegmentStore` iterates `uploadedSegments` map and downloads each file via `fileDownloader.download()`

**Critical**: The download path uses `sourceRemoteDirectory` which has a `remoteDataDirectory`. If that directory is a `DataFormatAwareRemoteDirectory`, it can route downloads to format-specific containers. If it's a plain `RemoteDirectory`, it can only read from `segments/data/`.

### 2.4 cleanUpRemoteStoreFilesForDeletedIndicesV2 (BlobStoreRepository.java, line 1465)

1. Iterates deleted indices
2. Calls `cleanRemoteStoreDirectoryIfNeeded()` → `remoteDirectoryCleanupAsync()`
3. Creates a `RemoteSegmentStoreDirectory` and calls either `delete()` or `deleteStaleSegments(0)`
4. `delete()` calls `remoteDataDirectory.delete()` which in `DataFormatAwareRemoteDirectory` iterates ALL registered format containers and deletes them

---

## Part 3: Gap Analysis

### Gap (a): DFA files ARE referenced in metadata — NO GAP here

**Evidence**: `uploadMetadata()` at line 853 includes all files from `catalogSnapshot.getFiles(true)` which returns format-prefixed names. The `segmentsUploadedToRemoteStore` map keys include `"parquet/_0.pqt"`. The pinned timestamp mechanism protects the metadata file, which implicitly protects all referenced blobs.

**Verdict**: ✅ Covered. Pinned timestamps protect DFA files via metadata references.

### Gap (b): GC of sibling format containers — PARTIAL GAP

**Evidence**: `deleteStaleSegments()` (line 1152) calls `remoteDataDirectory.deleteFiles(filesToDelete)` at line 1261. In `DataFormatAwareRemoteDirectory.deleteFiles()`, it broadcasts deletes to ALL registered format containers:
```java
super.deleteFiles(names);  // base container
for (String format : formatBlobRouter.registeredFormats()) {
    formatBlobRouter.containerFor(format).deleteBlobsIgnoringIfNotExists(names);
}
```

**Verdict**: ✅ Covered for normal GC. The broadcast delete handles format containers.

**BUT**: The `registeredFormats()` only returns formats that have been lazily accessed. If a `RemoteSegmentStoreDirectory` is created fresh for GC (as in `cleanRemoteStoreDirectoryIfNeeded`), the format containers may not be registered.

**Gap**: `RemoteSegmentStoreDirectoryFactory.newDirectory()` may not create a `DataFormatAwareRemoteDirectory` when called from the snapshot cleanup path. Need to verify the factory creates format-aware directories for DFA indices during cleanup.

**File**: `server/src/main/java/org/opensearch/index/store/RemoteSegmentStoreDirectory.java:1323` (`remoteDirectoryCleanup`)
**Symptom**: Orphan parquet blobs left in `segments/parquet/` after index deletion if the cleanup path uses a plain `RemoteDirectory`.

### Gap (c): initializeToSpecificTimestamp returns data enabling parquet download — CONDITIONAL

**Evidence**: `initializeToSpecificTimestamp` (line 232) calls `replaceUploadedSegments()` → `syncBlobFormatCache()`. This rebuilds the format cache from `originalFilename` in the metadata. Subsequent downloads via `copySegmentFiles` → `fileDownloader.download()` will use the `remoteDataDirectory` which, if it's a `DataFormatAwareRemoteDirectory`, can route to the correct container.

**Gap**: The `RemoteSegmentStoreDirectoryFactory` used in `recoverShallowSnapshotV2` (StoreRecovery.java:498) calls `directoryFactory.newDirectory(...)` with parameters that may NOT include `DataFormatRegistry` or `IndexSettings` needed to create a `DataFormatAwareRemoteDirectory`.

**File**: `server/src/main/java/org/opensearch/index/shard/StoreRecovery.java:498`
**Symptom**: Restore of DFA index from V2 snapshot fails with `NoSuchFileException` for parquet blobs because the directory only looks in `segments/data/`.

### Gap (d): recoverShallowSnapshotV2 handling sibling format containers — GAP

**Evidence**: `syncSegmentsFromGivenRemoteSegmentStore` (IndexShard.java:5871) downloads files from `sourceRemoteDirectory`. The `copySegmentFiles` method (line 5953) iterates `uploadedSegments.keySet()` which includes format-prefixed names like `"parquet/_0.pqt"`. It then calls `fileDownloader.download(sourceRemoteDirectory, storeDirectory, ...)`.

The download path needs:
1. `sourceRemoteDirectory.remoteDataDirectory` to be format-aware (to read from `segments/parquet/`)
2. `storeDirectory` to be a `DataFormatAwareStoreDirectory` (to write to local `parquet/` subdirectory)

**Gap**: During V2 recovery, the `storeDirectory` is obtained from `store.directory()`. For a NEW shard being restored, the store may not yet be initialized with `DataFormatAwareStoreDirectory`. The `openEngineAndRecoverFromTranslog` call happens AFTER segment download.

**File**: `server/src/main/java/org/opensearch/index/shard/StoreRecovery.java:524-536`
**Symptom**: Downloaded parquet files written to wrong local directory (flat in `index/` instead of `parquet/` subdirectory), causing engine open failure.

### Gap (e): DataformatAwareCatalogSnapshot reconstruction at pinned timestamp — PARTIAL GAP

**Evidence**: `RemoteSegmentMetadata.getSegmentInfosBytes()` stores the serialized commit bytes. For DFA, the `catalogSnapshotToRemoteMetadataSerializer` serializes the `CatalogSnapshot` into these bytes (via `LuceneCommitter.serializeToCommitFormat`). The catalog snapshot is embedded in `SegmentInfos.userData` under key `CatalogSnapshot.CATALOG_SNAPSHOT_KEY`.

During restore, `Store.buildSegmentInfos()` (line 5913 of IndexShard.java) reconstructs `SegmentInfos` from these bytes. The `DataformatAwareCatalogSnapshot` can be deserialized from `userData` via `DataformatAwareCatalogSnapshot.deserializeFromString()`.

**Gap**: The deserialization requires a `directoryResolver` function (`Function<String, String>`) that maps format names to directory paths. During V2 restore, this resolver may not be available because the shard's directory structure hasn't been set up yet.

**File**: `server/src/main/java/org/opensearch/index/store/Store.java:985-987`
**Symptom**: `DataformatAwareCatalogSnapshot` deserialization fails or produces incorrect directory paths during V2 restore.

### Gap (f): Remote-store deletion paths and format containers — GAP

**Evidence**: `RemoteSegmentStoreDirectory.delete()` (line 1367) calls `remoteDataDirectory.delete()`. In `DataFormatAwareRemoteDirectory.delete()`, it iterates `formatBlobRouter.registeredFormats()` and deletes each container.

**Gap**: When `remoteDirectoryCleanup` is called from `BlobStoreRepository` (line 1323), it creates a new `RemoteSegmentStoreDirectory` via `remoteDirectoryFactory.newDirectory()`. If this factory doesn't know the index uses DFA (no `DataFormatRegistry` context), it creates a plain `RemoteDirectory` that only knows about `segments/data/`. The `segments/parquet/` container is never deleted.

**File**: `server/src/main/java/org/opensearch/index/store/RemoteSegmentStoreDirectory.java:1323-1345`
**Symptom**: After deleting a DFA index, `segments/parquet/` (and other format containers) remain as orphans in the remote store, leaking storage indefinitely.

---

## Part 4: Required Changes

### Storage Layer

1. **RemoteSegmentStoreDirectoryFactory** — Ensure `newDirectory()` creates `DataFormatAwareRemoteDirectory` when the source index has DFA enabled. Pass `DataFormatRegistry` and `IndexSettings` (or at minimum the set of format names from `IndexMetadata`) so format containers are pre-registered.
   - **File**: Factory class (find via `RemoteSegmentStoreDirectoryFactory`)
   - **Why**: Without this, V2 restore and GC cleanup paths cannot route to sibling format containers.

2. **FormatBlobRouter** — Add a constructor/method that accepts a `Set<String>` of format names from index metadata (without requiring `DataFormatRegistry`). This enables format registration during cleanup/restore when the full registry isn't available.
   - **File**: `server/src/main/java/org/opensearch/index/store/remote/FormatBlobRouter.java`
   - **Why**: Cleanup and restore paths don't have access to `DataFormatRegistry` but can read format names from `IndexMetadata.getSettings()`.

3. **DataFormatAwareRemoteDirectory.deleteFiles()** — The broadcast-delete approach works but is inefficient. Consider adding format info to `UploadedSegmentMetadata` so targeted deletes are possible.
   - **File**: `server/src/main/java/org/opensearch/index/store/remote/DataFormatAwareRemoteDirectory.java:195`
   - **Why**: Performance optimization; not a correctness gap.

### Snapshot Layer

4. **StoreRecovery.recoverShallowSnapshotV2** — Before downloading segments, ensure the local store directory is a `DataFormatAwareStoreDirectory` with proper subdirectory routing. This may require initializing the shard's directory structure from `IndexMetadata` settings before segment download.
   - **File**: `server/src/main/java/org/opensearch/index/shard/StoreRecovery.java:465-560`
   - **Why**: Without format-aware local directory, downloaded parquet files go to wrong location.

5. **StoreRecovery.recoverShallowSnapshotV2** — Ensure `sourceRemoteDirectory` is created with format awareness. The `directoryFactory.newDirectory()` call at line 498 must produce a `DataFormatAwareRemoteDirectory` for DFA indices.
   - **File**: `server/src/main/java/org/opensearch/index/shard/StoreRecovery.java:498`
   - **Why**: Without this, `openInput` for parquet blob keys routes to `segments/data/` → `NoSuchFileException`.

6. **BlobStoreRepository.cleanRemoteStoreDirectoryIfNeeded** — Pass DFA format information when creating the `RemoteSegmentStoreDirectory` for cleanup. Read format names from `prevIndexMetadata.getSettings()`.
   - **File**: `server/src/main/java/org/opensearch/repositories/blobstore/BlobStoreRepository.java:2334`
   - **Why**: Without format awareness, `delete()` only removes `segments/data/`, leaving format containers orphaned.

### Metadata Layer

7. **No gap in RemoteSegmentMetadata itself** — DFA files are already tracked via format-prefixed `originalFilename` in `UploadedSegmentMetadata`. The metadata file correctly references all files.

8. **Store.buildSegmentInfos / CatalogSnapshot deserialization** — Ensure the `directoryResolver` function is available during V2 restore. It should map format names to the shard's local subdirectory paths.
   - **File**: `server/src/main/java/org/opensearch/index/store/Store.java:985`
   - **Why**: Without proper directory resolution, the reconstructed `DataformatAwareCatalogSnapshot` has incorrect paths, breaking engine initialization.

### Configuration/Gating

9. **Index metadata persistence of format names** — Ensure that when a DFA index is snapshotted, the `IndexMetadata` stored in the snapshot includes the data format configuration (format names). This is needed so restore/cleanup paths can determine which format containers exist.
   - **File**: Index settings registration for DFA formats
   - **Why**: Without persisted format info, restore/cleanup cannot know which sibling containers to expect.

10. **Feature flag gating** — All V2 changes should be gated behind `FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG` to avoid affecting non-DFA indices.
    - **Why**: Safety; non-DFA indices should continue using the existing V2 path unchanged.
