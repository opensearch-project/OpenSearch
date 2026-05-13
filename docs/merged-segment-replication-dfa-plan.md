# Merged Segment Replication for DataFormatAware Path — Implementation Plan

## Design Principles

1. **Follow the normal DFA replication pattern** — DFA is handled through polymorphism (`CatalogSnapshot` interface + `DataFormatAwareStoreDirectory`). No explicit `instanceof DataformatAwareCatalogSnapshot` checks in the replication path.
2. **Remote upload first, then replication** — Same as Lucene path: primary uploads merged segment to remote store, then sends `RemoteStoreMergedSegmentCheckpoint` to replicas who download from remote.
3. **DFAEngine integration comes later** — We build the infrastructure so that when `DataFormatAwareEngine.applyMergeChanges()` calls `publishMergedSegment()`, everything works end-to-end.
4. **Store is automatically DFA-wrapped** — When the dataformat plugin is enabled, the store directory is already `DataFormatAwareStoreDirectory`. File resolution, checksums, and subdirectory routing are handled transparently.

---

## Current Merged Segment Replication Flow (Lucene Path)

```
PRIMARY:
  IndexWriter merge completes
    → MergedSegmentWarmer.warm(LeafReader)
      → IndexShard.publishMergedSegment(SegmentCommitInfo)
        → IndexShard.computeMergeSegmentCheckpoint(SegmentCommitInfo)
          → Builds MergedSegmentCheckpoint(segmentName, metadataMap, ...)
        → mergedSegmentPublisher.publish(shard, checkpoint)
          → RemoteStorePublishMergedSegmentAction.publish()
            → Upload merged segment files to RemoteSegmentStoreDirectory
            → Build localToRemoteSegmentFilenameMap
            → Wrap in RemoteStoreMergedSegmentCheckpoint
            → Broadcast to replicas via transport action

REPLICA:
  SegmentReplicationTargetService.onNewMergedSegmentCheckpoint()
    → shouldProcessMergedSegmentCheckpoint()
    → isRemoteStoreMergedSegmentCheckpoint() → true
    → markMergedSegmentsPendingDownload(localToRemoteMap)
    → startMergedSegmentReplication()
      → MergedSegmentReplicationTarget.startReplication()
        → getCheckpointMetadata() [no-op: uses checkpoint's own metadata]
        → getFiles() [FILE_DIFF: compute missing files]
        → getFilesFromSource() → RemoteStoreReplicationSource.getMergedSegmentFiles()
          → Downloads from RemoteSegmentStoreDirectory using filename map
        → finalizeReplication()
          → multiFileWriter.renameAllTempFiles()
          → indexShard.addPendingMergeSegmentCheckpoint(checkpoint)
          → Segment is parked on disk, NOT yet searchable

  Later: Normal replication arrives
    → SegmentReplicationTarget.finalizeReplication()
      → indexShard.finalizeReplication(catalogSnapshot)
        → cleanupPendingMergedSegments() removes applied pre-copies
```

---

## What Needs to Change for DFA

### Key Differences Between Lucene and DFA Merged Segments

| Aspect | Lucene | DFA |
|--------|--------|-----|
| Segment identity | `SegmentCommitInfo.info.name` (e.g. `_3`) | `Segment.generation` (long) |
| Files | Flat in `<shard>/index/` | May span subdirectories: `<shard>/index/` + `<shard>/<format>/` |
| File naming | `_3.cfs`, `_3.si` | Format-aware: `parquet::segment_0.parquet` (via `FileMetadata.DELIMITER`) |
| Checksum | Lucene footer CRC32 | `DataFormatAwareStoreDirectory.calculateUploadChecksum()` (full-file CRC32 for non-Lucene formats) |
| Merge trigger | `IndexWriter` → `MergedSegmentWarmer` | `DataFormatAwareEngine.applyMergeChanges()` (no IndexWriter) |
| Metadata | `StoreFileMetadata` from `SegmentCommitInfo.files()` | `StoreFileMetadata` from `CatalogSnapshot.getFiles()` |

### Key Insight: The Replication Infrastructure Already Handles DFA

The normal replication path works for DFA because:
- `DataFormatAwareStoreDirectory.listAll()` returns format-aware filenames
- `Store.getSegmentMetadataMap(CatalogSnapshot)` iterates `catalogSnapshot.getFiles(false)` — polymorphic
- `RemoteStoreRefreshListener.getChecksumOfLocalFile()` already uses `dfasd.calculateUploadChecksum()` for DFA
- `RemoteSegmentStoreDirectory.copyFrom()` uploads by filename — format-aware names work transparently
- `SegmentFileTransferHandler` reads from `shard.store()` which is DFA-wrapped — file resolution is automatic

**The merged segment path should work the same way** — the only gap is the trigger and the checkpoint construction.

---

## Implementation Plan

### Phase 1: Primary-Side — Publish Hook (IndexShard)

#### 1.1 Add `publishMergedSegment(CatalogSnapshot, Segment)` overload to IndexShard

**File:** `server/src/main/java/org/opensearch/index/shard/IndexShard.java`

**Current:**
```java
public void publishMergedSegment(SegmentCommitInfo segmentCommitInfo) {
    mergedSegmentPublisher.publish(this, computeMergeSegmentCheckpoint(segmentCommitInfo));
}

private MergedSegmentCheckpoint computeMergeSegmentCheckpoint(SegmentCommitInfo segmentCommitInfo) {
    // Builds checkpoint from SegmentCommitInfo: segment name, file metadata, codec, etc.
}
```

**Add:**
```java
/**
 * Publishes a merged segment for DFA engines. Builds the checkpoint from the
 * CatalogSnapshot's file metadata for the given segment generation.
 */
public void publishMergedSegment(CatalogSnapshot catalogSnapshot, Segment mergedSegment) {
    mergedSegmentPublisher.publish(this, computeMergeSegmentCheckpoint(catalogSnapshot, mergedSegment));
}

private MergedSegmentCheckpoint computeMergeSegmentCheckpoint(CatalogSnapshot catalogSnapshot, Segment mergedSegment) {
    // 1. Get files belonging to this segment from the catalog snapshot
    //    (filter catalogSnapshot.getFiles(false) by segment generation)
    // 2. Build StoreFileMetadata map using store.getSegmentMetadataMap() or
    //    iterate files and compute checksums via DataFormatAwareStoreDirectory
    // 3. Construct MergedSegmentCheckpoint with:
    //    - segmentName = String.valueOf(mergedSegment.generation()) [or a format-aware identifier]
    //    - metadataMap = computed file metadata
    //    - codec = "dataformat" (or the primary format name)
    //    - length = sum of file sizes
}
```

**Design decision:** Use `String.valueOf(segment.generation())` as the segment name in `MergedSegmentCheckpoint`. This is the DFA equivalent of Lucene's `_3` segment name. The `pendingMergedSegmentCheckpoints` set matches by this name.

#### 1.2 Wire the publish call in DataFormatAwareEngine (future — NOT in this PR)

**File:** `server/src/main/java/org/opensearch/index/engine/DataFormatAwareEngine.java`

When DFA merges are production-ready, add to `applyMergeChanges()`:
```java
void applyMergeChanges(MergeResult result, OneMerge merge) {
    catalogSnapshotManager.applyMergeResults(result);
    refreshListeners();
    // Future: publish merged segment for replication
    // if (shouldPublishMergedSegment(result)) {
    //     shard.publishMergedSegment(getCurrentCatalogSnapshot(), result.getMergedSegment());
    // }
}
```

---

### Phase 2: Remote Upload — Upload Merged DFA Segments

#### 2.1 Update `RemoteStorePublishMergedSegmentAction.publish()`

**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/RemoteStorePublishMergedSegmentAction.java`

**Current flow:**
```java
publish(IndexShard shard, MergedSegmentCheckpoint checkpoint) {
    Map<String, String> localToRemote = uploadMergedSegmentsToRemoteStore(shard, checkpoint);
    RemoteStoreMergedSegmentCheckpoint rsmc = new RemoteStoreMergedSegmentCheckpoint(checkpoint, localToRemote);
    doPublish(shard, rsmc, ...);
}

private Map<String, String> uploadMergedSegmentsToRemoteStore(IndexShard shard, MergedSegmentCheckpoint checkpoint) {
    RemoteSegmentStoreDirectory remoteDir = shard.getRemoteDirectory();
    for (String file : checkpoint.getMetadataMap().keySet()) {
        remoteDir.copyFrom(shard.store().directory(), file, IOContext.DEFAULT, listener, ...);
        localToRemote.put(file, remoteDir.getExistingRemoteFilename(file));
    }
    return localToRemote;
}
```

**What to verify/change:**
- `remoteDir.copyFrom(shard.store().directory(), file, ...)` — the source directory is `shard.store().directory()` which is already `DataFormatAwareStoreDirectory` when DFA is enabled
- `DataFormatAwareStoreDirectory.openInput(file, ctx)` resolves format-aware filenames to the correct subdirectory
- **This should already work for DFA files** — the `copyFrom` reads from the DFA-wrapped directory which handles path resolution

**Verify:** `remoteDir.getExistingRemoteFilename(file)` works with format-aware filenames (e.g. `parquet::segment_0.parquet`). If `RemoteSegmentStoreDirectory` stores filenames as-is, this should work. If it strips format prefixes, we need to handle that.

**Action:** Write a test that uploads a DFA-format file through this path and verifies the `localToRemoteSegmentFilenameMap` is correct.

#### 2.2 Verify `RemoteSegmentStoreDirectory.copyFrom()` handles DFA filenames

**File:** `server/src/main/java/org/opensearch/index/store/RemoteSegmentStoreDirectory.java`

**Check:**
- `getNewRemoteSegmentFilename(src)` — generates UUID-suffixed remote name from local name. Must handle format-aware names (with `::` delimiter).
- `postUpload(from, src, remoteFileName, checksum)` — stores `UploadedSegmentMetadata`. The `src` is the local filename (format-aware).
- `containsFile(file, checksum)` — used by `skipUpload()`. Must match format-aware filenames.
- `getChecksumOfLocalFile(from, src)` — already uses `DataFormatAwareStoreDirectory.calculateUploadChecksum()` when DFA is enabled.

**Likely no changes needed** — but must verify with integration test.

---

### Phase 3: Replica Side — Receiving Merged DFA Segments

#### 3.1 `MergedSegmentReplicationTarget.finalizeReplication()` — Already Works

**File:** `server/src/main/java/org/opensearch/indices/replication/MergedSegmentReplicationTarget.java`

```java
protected void finalizeReplication(CheckpointInfoResponse checkpointInfoResponse) throws Exception {
    multiFileWriter.renameAllTempFiles();
    indexShard.addPendingMergeSegmentCheckpoint((MergedSegmentCheckpoint) checkpoint);
}
```

This is format-agnostic — it just renames temp files and adds the checkpoint to the pending set. **No changes needed.**

#### 3.2 `RemoteStoreReplicationSource.getMergedSegmentFiles()` — Verify DFA Compatibility

**File:** `server/src/main/java/org/opensearch/indices/replication/RemoteStoreReplicationSource.java`

```java
public void getMergedSegmentFiles(...) {
    assert checkpoint instanceof RemoteStoreMergedSegmentCheckpoint;
    indexShard.getFileDownloader().downloadAsync(cancellableThreads, remoteDirectory,
        new ReplicationStatsDirectoryWrapper(storeDirectory, fileProgressTracker),
        toDownloadSegmentNames, ...);
}
```

**Check:** `toDownloadSegmentNames` comes from the checkpoint's metadata map keys (format-aware filenames). The download writes to `storeDirectory` which is DFA-wrapped — `createOutput(file, ctx)` resolves to the correct subdirectory.

**Verify:** `remoteDirectory.openInput(remoteFilename, ctx)` can read the file uploaded with a format-aware name. The `localToRemoteSegmentFilenameMap` maps local format-aware names to remote UUID-suffixed names.

**Likely no changes needed** — but must verify the filename mapping round-trips correctly.

#### 3.3 `cleanupPendingMergedSegments()` — Add DFA Support

**File:** `server/src/main/java/org/opensearch/index/shard/IndexShard.java`

**Current:**
```java
private void cleanupPendingMergedSegments(CatalogSnapshot catalogSnapshot) {
    if (catalogSnapshot instanceof SegmentInfosCatalogSnapshot siSnapshot) {
        for (SegmentCommitInfo segmentCommitInfo : siSnapshot.getSegmentInfos()) {
            pendingMergedSegmentCheckpoints.removeIf(
                s -> s.getSegmentName().equals(segmentCommitInfo.info.name));
        }
    }
    // DFA snapshots: no cleanup (DFA engines don't use merged-segment pre-copy yet)
}
```

**Change (when DFA merges are enabled):**
```java
private void cleanupPendingMergedSegments(CatalogSnapshot catalogSnapshot) {
    if (catalogSnapshot instanceof SegmentInfosCatalogSnapshot siSnapshot) {
        for (SegmentCommitInfo segmentCommitInfo : siSnapshot.getSegmentInfos()) {
            pendingMergedSegmentCheckpoints.removeIf(
                s -> s.getSegmentName().equals(segmentCommitInfo.info.name));
        }
    } else if (catalogSnapshot instanceof DataformatAwareCatalogSnapshot dfaSnapshot) {
        // DFA segments are identified by generation
        Set<String> activeGenerations = dfaSnapshot.getSegments().stream()
            .map(seg -> String.valueOf(seg.generation()))
            .collect(Collectors.toSet());
        pendingMergedSegmentCheckpoints.removeIf(
            s -> activeGenerations.contains(s.getSegmentName()));
    }
}
```

**Note:** This can be added now (no-op until DFA merges produce pending checkpoints) or deferred to when DFA merges are enabled.

---

### Phase 4: Peer-to-Peer Path (Non-Remote-Store)

#### 4.1 `SegmentReplicationSourceService.GetMergedSegmentFilesRequestHandler`

**File:** `server/src/main/java/org/opensearch/indices/replication/SegmentReplicationSourceService.java`

**Current:**
```java
void messageReceived(GetSegmentFilesRequest request, ...) {
    // Creates SegmentFileTransferHandler from indexShard.store()
    // Reads files from store and sends as chunks
    createTransfer(indexShard.store(), checkpoint.getMetadataMap().values(), ...)
}
```

**Check:** `indexShard.store()` is DFA-wrapped. `SegmentFileTransferHandler` reads files via `store.directory().openInput(filename, ctx)`. For DFA files, `DataFormatAwareStoreDirectory.openInput()` resolves format-aware filenames to the correct subdirectory.

**Likely no changes needed** — the store abstraction handles it.

#### 4.2 `PrimaryShardReplicationSource.getMergedSegmentFiles()`

**File:** `server/src/main/java/org/opensearch/indices/replication/PrimaryShardReplicationSource.java`

Sends transport request with `GetSegmentFilesRequest` containing the file list from the checkpoint's metadata map. **No changes needed** — format-aware filenames are just strings in the request.

---

### Phase 5: Integration Points to Verify

| Component | File | What to Verify |
|-----------|------|----------------|
| `RemoteSegmentStoreDirectory.getNewRemoteSegmentFilename()` | `RemoteSegmentStoreDirectory.java` | Handles `::` delimiter in format-aware filenames without corruption |
| `RemoteSegmentStoreDirectory.getExistingRemoteFilename()` | `RemoteSegmentStoreDirectory.java` | Looks up by format-aware local filename |
| `MultiFileWriter` | `indices/recovery/MultiFileWriter.java` | Writes to correct subdirectory when target store is DFA-wrapped |
| `SegmentFileTransferHandler.createTransfer()` | `SegmentFileTransferHandler.java` | Opens files from DFA-wrapped store correctly |
| `Store.segmentReplicationDiff()` | `Store.java` | Diff works with format-aware filenames in metadata maps |
| `MergedSegmentCheckpoint` serialization | `MergedSegmentCheckpoint.java` | `segmentName` field handles generation-based names |
| `shouldProcessMergedSegmentCheckpoint()` | `IndexShard.java` | No Lucene-specific assumptions |

---

### Phase 6: Testing Plan

#### Unit Tests
1. **`computeMergeSegmentCheckpoint(CatalogSnapshot, Segment)`** — verify correct metadata map construction for DFA segments
2. **`cleanupPendingMergedSegments` with DFA snapshot** — verify generation-based cleanup
3. **`RemoteSegmentStoreDirectory.copyFrom` with format-aware filenames** — verify upload/download round-trip

#### Integration Tests
4. **End-to-end merged segment pre-copy with DFA store** — primary uploads merged DFA segment to remote, replica downloads and parks it, next regular replication applies it
5. **Peer-to-peer merged segment transfer with DFA store** — primary serves DFA files directly to replica via transport

---

## Summary: What to Build NOW vs LATER

### NOW (this PR / next PR):
| # | Task | Files |
|---|------|-------|
| 1 | Add `publishMergedSegment(CatalogSnapshot, Segment)` overload | `IndexShard.java` |
| 2 | Add `computeMergeSegmentCheckpoint(CatalogSnapshot, Segment)` | `IndexShard.java` |
| 3 | Verify `RemoteStorePublishMergedSegmentAction` works with DFA filenames | `RemoteStorePublishMergedSegmentAction.java` |
| 4 | Verify `RemoteSegmentStoreDirectory.copyFrom/getNewRemoteSegmentFilename` handles `::` delimiter | `RemoteSegmentStoreDirectory.java` |
| 5 | Verify `RemoteStoreReplicationSource.getMergedSegmentFiles` downloads to correct subdirectory | `RemoteStoreReplicationSource.java` |
| 6 | Add DFA branch to `cleanupPendingMergedSegments` | `IndexShard.java` |
| 7 | Write unit + integration tests | New test files |

### LATER (when DFA merges are production-ready):
| # | Task | Files |
|---|------|-------|
| 8 | Wire `publishMergedSegment()` call in `DataFormatAwareEngine.applyMergeChanges()` | `DataFormatAwareEngine.java` |
| 9 | Add size-threshold gating (equivalent to `MergedSegmentWarmer.shouldWarm()`) | `DataFormatAwareEngine.java` |
| 10 | Feature flag for DFA merged-segment-replication | Settings |

---

## API Touch Points (Complete List)

| API / Method | Role | DFA Impact |
|---|---|---|
| `IndexShard.publishMergedSegment()` | Trigger | New overload for DFA |
| `IndexShard.computeMergeSegmentCheckpoint()` | Build checkpoint | New overload for DFA |
| `MergedSegmentCheckpoint(segmentName, metadataMap, ...)` | Data model | `segmentName` = generation string for DFA |
| `RemoteStorePublishMergedSegmentAction.publish()` | Upload + broadcast | Verify DFA filename handling |
| `RemoteStorePublishMergedSegmentAction.uploadMergedSegmentsToRemoteStore()` | Upload | Verify `copyFrom` with DFA directory |
| `RemoteSegmentStoreDirectory.copyFrom()` | Upload file | Verify format-aware filename handling |
| `RemoteSegmentStoreDirectory.getNewRemoteSegmentFilename()` | Generate remote name | Verify `::` delimiter handling |
| `RemoteSegmentStoreDirectory.getExistingRemoteFilename()` | Lookup remote name | Verify format-aware lookup |
| `RemoteStoreMergedSegmentCheckpoint` | Checkpoint + filename map | No changes (carries strings) |
| `SegmentReplicationTargetService.onNewMergedSegmentCheckpoint()` | Replica entry point | No changes |
| `MergedSegmentReplicationTarget.getFilesFromSource()` | Fetch files | No changes (delegates to source) |
| `MergedSegmentReplicationTarget.finalizeReplication()` | Park files | No changes (renames + adds to pending) |
| `RemoteStoreReplicationSource.getMergedSegmentFiles()` | Download from remote | Verify DFA directory write |
| `PrimaryShardReplicationSource.getMergedSegmentFiles()` | Peer transport | No changes (sends file list) |
| `GetMergedSegmentFilesRequestHandler` | Primary serves files | Verify DFA store read |
| `SegmentFileTransferHandler.createTransfer()` | Read files from store | Verify DFA openInput |
| `MultiFileWriter` | Write files on replica | Verify DFA createOutput |
| `Store.segmentReplicationDiff()` | Compute missing files | No changes (operates on metadata maps) |
| `cleanupPendingMergedSegments()` | Cleanup after apply | Add DFA generation-based branch |
| `IndexShard.shouldProcessMergedSegmentCheckpoint()` | Gating | No changes (checks primary term only) |
