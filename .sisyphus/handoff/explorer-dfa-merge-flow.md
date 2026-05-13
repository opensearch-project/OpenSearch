# DFA Merge Completion Flow — Exhaustive Map

## A. DataFormatAwareEngine.applyMergeChanges(MergeResult, OneMerge)

**File:** `server/src/main/java/org/opensearch/index/engine/DataFormatAwareEngine.java:1367`

```java
private void applyMergeChanges(MergeResult mergeResult, OneMerge oneMerge) {
    refreshLock.lock();
    try {
        catalogSnapshotManager.applyMergeResults(mergeResult, oneMerge);
        try (GatedCloseable<CatalogSnapshot> newSnapshotRef = catalogSnapshotManager.acquireSnapshot()) {
            refreshListeners(true, newSnapshotRef.get());
        }
    } catch (Exception ex) {
        try {
            logger.error(() -> new ParameterizedMessage("Merge failed while registering merged files in Snapshot"), ex);
            failEngine("Merge failed while registering merged files in Snapshot", ex);
        } catch (Exception inner) {
            ex.addSuppressed(inner);
        }
        throw new MergeFailedEngineException(shardId, ex);
    } finally {
        refreshLock.unlock();
    }
}
```

**Caller:** `MergeScheduler` (line 146 for background merges, line 233 for force merges) invokes `applyMergeChanges.accept(mergeResult, oneMerge)` via a `BiConsumer<MergeResult, OneMerge>` callback passed at construction (line 334: `this::applyMergeChanges`).

**Lock semantics:** `refreshLock` (ReentrantLock, line 171) is held for the entire body. `refreshListeners` notifies `EngineReaderManager` instances so they can create readers for the new snapshot.

**Error handling:** If `applyMergeResults` or `refreshListeners` throws, the engine is failed via `failEngine(...)` and a `MergeFailedEngineException` is thrown. The lock is always released in `finally`.

## B. CatalogSnapshotManager.applyMergeResults(MergeResult, OneMerge)

**File:** `server/src/main/java/org/opensearch/index/engine/exec/coord/CatalogSnapshotManager.java:131`

```java
public synchronized void applyMergeResults(MergeResult mergeResult, OneMerge oneMerge) throws IOException {
    List<Segment> segmentList = new ArrayList<>(latestCatalogSnapshot.getSegments());
    Segment segmentToAdd = getSegment(mergeResult.getMergedWriterFileSet());
    Set<Segment> segmentsToRemove = new HashSet<>(oneMerge.getSegmentsToMerge());
    boolean inserted = false;
    int newSegIdx = 0;
    for (int segIdx = 0, cnt = segmentList.size(); segIdx < cnt; segIdx++) {
        Segment currSegment = segmentList.get(segIdx);
        if (segmentsToRemove.contains(currSegment)) {
            if (!inserted) {
                segmentList.set(segIdx, segmentToAdd);
                inserted = true;
                newSegIdx++;
            }
        } else {
            segmentList.set(newSegIdx, currSegment);
            newSegIdx++;
        }
    }
    segmentList.subList(newSegIdx, segmentList.size()).clear();
    if (!inserted) {
        segmentList.add(0, segmentToAdd);
    }
    commitNewSnapshot(segmentList);
}
```

**Return type:** `void`

**Side effects:** Calls `commitNewSnapshot(segmentList)` which:
1. Creates a new `DataformatAwareCatalogSnapshot` with incremented generation/version/id.
2. Registers new files with `indexFileDeleter.addFileReferences(newSnapshot)`.
3. Replaces `latestCatalogSnapshot` and decrefs the old one.

**Locks:** Method is `synchronized` on the CatalogSnapshotManager instance. Additionally, the caller holds `refreshLock`.

**Key insight for insertion point:** `segmentToAdd` is the newly merged `exec.Segment` — constructed via `getSegment(mergeResult.getMergedWriterFileSet())` at line 135. This is the segment identity needed for `publishMergedSegment`.

## C. CatalogSnapshotManager.acquireSnapshot()

**File:** `server/src/main/java/org/opensearch/index/engine/exec/coord/CatalogSnapshotManager.java:260`

```java
public GatedCloseable<CatalogSnapshot> acquireSnapshot() {
    if (closed.get()) { throw new IllegalStateException("CatalogSnapshotManager is closed"); }
    final CatalogSnapshot snapshot = latestCatalogSnapshot;
    if (snapshot.tryIncRef() == false) {
        throw new IllegalStateException("CatalogSnapshot [gen=" + snapshot.getGeneration() + "] is already closed");
    }
    return new GatedCloseable<>(snapshot, () -> decRefAndMaybeDelete(snapshot));
}
```

**Returns:** `GatedCloseable<CatalogSnapshot>` — reference-counted. Caller must close to decrement ref.

**Safe to call from within refreshLock?** Yes — it's a non-synchronized method using volatile read + tryIncRef. Already called inside `applyMergeChanges` at line 1371.

## D. Current Publish Hookup State

**`publishMergedSegment` usages (all Java files):**
1. `MergedSegmentWarmer.java:72` — `indexShard.publishMergedSegment(segmentCommitInfo)` (Lucene path)
2. `IndexShard.java:2184` — `publishMergedSegment(SegmentCommitInfo)` definition
3. `IndexShard.java:2225` — `publishMergedSegment(String segmentName, Collection<String> segmentFiles)` definition (DFA-ready overload)

**Is `publishMergedSegment(String, Collection<String>)` called anywhere in DFA code today?** **NO.** Zero callers found. The method exists but is never invoked.

**Is MergedSegmentWarmer wired for DFA?** **NO.** `MergedSegmentWarmer` is a Lucene `IndexWriter.IndexReaderWarmer` — DFA does not use IndexWriter, so this warmer is never triggered for DFA merges. The `DataFormatAwareEngine` has no reference to `MergedSegmentWarmer` or `MergedSegmentWarmerFactory`.

**Natural insertion point:** Inside `applyMergeChanges`, after `catalogSnapshotManager.applyMergeResults(...)` succeeds and before/after `refreshListeners`. The merged `exec.Segment` is available from `getSegment(mergeResult.getMergedWriterFileSet())` — but that's private to CatalogSnapshotManager. Alternative: identify the new segment from the acquired snapshot by generation.

## E. exec.Segment Available at the Insertion Point

**What `applyMergeResults` exposes:** It returns `void`. However:
- The `segmentToAdd` is constructed inside `applyMergeResults` via `getSegment(mergeResult.getMergedWriterFileSet())` (line 135).
- After `applyMergeResults` returns, the caller acquires a new snapshot (line 1371). The newly merged segment is in that snapshot's `getSegments()` list.

**Identifying the merged segment:** The merged segment's generation = `mergeResult.getMergedWriterFileSet().values().iterator().next().writerGeneration()` (from `getSegment` helper at line 351-362). The caller can reconstruct the same `Segment` object or find it by generation in the new snapshot.

**Recommended approach:** Either:
1. Change `applyMergeResults` to return the `Segment` it creates, OR
2. Reconstruct the `Segment` in `applyMergeChanges` using the same logic as `getSegment` (it's trivial — 5 lines).

## F. Feature-Flag / Setting Gating for DFA Replication

| Gate | Details |
|------|---------|
| `opensearch.pluggable.dataformat.merge.enabled` | System property, default `false`. Gates whether DFA merges run at all (line 1396). |
| `indices.replication.merges.warmer.enabled` | Node-scope dynamic setting, default `false`. Gates Lucene warmer. DFA should check this too. |
| `isSegRepEnabledOrRemoteNode()` | Condition in `IndexService:845` that determines if `mergedSegmentPublisher` is non-null for the shard. **Same condition applies to DFA shards** — if the index uses segment replication or remote store, the publisher is set. |
| `Version.V_3_4_0` min node version | Lucene warmer checks `V_3_4_0.compareTo(minNodeVersion) > 0`. DFA should port this guard. |
| `opensearch.experimental.feature.pluggable.dataformat.enabled` | Feature flag for the entire DFA feature. |

**Is `mergedSegmentPublisher` set for DFA IndexShards?** YES — the condition is `isSegRepEnabledOrRemoteNode()`, which is format-agnostic. If the index uses segment replication, the publisher is wired regardless of engine type.

## G. DFA-Specific Guards (Mirror of Lucene's shouldWarm)

**No equivalent exists today.** The Lucene path checks:
1. Min node version >= 3.4.0
2. `isMergedSegmentReplicationWarmerEnabled()` setting
3. Segment size >= threshold (`getMergedSegmentWarmerMinSegmentSizeThreshold()`)

**For DFA:** The implementer must add equivalent guards. Segment size can be computed from `WriterFileSet.getTotalSize()` summed across all formats in the merged segment.

## H. Checksum Computation for DFA Segments

**File:** `server/src/main/java/org/opensearch/index/store/Store.java:469`

```java
public Map<String, StoreFileMetadata> getFileMetadata(Collection<String> files) throws IOException {
    failIfCorrupted();
    Map<String, StoreFileMetadata> result = new HashMap<>();
    for (String file : files) {
        final long length = directory.fileLength(file);
        final String checksum;
        final DataFormatAwareStoreDirectory dfasd = DataFormatAwareStoreDirectory.unwrap(directory);
        if (dfasd != null && DataFormatAwareStoreDirectory.isDefaultFormat(FileMetadata.parseDataFormat(file)) == false) {
            checksum = dfasd.calculateUploadChecksum(file);
        } else {
            try (IndexInput in = directory.openInput(file, IOContext.READONCE)) {
                checksum = Store.digestToString(CodecUtil.retrieveChecksum(in));
            }
        }
        result.put(file, new StoreFileMetadata(file, length, checksum, org.opensearch.Version.CURRENT.luceneVersion));
    }
    return result;
}
```

**DFA branching:** YES — if `DataFormatAwareStoreDirectory` is present and the file is NOT default (Lucene) format, it uses `dfasd.calculateUploadChecksum(file)` which delegates to format-specific `FormatChecksumStrategy` (e.g., precomputed CRC32 for Parquet).

**`calculateUploadChecksum`:** `DataFormatAwareStoreDirectory.java:232` — returns `Long.toString(calculateChecksum(name))`.

**`getSegmentMetadataMap(SegmentInfos)`:** Strictly Lucene — uses `loadMetadata(segmentInfos, ...)`.
**`getSegmentMetadataMap(CatalogSnapshot)`:** DFA-aware — dispatches based on snapshot type.

## I. exec.Segment & WriterFileSet Accessors

**`exec.Segment`** (record): `server/src/main/java/org/opensearch/index/engine/exec/Segment.java`
- Fields: `long generation`, `Map<String, WriterFileSet> dfGroupedSearchableFiles`
- Implements `Writeable` for cross-node serialization.

**`exec.WriterFileSet`** (record): `server/src/main/java/org/opensearch/index/engine/exec/WriterFileSet.java`
- Fields: `String directory`, `long writerGeneration`, `Set<String> files`, `long numRows`
- `getTotalSize()` computes size by reading file system.

**Flatten utility:** **NONE EXISTS.** To get a flat `Collection<String>` of all files in a segment:
```java
segment.dfGroupedSearchableFiles().values().stream()
    .flatMap(wfs -> wfs.files().stream())
    .collect(Collectors.toList());
```
This utility should be added (e.g., `Segment.allFiles()`) or inlined at the IndexShard boundary.

## J. Extensibility Hooks

**No SPI/interface exists** for merge-replication hooks in the engine layer. The current design uses:
- Lucene: `IndexWriter.IndexReaderWarmer` (callback from IndexWriter after merge)
- DFA: `BiConsumer<MergeResult, OneMerge>` callback in `MergeScheduler`

The `IndexShard.publishMergedSegment(String, Collection<String>)` overload is format-agnostic and works for any engine that can provide a segment name + file list. The `exec.Segment` record is `Writeable`, supporting future serialization needs.

## K. Existing Tests for DFA Merge / Replication

**Test classes referencing DataFormatAwareEngine:**
1. `server/src/test/java/org/opensearch/index/engine/DataFormatAwareEngineTests.java` — uses `NoMergePolicy`, does NOT test merge flow.
2. `server/src/test/java/org/opensearch/index/engine/exec/coord/CatalogSnapshotManagerTests.java:327,367,399` — tests `applyMergeResults` directly.
3. `test/framework/src/main/java/org/opensearch/index/shard/IndexShardTestCase.java` — references DFA engine.

**Tests for DFA merged segment publish:** **NONE.** No test validates `publishMergedSegment` or `MergedSegmentCheckpoint` for DFA segments.

---

## Summary: Recommended Insertion Point

```
DataFormatAwareEngine.applyMergeChanges(MergeResult, OneMerge)
  ├── refreshLock.lock()
  ├── catalogSnapshotManager.applyMergeResults(mergeResult, oneMerge)  // updates catalog
  ├── *** INSERT publishMergedSegment HERE ***
  │     - Reconstruct exec.Segment from mergeResult.getMergedWriterFileSet()
  │     - Flatten files, call indexShard.publishMergedSegment(segmentName, files)
  │     - Guard with: warmer enabled + min version + size threshold
  ├── acquireSnapshot() → refreshListeners(true, snapshot)
  └── refreshLock.unlock()
```

The call should go AFTER `applyMergeResults` (so the catalog is consistent) and BEFORE or AFTER `refreshListeners` (order doesn't matter for correctness — publish is async). Placing it before `refreshListeners` means the segment is published before readers see it, which matches the Lucene warmer semantics (warm happens before the reader is visible).
