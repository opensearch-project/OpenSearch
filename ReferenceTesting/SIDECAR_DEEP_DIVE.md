# Star Tree Sidecar Upgrade — Deep Dive

## Why Sidecar? The Delete Problem

The codec-switching approach (Spec 3) breaks on segments with soft deletes. Here's why:

When documents are deleted, Lucene creates generation-based update files:
- `_0_1.fnm` — updated field infos (adds `__soft_deletes` field with NEW field numbers)
- `_0_1_Lucene90_0.dvd/dvm` — updated doc values (soft delete markers)

The base doc values inside `.cfs` retain the ORIGINAL field numbers.

When we switch the codec to `Composite912Codec`, Lucene's `SegmentDocValuesProducer` routing breaks:
1. Reads `.si` → codec = `Composite912Codec`
2. Reads updated `FieldInfos` from `_0_1.fnm` (has `__soft_deletes` with new field numbers)
3. Opens base doc values via `Composite912DocValuesFormat.fieldsProducer()` → our reader
4. `PendingSoftDeletes.onNewReader()` asks for `__soft_deletes`
5. Routing is supposed to send `__soft_deletes` to the update-file producer — but it doesn't
6. Our base producer gets asked, returns null → soft delete count = 0 → assertion fails

The sidecar approach eliminates this entirely: **no codec switch, no routing confusion**.

---

## The Core Idea

Instead of switching the codec on existing segments, build star tree data as sidecar files alongside them. The original codec stays. Lucene is happy. Star tree data is discovered at query time through a parallel path.

```
Old approach:  segment codec → Composite912Codec → star tree via codec pipeline
New approach:  segment codec → unchanged → star tree via sidecar reader
```

Over time, background merges produce native `Composite912Codec` segments (because the engine was restarted with the composite codec). The sidecar files are cleaned up as their parent segments are merged away.

---

## The GC Protection Problem

Sidecar files are NOT in any Lucene commit. `IndexWriter.deleteUnusedFiles()` enumerates referenced files via `SegmentInfos.files()` and deletes everything else. Without protection, sidecar files would be silently deleted.

### Why setLiveCommitData() Doesn't Work

`IndexWriter.setLiveCommitData()` stores key-value pairs in the commit. But Lucene treats these as opaque strings — it does NOT parse them to extract file names for GC protection. The commit data is useful for crash recovery cross-checking, but it doesn't prevent file deletion.

### The Real Protection: SidecarProtectedDirectory

A `FilterDirectory` wrapper that intercepts `deleteFile()`:

```java
@Override
public void deleteFile(String name) throws IOException {
    if (protectedFiles.contains(name)) {
        return;  // silently skip — IndexWriter is trying to GC a sidecar file
    }
    super.deleteFile(name);
}
```

This wrapper wraps `Store.directory()` and is returned by `Store.engineDirectory()`. All engine construction paths use `store.engineDirectory()`:
- `InternalEngine.createWriter()` → `store.engineDirectory()`
- `Engine.cleanUpUnreferencedFiles()` → `store.engineDirectory()`
- `resetEngineToGlobalCheckpoint()` → `newEngineConfig()` → `store.engineDirectory()`

`Store.cleanupAndVerify()` uses `store.directory()` (raw) but is patched to check the protected set before deleting.

### The unprotect() Ordering Problem

When a sidecar segment is merged away, we need to delete its files. But if we `unprotect()` before all queries finish reading, IndexWriter could delete the files mid-read.

Solution: `unprotect()` happens INSIDE `StarTreeSidecarReader.deleteFiles()`, which is called ONLY when `refCount` reaches 0. At that point, no queries are reading the files.

```
Merge cleanup:
  1. markPendingDeletion() + decRef() on cached reader
  2. Files stay PROTECTED while queries hold references
  3. Last query's decRef() → refCount=0 → deleteFiles()
  4. deleteFiles(): unprotect() THEN deleteFile() on underlying directory
```

---

## The Reference Counting Pattern

Same pattern as Lucene's `AbstractRefCounted`:

```java
public void incRef() {
    while (true) {
        int count = refCount.get();
        if (count <= 0) {
            throw new AlreadyClosedException("already closed");
        }
        if (refCount.compareAndSet(count, count + 1)) {
            return;
        }
    }
}
```

The CAS loop handles the race between a query retrieving the reader from the cache and calling `incRef()` while the cache is releasing its reference. If the count was already 0, `incRef()` throws `AlreadyClosedException` instead of silently incrementing from 0 to 1.

---

## The LiveDocs Filtering

When a segment has soft deletes, we need to exclude deleted docs from the star tree. The `LiveDocsFilteredDocValuesProducer` wraps the segment's `DocValuesProducer` and:

1. Builds a mapping from contiguous IDs (0, 1, 2, ...) to original doc IDs
2. Wraps each doc values iterator to skip deleted docs and remap IDs

```java
// Build mapping
int[] remappedToOriginal = new int[numLiveDocs];
int remapped = 0;
for (int original = 0; original < maxDoc; original++) {
    if (liveDocs.get(original)) {
        remappedToOriginal[remapped++] = original;
    }
}

// In each wrapper:
@Override
public boolean advanceExact(int target) throws IOException {
    return inner.advanceExact(remappedToOriginal[target]);
}
```

The star tree is built with `numLiveDocs` as `maxDoc` in the `SegmentWriteState`. It's a self-contained aggregation index — no mapping back to original doc IDs.

---

## The TOCTOU Race and Why It's Benign

Between building sidecar files and registering them in metadata, a segment can be merged away. The design makes this race benign:

1. `DirectoryReader` snapshot pins file handles during the build
2. After building, compare generations under `sidecarMetadataLock`
3. Discard sidecar files for merged-away segments
4. If a segment slips through (tiny window between lock release and metadata commit):
   - `StarTreeSidecarReader` constructor throws `FileNotFoundException`
   - `StarTreeQueryHelper.getStarTreeValues()` catches it
   - Removes stale entry from metadata
   - Returns null → query falls back to normal aggregation
5. Periodic cleanup (on merge) catches remaining stale entries

No crash, no wrong results — just a one-time fallback for that query.

---

## The Cleanup Lock Scope

The merge cleanup callback holds `sidecarMetadataLock` for in-memory state updates ONLY. Disk I/O happens outside the lock:

```java
void performSidecarCleanup() {
    boolean changed = false;
    
    // Phase 1: In-memory updates under lock (fast, no I/O)
    synchronized (sidecarMetadataLock) {
        Set<String> orphanedFiles = metadata.removeOrphanedSegments(currentInfos);
        if (orphanedFiles.isEmpty() == false) {
            changed = true;
            // Mark readers for deferred deletion
            for (String segName : orphanedSegments) {
                StarTreeSidecarReader reader = sidecarReaderCache.remove(segName);
                if (reader != null) {
                    reader.markPendingDeletion();
                    reader.decRef();
                }
            }
        }
    }
    
    // Phase 2: Disk I/O outside lock (prevents deadlock with flush)
    if (changed) {
        metadata.commit(directory);
    }
}
```

This prevents the deadlock: upgrade holds lock waiting for flush, flush thread runs merge cleanup which also needs the lock.

---

## The Engine Restart

After sidecar files are built, the engine is restarted so future flushes and merges use `Composite912Codec`. The expensive translog-draining flush happens OUTSIDE `blockOperations()`:

```
1. flush(force=true)           ← OUTSIDE block (expensive, not write-blocked)
2. blockOperations()           ← writes blocked from here
3. updateCommitDataForSidecarFiles()
4. flush(force=true)           ← persist commit data (fast, translog drained)
5. codecServiceOverride = ...  ← inject Composite912Codec
6. resetEngineToGlobalCheckpoint()  ← engine restart (fast, empty translog)
7. unblockOperations()         ← writes unblocked
```

Total write-blocking window: ~200ms (one fast flush + engine restart with empty translog).

---

## The Replica Consistency Solution

During the upgrade window, the primary might have star tree acceleration while replicas don't yet. This causes result divergence.

Solution: `StarTreeQueryHelper.isStarTreeSupported()` checks `indexShard.isStarTreeUpgradeInProgress()`. When true, star tree acceleration is skipped — all shards use normal doc values aggregation. The flag is cleared by `TransportStarTreeUpgradeAction.shardOperation()` in a `finally` block after each shard's upgrade completes.

Since `TransportBroadcastByNodeAction` runs `shardOperation()` on every node for every shard, all flags are cleared when the broadcast response arrives at the coordinator.

---

## The Crash Recovery Strategy

The metadata file is the primary source of truth. Commit data is secondary.

```
On shard start:
1. Load highest valid generational metadata file
2. For each segment in metadata:
   - Files on disk → valid, protect them
   - Files missing → stale, remove entry
3. Initialize SidecarProtectedDirectory BEFORE engine opens
4. Cross-check commit data:
   - Files in commit data but NOT in metadata → incomplete build, clean up
   - Files in metadata but NOT in commit data → metadata committed but
     commit data flush didn't complete → files are valid (metadata is primary)
```

The common crash case (crash between metadata commit and commit data flush) is handled correctly: metadata exists, commit data doesn't → metadata wins, files are protected.

---

## Segment State Machine

```
┌─────────────────┐     upgrade API      ┌──────────────────┐     background merge     ┌─────────────────┐
│  No Star Tree   │ ──────────────────→  │  Sidecar Star    │ ──────────────────────→  │  Native Star    │
│  (Lucene912)    │                      │  Tree (sidecar)  │                          │  Tree (native)  │
│                 │                      │                  │                          │                 │
│ Normal agg only │                      │ Sidecar reader   │                          │ Codec pipeline  │
└─────────────────┘                      │ serves queries   │                          │ serves queries  │
                                         └──────────────────┘                          └─────────────────┘
                                               │                                             │
                                               │ segment merged away                         │ subsequent merges
                                               ▼                                             ▼
                                         ┌──────────────────┐                          ┌─────────────────┐
                                         │  Cleaned Up      │                          │  Native Star    │
                                         │  (files deleted)  │                          │  Tree (native)  │
                                         └──────────────────┘                          └─────────────────┘
```

---

## Comparison: Codec Switch vs Sidecar

| Aspect | Codec Switch (Spec 3) | Sidecar (This Spec) |
|--------|----------------------|---------------------|
| Segments with deletes | BROKEN | Works (liveDocs filtering) |
| Write availability | Blocked 27+ seconds | Blocked ~200ms |
| Read availability | Via ReadOnlyEngine bridge | Via live engine |
| GC protection | Automatic (segment file set) | SidecarProtectedDirectory wrapper |
| Star tree coverage | Immediate (all segments) | Gradual (sidecar → native via merges) |
| Crash recovery | SegmentInfos atomic commit | Generational metadata + commit data |
| Codec on existing segments | Changed to Composite912 | Unchanged |
| SegmentInfos modification | Yes (rewrite + commit) | No |
| Reader lifecycle | Lucene codec pipeline | CAS ref counting |
| Cleanup mechanism | N/A | afterMerge → FLUSH thread pool |
| Replica consistency | ReadOnlyEngine null safety | Skip star tree during upgrade |
| Interface coupling | Full CompositeIndexReader | Narrow StarTreeValuesProvider |
