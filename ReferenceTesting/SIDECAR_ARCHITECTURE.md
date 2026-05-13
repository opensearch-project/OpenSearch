# Star Tree Sidecar Upgrade — Full Architecture & Method Reference

## Overview

This document traces every file, class, method, return type, and inheritance chain involved in the sidecar star tree upgrade feature. It covers what each component does, what we added/modified, and the complete execution flow.

The sidecar approach replaces the codec-switching approach. Instead of switching each segment's codec to Composite912Codec (which breaks on segments with soft deletes), star tree files are built as sidecar files alongside existing segments without modifying the codec. The star tree data is discovered at query time through a parallel discovery path.

---

## New Files (6)

### File 1: SidecarProtectedDirectory.java

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/SidecarProtectedDirectory.java`

**Extends**: `FilterDirectory` (Lucene abstract class)

#### What FilterDirectory Does

`FilterDirectory` wraps another `Directory` and delegates all operations to it. Subclasses override specific methods to add behavior.

#### What SidecarProtectedDirectory Does

Intercepts `deleteFile()` calls to protect sidecar star tree files from IndexWriter's garbage collection. `IndexWriter.deleteUnusedFiles()` enumerates referenced files via `SegmentInfos.files()` and deletes everything else. Since sidecar files are NOT in any segment's file set, they would be deleted without this protection.

#### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `protectedFiles` | `Set<String>` (ConcurrentHashMap.newKeySet()) | Thread-safe set of file names protected from deletion |

#### Methods

| Method | Returns | What It Does |
|--------|---------|-------------|
| `deleteFile(String name)` | void | If `protectedFiles.contains(name)`, silently returns (no-op). Otherwise delegates to `super.deleteFile(name)`. |
| `protect(Set<String> fileNames)` | void | Adds file names to the protected set. Called after sidecar build. |
| `unprotect(Set<String> fileNames)` | void | Removes file names from the protected set. Called inside `StarTreeSidecarReader.deleteFiles()` at refCount=0. |
| `isProtected(String fileName)` | boolean | Checks if a file is in the protected set. Used by `Store.cleanupAndVerify()`. |
| `getDelegate()` | Directory | Inherited from FilterDirectory. Returns the underlying directory for bypassing protection. |

#### Constructors

| Constructor | What It Does |
|-------------|-------------|
| `SidecarProtectedDirectory(Directory in)` | Wraps directory with empty protected set |
| `SidecarProtectedDirectory(Directory in, Set<String> initialProtectedFiles)` | Wraps directory with initial protected set (used on shard start) |

#### Where It's Installed

Installed in `IndexShard.innerOpenEngineAndTranslog()` BEFORE engine creation:
1. Load `StarTreeSidecarMetadata` from generational files
2. Create `SidecarProtectedDirectory(store.directory(), metadata.getAllSidecarFileNames())`
3. Call `store.installSidecarDirectory(wrapper)`
4. Engine creation uses `store.engineDirectory()` which returns the wrapper

---

### File 2: StarTreeValuesProvider.java

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeValuesProvider.java`

**Extends**: Nothing (standalone interface)

#### What It Does

Narrow interface for providing star tree values from any source — native codec pipeline or sidecar files. Intentionally NOT extending `CompositeIndexReader` to avoid invasive changes to the existing public API.

#### Methods

| Method | Returns | What It Does |
|--------|---------|-------------|
| `getCompositeIndexFields()` | `List<CompositeIndexFieldInfo>` | Returns list of composite index fields available |
| `getCompositeIndexValues(CompositeIndexFieldInfo)` | `CompositeIndexValues` | Returns star tree values for a given field |

#### Who Implements It

- `Composite912DocValuesReader` — native codec pipeline (added `implements StarTreeValuesProvider`)
- `StarTreeSidecarReader` — sidecar file reader

#### How It's Used

`StarTreeQueryHelper.getStarTreeValues()` checks `CompositeIndexReader` first (existing native path), then falls back to sidecar reader by segment name. The `StarTreeValuesProvider` interface is used by the sidecar reader only — the native path still uses `CompositeIndexReader`.

---

### File 3: StarTreeSidecarMetadata.java

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeSidecarMetadata.java`

**Extends**: Nothing (utility class)

#### What It Does

Manages a generational per-shard metadata file that tracks which segments have sidecar star tree files. Uses the pattern `_startree_sidecar_gen0.meta`, `_startree_sidecar_gen1.meta`, etc. — mirroring Lucene's `segments_N` pattern for crash recovery.

The metadata file is the primary source of truth for crash recovery. If it exists and referenced files are on disk, they are valid regardless of commit data state.

#### Inner Class: SidecarSegmentEntry

Simple record containing `Set<String> files` — the sidecar file names for a segment.

#### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `segments` | `ConcurrentHashMap<String, SidecarSegmentEntry>` | In-memory map of segment name → sidecar files |
| `generation` | `long` | Current generation number (-1 if no gen file written) |

#### Static Methods

| Method | Returns | What It Does |
|--------|---------|-------------|
| `load(Directory)` | `StarTreeSidecarMetadata` | Scans for gen files, loads highest valid, falls back if corrupt, deletes stale gen files |
| `genFileName(long gen)` | `String` | Returns file name for a generation (e.g., `_startree_sidecar_gen3.meta`) |

#### Instance Methods

| Method | Returns | What It Does |
|--------|---------|-------------|
| `hasStarTreeData(String segmentName)` | boolean | Checks if segment has sidecar entry |
| `register(String segmentName, Set<String> files)` | void | Adds entry to in-memory map |
| `remove(String segmentName)` | `Set<String>` | Removes entry, returns removed files |
| `getSegmentNames()` | `Set<String>` | Returns all segment names with sidecar data |
| `getStarTreeFiles(String segmentName)` | `Set<String>` | Returns file names for a segment |
| `getAllSidecarFileNames()` | `Set<String>` | Union of all sidecar files + current metadata gen file |
| `containsFile(String fileName)` | boolean | Checks if any entry contains this file |
| `isEmpty()` | boolean | Checks if no segments have sidecar data |
| `getGeneration()` | long | Returns current generation number |
| `commit(Directory)` | void | Writes genN+1 atomically, deletes genN |
| `removeOrphanedSegments(SegmentInfos)` | `Set<String>` | Removes entries for segments not in SegmentInfos |

#### JSON Format (version=1)

```json
{
  "version": 1,
  "generation": 3,
  "segments": {
    "_0": { "files": ["_0.cid", "_0.cim", "_0.cidvd", "_0.cidvm"] },
    "_2": { "files": ["_2.cid", "_2.cim", "_2.cidvd", "_2.cidvm"] }
  }
}
```

#### Lifecycle

- Loaded once on shard start in `IndexShard.innerOpenEngineAndTranslog()`
- Kept in memory at `IndexShard` level (`starTreeSidecarMetadata` field)
- Mutations under `sidecarMetadataLock` (in-memory only)
- Disk I/O (commit) outside the lock to prevent deadlocks with flush
- Persisted only when actually changed

---

### File 4: LiveDocsFilteredDocValuesProducer.java

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/LiveDocsFilteredDocValuesProducer.java`

**Extends**: `DocValuesProducer` (Lucene abstract class)

#### What It Does

Wraps a `DocValuesProducer` delegate and filters out soft-deleted documents using a `Bits liveDocs` bitset. Remaps document IDs to a contiguous space (0 to numLiveDocs-1).

The sidecar star tree is a self-contained aggregation index. Ordinals in `SortedSetDocValues` and `SortedDocValues` are internal to this filtered view and do NOT correspond to ordinals in the original segment's doc values. There is no per-doc mapping back to the original segment.

#### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `delegate` | `DocValuesProducer` | The underlying producer to wrap |
| `remappedToOriginal` | `int[]` | Maps remapped doc ID → original doc ID |
| `numLiveDocs` | `int` | Number of live (non-deleted) documents |

#### Constructor

```java
LiveDocsFilteredDocValuesProducer(DocValuesProducer delegate, Bits liveDocs, int maxDoc)
```

Builds the `remappedToOriginal` array by iterating all docs and recording live ones:
```java
int remapped = 0;
for (int original = 0; original < maxDoc; original++) {
    if (liveDocs.get(original)) {
        remappedToOriginal[remapped++] = original;
    }
}
```

If `liveDocs` is null (no deletes), creates an identity mapping.

#### Overridden Methods

Each method wraps the delegate's return value in a filtered wrapper that:
1. Translates `advanceExact(remappedId)` to `delegate.advanceExact(remappedToOriginal[remappedId])`
2. Translates `nextDoc()` and `advance(target)` to iterate through the remapped space
3. Returns `numLiveDocs` from `cost()`

| Method | Wrapper Class | What It Wraps |
|--------|--------------|---------------|
| `getSortedNumeric(FieldInfo)` | `FilteredSortedNumericDocValues` | `SortedNumericDocValues` |
| `getNumeric(FieldInfo)` | `FilteredNumericDocValues` | `NumericDocValues` |
| `getSortedSet(FieldInfo)` | `FilteredSortedSetDocValues` | `SortedSetDocValues` |
| `getSorted(FieldInfo)` | `FilteredSortedDocValues` | `SortedDocValues` |
| `getBinary(FieldInfo)` | `FilteredBinaryDocValues` | `BinaryDocValues` |

---

### File 5: StarTreeSidecarReader.java

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeSidecarReader.java`

**Implements**: `StarTreeValuesProvider`, `Closeable`

#### What It Does

Opens sidecar star tree files directly from the directory (independent of the segment's codec) and provides star tree values. Uses Lucene-style CAS reference counting for safe concurrent access.

#### Fields

| Field | Type | Purpose |
|-------|------|---------|
| `sidecarProtectedDirectory` | `SidecarProtectedDirectory` | For deferred file deletion at refCount=0 |
| `starTreeFiles` | `Set<String>` | The sidecar file names |
| `dataIn` | `IndexInput` | Open handle to `.cid` file |
| `compositeIndexInputMap` | `Map<String, IndexInput>` | Star tree data slices per field |
| `compositeIndexMetadataMap` | `Map<String, CompositeIndexMetadata>` | Star tree metadata per field |
| `compositeFieldInfos` | `List<CompositeIndexFieldInfo>` | Parsed field info list |
| `compositeDocValuesProducer` | `DocValuesProducer` | Reader for `.cidvd`/`.cidvm` |
| `readState` | `SegmentReadState` | Read context for doc values |
| `refCount` | `AtomicInteger` | Reference count (starts at 1) |
| `pendingDeletion` | `volatile boolean` | Whether files should be deleted at refCount=0 |

#### Constructor

```java
StarTreeSidecarReader(Directory directory, String segmentName,
    Set<String> starTreeFiles, SidecarProtectedDirectory sidecarProtectedDirectory,
    byte[] segmentId, int maxDoc)
```

Accepts segment ID and maxDoc from `SegmentInfos` (previously tried to parse segment ID from file header, which failed due to Lucene 10 byte order incompatibility). Opens `.cim` and `.cid` files using the same parsing logic as `Composite912DocValuesReader`:
1. Construct `SegmentInfo` with provided `segmentId` and `maxDoc`
2. Open `.cim` (meta) — validate header, parse `StarTreeMetadata` entries
3. Open `.cid` (data) — slice for each star tree field
4. Open `.cidvd`/`.cidvm` via `LuceneDocValuesProducerFactory`
5. Build `compositeFieldInfos` list

Throws `FileNotFoundException`/`NoSuchFileException` if files are missing (stale entry).

#### Reference Counting Methods

| Method | What It Does |
|--------|-------------|
| `incRef()` | CAS loop: if count ≤ 0, throws `AlreadyClosedException`; else CAS increment |
| `decRef()` | Decrement; if 0: `closeInternal()`; if `pendingDeletion`: `deleteFiles()` |
| `markPendingDeletion()` | Sets volatile flag for deferred deletion |
| `close()` | Calls `decRef()` |

#### deleteFiles() — Called ONLY at refCount=0 with pendingDeletion

```java
private void deleteFiles() {
    sidecarProtectedDirectory.unprotect(starTreeFiles);  // NOW safe to unprotect
    for (String file : starTreeFiles) {
        sidecarProtectedDirectory.getDelegate().deleteFile(file);  // bypass wrapper
    }
}
```

Key invariant: `unprotect()` happens INSIDE `deleteFiles()` at refCount=0, NEVER before `decRef()`. This prevents IndexWriter from deleting files that in-flight queries are still reading.

#### StarTreeValuesProvider Methods

| Method | Returns | What It Does |
|--------|---------|-------------|
| `getCompositeIndexFields()` | `List<CompositeIndexFieldInfo>` | Returns parsed field info from `.cim` |
| `getCompositeIndexValues(CompositeIndexFieldInfo)` | `CompositeIndexValues` | Returns `StarTreeValues` for the field |

---

### File 6: StarTreeUpgradeService.java — New Sidecar Methods

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeUpgradeService.java`

**Status**: MODIFIED (added two new methods, existing methods unchanged)

#### New Method: buildSidecarStarTreeData()

```java
public static int buildSidecarStarTreeData(
    Directory directory, StarTreeField starTreeField,
    MapperService mapperService, StarTreeSidecarMetadata metadata)
```

- **Called by**: `IndexShard.upgradeToStarTree()` (sidecar path)
- **What it does**:
  1. Opens `DirectoryReader` snapshot (pins segment file handles)
  2. Iterates segments: skip native Composite912Codec, skip if sidecar exists
  3. For each eligible segment: get `SegmentReader`, get `liveDocs`, call `buildSidecarStarTreeDataForSegment()`
  4. Tracks every file written in `filesWrittenThisRun`
  5. Yields between segments (`Thread.yield()`)
  6. Closes `DirectoryReader`
  7. Reads current `SegmentInfos` — discards sidecar files for merged-away segments
  8. Registers surviving segments in metadata, commits
  9. Finally block: deletes any file in `filesWrittenThisRun` not in committed metadata
- **Returns**: `int` — number of segments upgraded

#### New Method: buildSidecarStarTreeDataForSegment()

```java
static Set<String> buildSidecarStarTreeDataForSegment(
    Directory directory, SegmentCommitInfo commitInfo,
    StarTreeField starTreeField, MapperService mapperService,
    Bits liveDocs, int numLiveDocs)
```

- **Called by**: `buildSidecarStarTreeData()` for each eligible segment
- **What it does**: Same as existing `buildStarTreeData()` but:
  - Wraps `DocValuesProducer` in `LiveDocsFilteredDocValuesProducer` when `liveDocs != null`
  - Uses `numLiveDocs` (not `maxDoc`) for `SegmentWriteState`
  - Returns `Set<String>` of sidecar file names written
- **Returns**: `Set<String>` — e.g., `{"_0.cid", "_0.cim", "_0.cidvd", "_0.cidvm"}`

---

## Modified Files (7)

### File 7: Store.java — engineDirectory() + cleanupAndVerify patch

**Path**: `server/src/main/java/org/opensearch/index/store/Store.java`

#### New Field

| Field | Type | Purpose |
|-------|------|---------|
| `engineDirectory` | `volatile Directory` | The `SidecarProtectedDirectory` wrapper when installed |

#### New Methods

| Method | Returns | What It Does |
|--------|---------|-------------|
| `installSidecarDirectory(SidecarProtectedDirectory)` | void | Sets `engineDirectory` field |
| `engineDirectory()` | `Directory` | Returns `engineDirectory` if non-null, else `directory()` |

#### Modified Method: cleanupAndVerify()

Added check before `deleteFile()`:
```java
if (engineDirectory instanceof SidecarProtectedDirectory
    && ((SidecarProtectedDirectory) engineDirectory).isProtected(existingFile)) {
    continue;  // skip sidecar-protected files
}
```

### File 8: InternalEngine.java — engineDirectory() + merge cleanup

#### Changed: createWriter()

```java
// Before:
return createWriter(store.directory(), iwc);
// After:
return createWriter(store.engineDirectory(), iwc);
```

This ensures IndexWriter uses the `SidecarProtectedDirectory` wrapper.

#### New Field

| Field | Type | Purpose |
|-------|------|---------|
| `sidecarMergeCleanupCallback` | `volatile Runnable` | Callback for sidecar cleanup after merge |

#### New Method

| Method | Returns | What It Does |
|--------|---------|-------------|
| `setSidecarMergeCleanupCallback(Runnable)` | void | Sets the cleanup callback |

#### Modified: EngineMergeScheduler.afterMerge()

Added dispatch to FLUSH thread pool:
```java
Runnable cleanupCallback = sidecarMergeCleanupCallback;
if (cleanupCallback != null) {
    engineConfig.getThreadPool().executor(ThreadPool.Names.FLUSH).execute(() -> {
        cleanupCallback.run();
    });
}
```

### File 9: Engine.java — engineDirectory()

#### Changed: cleanUpUnreferencedFiles()

```java
// Before:
try (IndexWriter writer = new IndexWriter(store.directory(), iwc)) {
// After:
try (IndexWriter writer = new IndexWriter(store.engineDirectory(), iwc)) {
```

### File 10: Composite912DocValuesReader.java — implements StarTreeValuesProvider

Added `implements StarTreeValuesProvider` to class declaration. Methods already existed — no code changes needed.

### File 11: StarTreeQueryHelper.java — sidecar reader wiring + upgrade check

#### Modified: isStarTreeSupported()

Added `&& context.indexShard().isStarTreeUpgradeInProgress() == false` — skips star tree acceleration on shards where upgrade is in progress.

#### Modified: getStarTreeValues() — NOW WIRED TO SIDECAR CACHE

Added `SearchContext` parameter. After checking `CompositeIndexReader` (native path), looks up `StarTreeSidecarReader` from `searchContext.indexShard().getSidecarReaderCache()` by segment name. Uses `incRef()` to protect against concurrent merge cleanup, catches `AlreadyClosedException` for the race window. All 7+ aggregator call sites updated to pass `context`.

### File 12: IndexShard.java — sidecar fields + cleanup + cache population + NO ENGINE RESTART

#### New Fields

| Field | Type | Purpose |
|-------|------|---------|
| `starTreeSidecarMetadata` | `volatile StarTreeSidecarMetadata` | Loaded on shard start |
| `sidecarProtectedDirectory` | `volatile SidecarProtectedDirectory` | The directory wrapper |
| `sidecarMetadataLock` | `final Object` | Lock for in-memory metadata mutations |
| `sidecarReaderCache` | `ConcurrentHashMap<String, StarTreeSidecarReader>` | Cached sidecar readers |

#### New Methods

| Method | Returns | What It Does |
|--------|---------|-------------|
| `isStarTreeUpgradeInProgress()` | boolean | Returns `starTreeUpgradeInProgress.get()` |
| `clearStarTreeUpgradeInProgress()` | void | Sets flag to false |
| `getStarTreeSidecarMetadata()` | `StarTreeSidecarMetadata` | Returns metadata instance |
| `getSidecarReaderCache()` | `ConcurrentHashMap<...>` | Returns reader cache |
| `performSidecarCleanup()` | void | Compares metadata vs SegmentInfos, cleans up orphans |
| `populateSidecarReaderCache()` | void | Creates `StarTreeSidecarReader` for each segment in metadata, using segment ID and maxDoc from `SegmentInfos` |

#### Modified: innerOpenEngineAndTranslog()

Added sidecar metadata loading + `SidecarProtectedDirectory` installation + `populateSidecarReaderCache()` BEFORE engine creation. Also wires `performSidecarCleanup` callback to the engine's merge scheduler.

#### Modified: upgradeToStarTree() — NO ENGINE RESTART

Removed `blockOperations()` / `codecServiceOverride` / `resetEngineToGlobalCheckpoint()`. Now: flush → build sidecar files → install `SidecarProtectedDirectory` (if first upgrade) → `populateSidecarReaderCache()` → wire merge cleanup callback. Star tree acceleration is immediate via sidecar reader cache.

### File 13: TransportStarTreeUpgradeAction.java — flag clearing

#### Modified: shardOperation()

Added `finally` block that calls `indexShard.clearStarTreeUpgradeInProgress()` after `upgradeToStarTree()` returns (success or failure).

---

## Complete Execution Flow (Sidecar Approach — Updated)

```
Client: POST /my-index/_star_tree/upgrade { star_tree: {...} }
  │
  ▼
TransportStarTreeUpgradeAction.doExecute()
  │
  ├─ applyStarTreeMapping() — cluster state update
  │    ├─ Sets composite_index=true, append_only=true
  │    ├─ Adds star tree field to mapping
  │    └─ Propagates to all nodes
  │
  ├─ super.doExecute() → broadcast to all shards
  │    │
  │    ▼
  │  [For each shard on each node:]
  │  shardOperation(request, shardRouting)
  │    │
  │    ├─ indexShard.upgradeToStarTree(starTreeField)
  │    │    │
  │    │    ├─ starTreeUpgradeInProgress.set(true)
  │    │    ├─ flush(force=true) — ensure all segments committed
  │    │    │
  │    │    ├─ [Phase 1: Build sidecar files — ENGINE STAYS LIVE]
  │    │    │    ├─ StarTreeUpgradeService.buildSidecarStarTreeData()
  │    │    │    │    ├─ Open DirectoryReader snapshot (pins file handles)
  │    │    │    │    ├─ For each segment:
  │    │    │    │    │    ├─ Skip if native Composite912Codec
  │    │    │    │    │    ├─ Skip if sidecar already exists
  │    │    │    │    │    ├─ Get liveDocs → wrap in LiveDocsFilteredDocValuesProducer
  │    │    │    │    │    ├─ Build star tree: .cid/.cim/.cidvd/.cidvm
  │    │    │    │    │    └─ Thread.yield()
  │    │    │    │    ├─ Close DirectoryReader
  │    │    │    │    ├─ Compare generations → discard merged-away segments
  │    │    │    │    ├─ Register in metadata, commit
  │    │    │    │    └─ Finally: cleanup uncommitted files
  │    │    │    │
  │    │    │    └─ Reads + writes available throughout
  │    │    │
  │    │    ├─ [Phase 2: Install protection + populate cache — NO ENGINE RESTART]
  │    │    │    ├─ Install SidecarProtectedDirectory (if first upgrade)
  │    │    │    │    OR protect(new files) (if already installed)
  │    │    │    ├─ populateSidecarReaderCache()
  │    │    │    │    ├─ Read SegmentInfos for segment IDs + maxDoc
  │    │    │    │    ├─ For each segment in metadata:
  │    │    │    │    │    └─ Create StarTreeSidecarReader(dir, segName, files, spd, segId, maxDoc)
  │    │    │    │    └─ Put in sidecarReaderCache
  │    │    │    └─ Wire sidecarMergeCleanupCallback to engine
  │    │    │
  │    │    └─ Returns: count of upgraded segments
  │    │
  │    ├─ finally: indexShard.clearStarTreeUpgradeInProgress()
  │    └─ Returns: ShardStarTreeUpgradeResult
  │
  └─ newResponse() → StarTreeUpgradeResponse
       │
       ▼
Client: { "_shards": { "total": 6, "successful": 6, "failed": 0 } }
```

**Key change from previous version**: Phase 2 no longer does `blockOperations()` / `codecServiceOverride` / `resetEngineToGlobalCheckpoint()`. The engine restart was causing terms aggregation to break via normal doc values collection. Instead, we install the `SidecarProtectedDirectory` and populate the sidecar reader cache. Star tree acceleration is immediate via the sidecar reader path.

---

## Post-Upgrade Query Flow (How Sidecar Star Tree Data Is Read — Updated)

```
Search query arrives → StarTreeQueryHelper.isStarTreeSupported()
  │
  ├─ Check: starTreeUpgradeInProgress == false? (skip during upgrade)
  ├─ Check: aggregations != null?
  ├─ Check: compositeIndexPresent?
  ├─ Check: no post filter?
  │
  ▼
StarTreeQueryHelper.getStarTreeValues(leafContext, starTree, searchContext)
  │                                                        ↑ NEW: SearchContext parameter
  ├─ SegmentReader reader = Lucene.segmentReader(context.reader())
  │
  ├─ Path 1: reader.getDocValuesReader() instanceof CompositeIndexReader?
  │    ├─ YES → native composite segment (produced by merge after upgrade)
  │    │    └─ starTreeDocValuesReader.getCompositeIndexValues(starTree)
  │    │         → StarTreeValues (star tree acceleration via native codec)
  │    │
  │    └─ NO → not a native composite segment
  │
  ├─ Path 2: sidecar reader cache lookup ← NEW (was TODO, now wired)
  │    ├─ cache = searchContext.indexShard().getSidecarReaderCache()
  │    ├─ sidecarReader = cache.get(reader.getSegmentName())
  │    │    ├─ Found:
  │    │    │    ├─ sidecarReader.incRef() — protect against concurrent cleanup
  │    │    │    ├─ sidecarReader.getCompositeIndexValues(starTree)
  │    │    │    │    → StarTreeValues (star tree acceleration via sidecar)
  │    │    │    └─ Note: incRef() held for query duration, NOT decRef'd in getStarTreeValues
  │    │    │
  │    │    └─ Not found → return null
  │    │
  │    └─ Catch AlreadyClosedException → remove stale entry, return null
  │
  └─ Path 3: return null → caller falls back to normal aggregation
```

---

## Sidecar File Lifecycle

```
Segment created (no star tree)
  │
  ▼ upgrade API called
  │
Sidecar files built (.cid/.cim/.cidvd/.cidvm)
  ├─ Registered in StarTreeSidecarMetadata
  ├─ Protected by SidecarProtectedDirectory
  ├─ Cached in sidecarReaderCache
  │
  ▼ background merge
  │
Merge consumes sidecar segment → produces native composite segment
  ├─ EngineMergeScheduler.afterMerge() → FLUSH thread pool
  ├─ performSidecarCleanup():
  │    ├─ Under sidecarMetadataLock: remove from metadata, markPendingDeletion + decRef on reader
  │    └─ Outside lock: commit metadata
  ├─ When reader refCount reaches 0:
  │    ├─ closeInternal() — close file handles
  │    └─ deleteFiles() — unprotect() + deleteFile() on underlying directory
  │
  ▼
Sidecar files deleted, native composite segment serves star tree queries
```

---

## GC Protection Chain

```
IndexWriter.deleteUnusedFiles()
  │
  ├─ Enumerates referenced files via SegmentInfos.files()
  ├─ Sidecar files NOT in any segment's file set
  ├─ IndexWriter calls directory.deleteFile("_0.cid")
  │
  ▼
SidecarProtectedDirectory.deleteFile("_0.cid")
  │
  ├─ protectedFiles.contains("_0.cid") → true
  └─ Silently returns (no-op) — file preserved

Store.cleanupAndVerify()
  │
  ├─ Iterates directory.listAll()
  ├─ For each file not in source metadata:
  │    ├─ Check: engineDirectory instanceof SidecarProtectedDirectory
  │    │    && spd.isProtected(existingFile)?
  │    ├─ YES → skip (sidecar file)
  │    └─ NO → delete
```

---

## Reference Counting Flow

```
Cache holds initial reference (refCount=1)
  │
  ├─ Query thread 1: incRef() → refCount=2
  ├─ Query thread 2: incRef() → refCount=3
  │
  ├─ Merge cleanup: markPendingDeletion() + decRef() → refCount=2
  │    (files stay protected — refCount > 0)
  │
  ├─ Query thread 1 finishes: decRef() → refCount=1
  ├─ Query thread 2 finishes: decRef() → refCount=0
  │    ├─ closeInternal() — close file handles
  │    └─ deleteFiles() — unprotect() + delete
  │
  └─ Files deleted safely — no in-flight queries
```

---

## Crash Recovery Flow

```
On shard start:
  │
  ├─ Scan for _startree_sidecar_gen*.meta files
  ├─ Load highest valid generation (fall back if corrupt)
  ├─ Delete all gen files except highest valid
  │
  ├─ For each segment in metadata:
  │    ├─ Files exist on disk → valid, add to protected set
  │    └─ Files missing → stale entry, remove from metadata
  │
  ├─ Create SidecarProtectedDirectory with protected set
  ├─ store.installSidecarDirectory(wrapper)
  │
  ├─ Cross-check commit data (if available):
  │    ├─ Files in commit data but NOT in metadata → incomplete build, clean up
  │    └─ Files in metadata but NOT in commit data → metadata is primary, protect
  │
  └─ Open engine — IndexWriter uses store.engineDirectory() → wrapped directory
       └─ Sidecar files survive crash recovery GC
```


---

## Resolved Issue: Terms Aggregation Empty Buckets — Fixed

### Original Problem

After the star tree upgrade with engine restart (`resetEngineToGlobalCheckpoint()` + `codecServiceOverride`), terms aggregations returned empty buckets. The engine restart broke `SortedSetDocValues` iteration on existing segments.

### Root Cause

The engine restart set `Composite912Codec` as the write codec, but existing segments kept their `.si`-declared codec (`Lucene912Codec`). The `SoftDeletesDirectoryReaderWrapper` interacted differently with `SortedSetDocValues` after the codec override, causing keyword field iteration to break. Star tree was never actually used — all aggregations fell back to normal doc values collection, and the normal path was broken for `SortedSetDocValues`.

### Fix Applied

1. **Removed engine restart entirely** from `upgradeToStarTree()`. No more `blockOperations()`, `codecServiceOverride`, or `resetEngineToGlobalCheckpoint()`.
2. **Wired sidecar reader cache** into `StarTreeQueryHelper.getStarTreeValues()` — added `SearchContext` parameter, lookup by segment name from `IndexShard.getSidecarReaderCache()`.
3. **Populated sidecar reader cache** after build (in `upgradeToStarTree()`) and on shard start (in `innerOpenEngineAndTranslog()`).
4. **Fixed `StarTreeSidecarReader` constructor** — removed manual header parsing (`readSegmentIdFromHeader`) that had a Lucene 10 byte order incompatibility. Now accepts `segmentId` and `maxDoc` from `SegmentInfos` directly.
5. **Fixed `SidecarProtectedDirectory` installation** — on first upgrade of a fresh index, the directory wrapper is now created and installed (previously only called `protect()` on an already-installed wrapper, which was null).

### Bugs Found and Fixed During Implementation

| Bug | Root Cause | Fix |
|-----|-----------|-----|
| Terms empty buckets | Engine restart with `codecServiceOverride` broke `SortedSetDocValues` on existing segments | Removed engine restart entirely |
| `readSegmentIdFromHeader` CorruptIndexException | Manual `readInt()` incompatible with Lucene 10's `MemorySegmentIndexInput` byte order | Removed manual parsing; pass `segmentId` from `SegmentInfos` |
| `maxDoc isn't set yet` IllegalStateException | `SegmentInfo` constructed with `maxDoc=-1`, but `Lucene90DocValuesProducer` calls `maxDoc()` | Pass actual `maxDoc` from `SegmentInfos` |
| `Cannot populate sidecar reader cache` warning | `SidecarProtectedDirectory` was null on first upgrade (never installed for fresh indices) | Install wrapper during upgrade if null |

---

## Resolved Issue: Nested Terms + Sub-Aggregations Wrong Doc Counts — Fixed

### Original Problem

After upgrade, nested terms aggregations with sub-aggregations (e.g., terms + sum) returned wrong `doc_count` values on multi-segment indices. Plain terms and simple metric aggregations were correct.

### Root Cause

`GlobalOrdinalsStringTermsAggregator.getStarTreeBucketCollector()` only called `collectionStrategy.globalOrdsReady(globalOrds)` when `parent != null` (nested sub-collector path). For the top-level terms aggregator (`parent == null`), `globalOrdsReady` was never called in the star tree precompute path because `tryStarTreePrecompute()` returns `true` and skips `getLeafCollector()` which normally initializes it.

Without `globalOrdsReady`, the `DenseGlobalOrds` / `RemapGlobalOrds` strategy had uninitialized internal state, causing bucket ordinals to be wrong and doc counts to land in incorrect buckets.

### Fix

Always call `collectionStrategy.globalOrdsReady(globalOrds)` in `getStarTreeBucketCollector()`, regardless of whether `parent` is null:

```java
// Always initialize globalOrds for the collection strategy — the star tree precompute
// path skips getLeafCollector() which normally calls globalOrdsReady().
SortedSetDocValues globalOrds = valuesSource.globalOrdinalsValues(ctx);
collectionStrategy.globalOrdsReady(globalOrds);
```

### Verification

1M docs, 10 segments, nested terms on `customer_gender` + sum/avg sub-aggs:
- MALE doc_count: 500,915 = 500,915 (exact match before/after) ✅
- MALE revenue: 126,481,784.88 = 126,481,784.88 (exact match) ✅
- FEMALE doc_count: 499,085 = 499,085 (exact match) ✅
- FEMALE revenue: 126,025,998.05 = 126,025,998.05 (exact match) ✅
- All 8 comparison checks passed ✅
