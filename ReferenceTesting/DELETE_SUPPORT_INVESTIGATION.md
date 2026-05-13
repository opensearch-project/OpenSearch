# Star Tree Upgrade — Delete Support Investigation

## Problem

The upgrade fails when the index has had document deletions. The crash occurs during engine restart when `IndexWriter` opens the upgraded segments.

## Background

When documents are deleted (soft deletes), Lucene creates generation-based update files outside `.cfs`:
- `_0_1.fnm` — updated field infos (adds `__soft_deletes` field, potentially renumbers fields)
- `_0_1_Lucene90_0.dvd/dvm` — updated doc values (soft delete markers)

The base `.dvm` file inside `.cfs` retains the ORIGINAL field numbers. The updated `.fnm` may have DIFFERENT field numbers for the same fields.

---

## Error 1: Missing file sets on new SegmentCommitInfo

**Error**: `no_such_file_exception: .../index/_0_1.fnm`

**Why it happened**: `rewriteSegmentInfos()` creates a new `SegmentCommitInfo` and passes `getDelGen()`, `getFieldInfosGen()`, `getDocValuesGen()` to the constructor. But the constructor initializes `dvUpdatesFiles` and `fieldInfosFiles` as empty maps — it doesn't copy them from the original. `SegmentCommitInfo.files()` returns the union of base files + `dvUpdatesFiles` + `fieldInfosFiles`. With empty maps, the generation-based files (`_0_1.fnm`, `_0_1_Lucene90_0.dvd/dvm`) are not referenced in `segments_N+1`. When `IndexWriter` opens and calls `filesExist()`, it can't find these files because they're not in the file set.

**Fix**: `newCommitInfo.setFieldInfosFiles()` + `setDocValuesUpdatesFiles()`.

**Fix location**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeUpgradeService.java`, line 494 — after the `new SegmentCommitInfo(...)` constructor call in `rewriteSegmentInfos()`. Added:
```java
newCommitInfo.setFieldInfosFiles(commitInfo.getFieldInfosFiles());
newCommitInfo.setDocValuesUpdatesFiles(commitInfo.getDocValuesUpdatesFiles());
```

---

## Error 2: ReadOnlyEngine translog assertion

**Error**: `AssertionError: multiple translogs instances should not be opened at the same time`

**Why it happened**: The `ReadOnlyEngine` was created with `null` for `seqNoStats` and `translogStats`. When these are null, the `ReadOnlyEngine` constructor tries to open the translog files to compute them. With deletes, the translog has active state from delete operations — the `InternalEngine` was just closed but the translog lock file may not be released immediately. The `ReadOnlyEngine` tries to open the same translog → assertion failure. This does NOT happen without deletes because the double-flush cleanly commits everything and the translog is in a clean state.

**Fix**: Capture `SeqNoStats`/`TranslogStats` from the live engine before closing it, pass them to the `ReadOnlyEngine` constructor so it uses `NoOpTranslogManager` and never opens the translog files.

**Fix location**: `server/src/main/java/org/opensearch/index/shard/IndexShard.java`, line 2350 — inside `upgradeToStarTree()`, before the `synchronized (engineMutex)` block for Swap 1. Added:
```java
final SeqNoStats capturedSeqNoStats = seqNoStats();
final TranslogStats capturedTranslogStats = translogStats();
```
Then passed `capturedSeqNoStats` and `capturedTranslogStats` to the `ReadOnlyEngine` constructor (lines 2357-2358) instead of `null`.

---

## Error 3: Star tree file suffix mismatch

**Error**: `no_such_file_exception: .../index/_0_1.cim` and `CorruptIndexException: file mismatch, expected suffix=1, got=`

**Why it happened**: `Composite912DocValuesReader` constructs star tree file names using `readState.segmentSuffix`. When `fieldInfosGen != -1`, Lucene sets `segmentSuffix` to the generation value (e.g., `"1"`). This produces `_0_1.cim` instead of `_0.cim`. Star tree files are always written with empty suffix `""`. Similarly, `CodecUtil.checkIndexHeader()` validates the suffix stored in the file header against `readState.segmentSuffix` — the star tree file header has `""` but `readState.segmentSuffix` is `"1"` → mismatch.

**Fix**: Hardcode empty suffix `""` for all star tree file name construction and `CodecUtil.checkIndexHeader` calls in `Composite912DocValuesReader`.

**Fix location**: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesReader.java` — introduced `String starTreeSuffix = ""` at line 86, then replaced `readState.segmentSuffix` with `starTreeSuffix` at 5 locations:
- Line 90: `metaFileName` construction
- Line 96: `dataFileName` construction
- Line 124: `CodecUtil.checkIndexHeader` for data file
- Line 136: `CodecUtil.checkIndexHeader` for meta file
- Line 220: `SegmentReadState` for composite doc values producer

---

## Error 4: Base doc values field number mismatch

**Error**: `CorruptIndexException: Invalid field number: 1 (resource=_0.cfs [slice=_0_Lucene90_0.dvm])`

**Why it happened**: `Composite912DocValuesFormat.fieldsProducer()` passes the UPDATED field infos (from `state.fieldInfos`) to `Lucene90DocValuesProducer` via the delegate. The `Lucene90DocValuesProducer` constructor reads the `.dvm` metadata file and validates each entry's field number against the `FieldInfos`. The `.dvm` was written with ORIGINAL field numbers (e.g., `customer_gender`=0, `currency`=1, ...). The updated field infos may have different numbers for the same fields (because adding `__soft_deletes` can cause renumbering). When the constructor reads field number 1 from the `.dvm` metadata and looks it up in the updated `FieldInfos`, it finds a different field (or no field) → `Invalid field number`.

**Fix attempted**: Read ORIGINAL field infos from `.cfs` via `fieldInfosFormat().read()` and use them for the delegate. This resolved the `CorruptIndexException` — the `Lucene90DocValuesProducer` initialized correctly with matching field numbers.

**Fix location**: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesFormat.java`, line 102 — updated comments in `fieldsProducer()` documenting the field number mismatch. The actual code path is guarded by Fix 5 — segments with `docValuesGen != -1` are skipped in `rewriteSegmentInfos()` and never reach this code with `Composite912Codec`.

---

## Error 5: softDeleteCount assertion (BLOCKER)

**Error**: `AssertionError: softDeleteCount doesn't match 767 != 0`

**Why it happened**: After Fix 4 (using original field infos), the `Lucene90DocValuesProducer` initializes correctly. But `PendingSoftDeletes.onNewReader()` calls `getNumericDocValues("__soft_deletes")` through the `CodecReader`. The call chain is: `CodecReader` → `SegmentDocValuesProducer` → our `Composite912DocValuesReader` → `Lucene90DocValuesProducer`. Since `__soft_deletes` doesn't exist in the original field infos (it was added by the update), the `Lucene90DocValuesProducer` returns null (or NPEs). `PendingSoftDeletes` counts 0 soft deletes instead of the expected 767 → assertion fails.

**What we expected**: `SegmentDocValuesProducer` should route `__soft_deletes` to the update-file producer (opened from `_0_1_Lucene90_0.dvd`), not to our base producer. The `dvUpdatesFiles` and `docValuesGen` are correctly set (verified with logging). But the routing doesn't happen — our base producer is asked for `__soft_deletes`.


**Why `SegmentDocValuesProducer` routing fails**: This is the part we couldn't determine. `SegmentDocValuesProducer` builds a `dvProducers[]` array indexed by field number. For update fields, `dvProducers[N]` should point to the update-file producer. For base fields, it should point to our producer. The `dvUpdatesFiles` are set correctly, `docValuesGen` is correct, update files exist on disk. But somehow the routing doesn't work when `Composite912DocValuesReader` wraps the base `Lucene90DocValuesProducer`. The wrapping may interfere with how `ReadersAndUpdates` initializes the producer routing — this is deep Lucene internals that we couldn't debug further without adding logging to Lucene itself.

---

## Working solution tested

Skip the codec switch for segments with `docValuesGen != -1` in `rewriteSegmentInfos()`. Star tree data is built but the codec isn't switched for those segments. Background merges will eventually produce composite-codec segments.

**Result**: Upgrade succeeds, no crashes, correct doc count. But aggregations return empty because the star tree query path doesn't fall back to normal aggregation when segments lack the composite codec.

**Fix location**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeUpgradeService.java`, line 425 — at the top of the `if (upgradedSegmentNames.contains(...))` block in `rewriteSegmentInfos()`. Added early check:
```java
if (commitInfo.getDocValuesGen() != -1) {
    logger.info("Skipping codec switch for segment {} — has doc values updates (docValuesGen={})...",
        commitInfo.info.name, commitInfo.getDocValuesGen(), commitInfo.info.getCodec().getName());
    newSegmentInfos.add(commitInfo);
    continue;
}
```
Star tree data IS built for these segments — it just won't be read via the native codec path until a background merge produces a clean native composite segment.

---

## Root cause (one sentence)

When we switch the codec to `Composite912Codec` on segments with soft deletes, `Lucene90DocValuesProducer` can't be initialized with the updated field infos (field numbers don't match the base `.dvm` file), and when initialized with original field infos, `SegmentDocValuesProducer` fails to route `__soft_deletes` to the update-file producer — causing the soft delete count assertion to fail.
