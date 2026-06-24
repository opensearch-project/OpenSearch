# Star Tree Upgrade — Delete Support Investigation

## Problem Statement

The per-segment star tree upgrade fails when the index has had document deletions. The crash occurs during engine restart when `IndexWriter` opens the upgraded segments.

## Background: What deletes do to segments

When documents are deleted (soft deletes), Lucene creates generation-based update files outside `.cfs`:
- `_0_1.fnm` — updated field infos (adds `__soft_deletes` field, potentially renumbers fields)
- `_0_1_Lucene90_0.dvd/dvm` — updated doc values (soft delete markers)

The base `.dvm` file inside `.cfs` retains the ORIGINAL field numbers. The updated `.fnm` may have DIFFERENT field numbers for the same fields.

---

## Error 1: Missing generation-tracked file sets

**Error**: `no_such_file_exception: .../index/_0_1.fnm`

**Why**: `rewriteSegmentInfos()` creates a new `SegmentCommitInfo` but never copies `dvUpdatesFiles`/`fieldInfosFiles` from the original. These maps are empty on the new object, so generation-based files are not referenced in `segments_N+1`. `IndexWriter.filesExist()` can't find them.

**Fix**: `newCommitInfo.setFieldInfosFiles(commitInfo.getFieldInfosFiles())` + `setDocValuesUpdatesFiles(commitInfo.getDocValuesUpdatesFiles())` in `StarTreeUpgradeService.java`.

---

## Error 2: ReadOnlyEngine translog assertion

**Error**: `AssertionError: multiple translogs instances should not be opened at the same time`

**Why**: `ReadOnlyEngine` created with null `seqNoStats`/`translogStats` tries to open the translog. With deletes, the translog has active state → assertion failure. Only happens with deletes because the double-flush doesn't fully clean the translog state.

**Fix**: Capture `SeqNoStats`/`TranslogStats` from the live engine before closing, pass to `ReadOnlyEngine` so it uses `NoOpTranslogManager`.

---

## Error 3: Star tree file suffix mismatch

**Error**: `no_such_file_exception: .../index/_0_1.cim` and `CorruptIndexException: file mismatch, expected suffix=1, got=`

**Why**: `Composite912DocValuesReader` uses `readState.segmentSuffix` for star tree file names and `CodecUtil.checkIndexHeader`. When `fieldInfosGen != -1`, Lucene sets `segmentSuffix` to the generation value (e.g., `"1"`), producing `_0_1.cim` instead of `_0.cim`. Star tree files are always written with empty suffix.

**Fix**: Hardcode empty suffix `""` for all star tree file name construction and `CodecUtil.checkIndexHeader` calls in `Composite912DocValuesReader`.

---

## Error 4: Base doc values field number mismatch

**Error**: `CorruptIndexException: Invalid field number: 1 (resource=_0.cfs [slice=_0_Lucene90_0.dvm])`

**Why**: `Composite912DocValuesFormat.fieldsProducer()` passes UPDATED field infos to `Lucene90DocValuesProducer`. The base `.dvm` inside `.cfs` was written with ORIGINAL field numbers. The `Lucene90DocValuesProducer` constructor validates field numbers against the `FieldInfos` → mismatch → `CorruptIndexException`.

**Fix attempted**: Read ORIGINAL field infos from `.cfs` via `fieldInfosFormat().read()` and use them for the delegate. Resolved the `CorruptIndexException`.

---

## Error 5: softDeleteCount assertion (the fundamental blocker for codec switch)

**Error**: `AssertionError: softDeleteCount doesn't match 767 != 0`

**Why**: After using original field infos (Fix 4), `Lucene90DocValuesProducer` initializes correctly. But `PendingSoftDeletes.onNewReader()` calls `getNumericDocValues("__soft_deletes")` through the `CodecReader`. The `__soft_deletes` field doesn't exist in the original field infos (it was added by the update). The producer returns null → `PendingSoftDeletes` counts 0 soft deletes → assertion fails.

**What we tried**:
- Try-catch NPE in `Composite912DocValuesReader` accessor methods, return null → same assertion
- Field number mapping wrapper (translate updated field numbers to original by name) → same assertion
- Both approaches correctly handle the base producer, but `SegmentDocValuesProducer` still routes `__soft_deletes` to our producer instead of the update-file producer

**Root cause**: `SegmentDocValuesProducer` routes by field number from the UPDATED field infos. The base producer (initialized with original field infos) has different field numbers. When `SegmentDocValuesProducer` calls `getNumeric(updatedFieldInfo)` on our producer, the field number doesn't match what the base `.dvm` has → null/NPE. The `__soft_deletes` field should be routed to the update-file producer, but the routing doesn't work when `Composite912DocValuesReader` wraps the base `Lucene90DocValuesProducer`.

**Conclusion**: The codec switch to `Composite912Codec` is fundamentally incompatible with segments that have `docValuesGen != -1`. The field number mismatch between original and updated field infos cannot be resolved at the `fieldsProducer()` level without modifying Lucene internals.

---

## Resolution: Hybrid Approach (Skip Codec Switch + Direct Reader Cache)

Instead of trying to make the codec switch work for segments with soft deletes, the code:

1. **Skips the codec switch** for segments where `docValuesGen != -1` in `rewriteSegmentInfos()`
2. **Still builds star tree data** for those segments (the build path works fine — it reads through `LeafReader` which has the merged view)
3. **Adds star tree files to the segment's file set** (prevents IndexWriter GC)
4. **Rewrites the `.si` file** to persist the expanded file set
5. **Populates `StarTreeDirectReader` cache** for those segments after engine restart
6. **`StarTreeQueryHelper.getStarTreeValues()` Path 2** looks up the direct reader cache by segment name when the native codec path doesn't apply

The `StarTreeDirectReader` opens `.cid`/`.cim`/`.cidvd`/`.cidvm` directly from the directory, completely bypassing the segment's codec. This avoids all field number mismatch issues.

---

## Verification

### Java Integration Test (`StarTreeUpgradeWithDocValuesGenIT`)

Deterministically forces `docValuesGen != -1` via: ingest → flush → delete → flush. Then:
- ✅ Verifies `docValuesGen != -1` on at least one segment
- ✅ Runs full upgrade API (`StarTreeUpgradeAction`)
- ✅ Asserts upgrade succeeds (`successful=1, failed=0`)
- ✅ Asserts direct reader cache is populated (not empty)
- ✅ Asserts star tree is active (`terminated_early=true`)
- ✅ Asserts aggregation values match exactly before vs after

### 1M Scale Test (`test_1m_full_verification.sh`)

1M docs + 50k deletes. Logs confirmed 7 segments with `docValuesGen=1` served via direct reader cache:
```
populateCache: segment=_b docValuesGen=1 codec=Lucene104
populateCache: segment=_c docValuesGen=1 codec=Lucene104
populateCache: segment=_l docValuesGen=1 codec=Lucene104
populateCache: segment=_m docValuesGen=1 codec=Lucene104
populateCache: segment=_n docValuesGen=1 codec=Lucene104
populateCache: segment=_o docValuesGen=1 codec=Lucene104
populateCache: segment=_p docValuesGen=1 codec=Lucene104
```

Results:
- Upgrade: 7.5s (parallel build), successful
- Aggregation: ALL VALUES MATCH (before == after)
- `terminated_early: True` — star tree active on all segments
- 0 wrong values during concurrent reads
- Doc count: 950,000 (correct)

### Reads During Upgrade (`test_reads_correctness_with_deletes.sh`)

100k docs + 5k deletes, concurrent reads during upgrade:
- 14/16 reads returned exact correct values (MATCH)
- 2/16 reads returned empty (during engine swap window)
- 0 reads returned wrong values (MISMATCH)
- Deleted docs never included in any response

---

## Summary

| Segment Type | Codec Switch | Star Tree Read Path | Status |
|-------------|-------------|-------------------|--------|
| `docValuesGen == -1` (no soft delete updates) | ✅ Switch to Composite912Codec | Native `Composite912DocValuesReader` | Works |
| `docValuesGen != -1` (has soft delete updates) | ❌ Skip (would crash) | `StarTreeDirectReader` via cache | Works |
| Delete-only segments (0 live docs) | ❌ Skip (no data to build from) | N/A | Skipped |
