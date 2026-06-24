# Star Tree Hybrid Upgrade — Architecture & Implementation Status

## Overview

This document describes the hybrid approach for retroactive star tree building that combines the per-segment codec-switching approach (for clean segments) with a direct-reader approach (for segments with soft deletes). It traces what has been implemented, what errors were encountered, and what remains to be done.

The core idea: for segments without soft deletes, switch the codec to `Composite912Codec` (the per-segment approach works perfectly). For segments with soft deletes (`docValuesGen != -1`), skip the codec switch but still build star tree data, then read it at query time via a `StarTreeDirectReader` that opens the files independently of the segment's codec.

---

## Why the Hybrid Approach

The per-segment approach (documented in `ARCHITECTURE_DETAILED.md`) works for clean segments but fails on segments with soft deletes due to 5 cascading errors (documented in `DELETE_SUPPORT_INVESTIGATION.md`). The root cause: switching the codec to `Composite912Codec` on a segment with soft deletes causes `Lucene90DocValuesProducer` field number mismatches and `SegmentDocValuesProducer` routing failures that cannot be resolved without modifying Lucene internals.

The sidecar approach (documented in `SIDECAR_ARCHITECTURE.md`) handles soft deletes correctly using `LiveDocsFilteredDocValuesProducer` to filter out deleted docs before building the star tree, and `StarTreeSidecarReader` to read the data independently. But it requires significant infrastructure: `SidecarProtectedDirectory`, `StarTreeSidecarMetadata`, reference counting, merge cleanup callbacks.

The hybrid approach takes the best of both:
- **Clean segments**: Per-segment codec switch (simple, native, no extra infrastructure)
- **Soft-delete segments**: Build star tree from filtered live docs + read via direct reader (no codec switch, no `SegmentDocValuesProducer` routing issues)

---

## Current Implementation State

### What's Committed (on `per-segment-star-tree-upgrade` branch)

These fixes are committed and pushed:

| Fix | File | Line | Description |
|-----|------|------|-------------|
| Fix 1 | `StarTreeUpgradeService.java` | 494 | Copy `fieldInfosFiles` and `dvUpdatesFiles` on new `SegmentCommitInfo` |
| Fix 2 | `IndexShard.java` | 2350 | Capture `SeqNoStats`/`TranslogStats` before engine close for `ReadOnlyEngine` |
| Fix 3 | `Composite912DocValuesReader.java` | 86 | Hardcode empty suffix `""` for star tree file names |
| Fix 4 | `Composite912DocValuesFormat.java` | 102 | Document field number mismatch (guarded by Fix 5) |
| Fix 5 | `StarTreeUpgradeService.java` | 425 | Skip codec switch for segments with `docValuesGen != -1` |

### What's Uncommitted (working changes)

| Change | File | Description |
|--------|------|-------------|
| NEW | `StarTreeDirectReader.java` | Standalone star tree file reader implementing `CompositeIndexReader` |
| NEW | `LiveDocsFilteredDocValuesProducer.java` | Filters soft-deleted docs during star tree build |
| MOD | `IndexShard.java` | Added `starTreeDirectReaderCache`, `populateStarTreeDirectReaderCache()`, `performDirectReaderCleanup()`, merge cleanup wiring |
| MOD | `StarTreeQueryHelper.java` | Added `getStarTreeValues(ctx, starTree, searchContext)` with direct reader fallback (currently DISABLED) |
| MOD | `StarTreeUpgradeService.java` | Added star tree files to file set for skipped segments; skip empty segments; `LiveDocsFilteredDocValuesProducer` wiring |
| MOD | `InternalEngine.java` | Added `directReaderMergeCleanupCallback` field, setter, dispatch in `afterMerge()` |
| MOD | `GlobalOrdinalsStringTermsAggregator.java` | Always call `globalOrdsReady()` in star tree path |
| MOD | 6 aggregator files | Pass `context` to `getStarTreeValues()` for direct reader cache access |

---

## Architecture

### Segment Classification After Upgrade

```
Segments after upgrade:
├── Type A: Clean segments (no soft deletes)
│   ├── Codec switched to Composite912Codec
│   ├── Star tree files in segment file set
│   ├── Read via native codec pipeline (Composite912DocValuesReader)
│   └── Full star tree acceleration
│
├── Type B: Soft-delete segments (docValuesGen != -1)
│   ├── Codec stays as Lucene912Codec / Lucene104Codec
│   ├── Star tree files in segment file set (added in rewriteSegmentInfos)
│   ├── Read via StarTreeDirectReader (bypasses codec pipeline)
│   └── Star tree acceleration via direct reader cache
│
├── Type C: Empty segments (all docs deleted, e.g., tombstone segments)
│   ├── Skipped entirely (no star tree data built)
│   └── No star tree acceleration (falls back to normal aggregation)
│
└── Type D: New segments (flushed after upgrade)
    ├── Written with Composite912Codec (write codec from codecServiceOverride)
    ├── Star tree data built natively during flush
    └── Full star tree acceleration
```

### Query Path (Dual-Path Resolution)

```
StarTreeQueryHelper.getStarTreeValues(ctx, starTree, searchContext)
  │
  ├── Path 1: reader.getDocValuesReader() instanceof CompositeIndexReader?
  │   ├── YES → Type A or Type D segment (native composite)
  │   │   └── Return StarTreeValues from Composite912DocValuesReader
  │   └── NO → Type B or Type C segment
  │
  ├── Path 2: searchContext.indexShard().getStarTreeDirectReaderCache().get(segmentName)?
  │   ├── Found → Type B segment (soft-delete with star tree files)
  │   │   └── Return StarTreeValues from StarTreeDirectReader
  │   └── Not found → Type C segment (no star tree data)
  │
  └── Return null → caller falls back to normal doc values aggregation
```

### Build Path

```
StarTreeUpgradeService.buildStarTreeDataForSegments()
  │
  For each segment:
  ├── Skip if already Composite912Codec
  ├── Skip if no live docs (empty/tombstone segment)
  ├── Build star tree data: .cid/.cim/.cidvd/.cidvm
  │   └── *** PROBLEM: Currently reads ALL docs including soft-deleted ones ***
  │       *** NEEDS: LiveDocsFilteredDocValuesProducer to filter deleted docs ***
  └── Add to upgradedSegmentNames

StarTreeUpgradeService.rewriteSegmentInfos()
  │
  For each upgraded segment:
  ├── If docValuesGen != -1 (soft deletes):
  │   ├── Add star tree files to segment file set (for IndexWriter GC protection)
  │   ├── Do NOT switch codec (avoid Error 4/5)
  │   └── Keep original SegmentCommitInfo with expanded file set
  │
  └── If docValuesGen == -1 (clean):
      ├── Switch codec to Composite912Codec
      ├── Add star tree files to segment file set
      ├── Rewrite .si file
      └── Copy fieldInfosFiles + dvUpdatesFiles (Fix 1)
```

---

## Remaining Issues

### Issue 1: Star Tree Built from ALL Docs (Including Deleted) — FIXED

**Status**: FIXED

**Problem**: `buildStarTreeData()` reads doc values from the `SegmentReader` which includes ALL documents — including soft-deleted ones. The star tree is built from `maxDoc` documents, not `numLiveDocs`. This means aggregation results from the star tree include deleted document values.

**Fix Applied**: Created `LiveDocsFilteredDocValuesProducer` and wired it into `buildStarTreeData()`. When `segmentReader.getLiveDocs()` is non-null, the `DocValuesProducer` is wrapped in `LiveDocsFilteredDocValuesProducer` which filters out soft-deleted documents and remaps doc IDs to contiguous space (0 to numLiveDocs-1). The `SegmentWriteState` uses `numLiveDocs` instead of `maxDoc`.

**Files**:
- NEW: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/LiveDocsFilteredDocValuesProducer.java`
- MOD: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeUpgradeService.java` (line ~259)

### Issue 2: GlobalOrdinalsStringTermsAggregator Empty Buckets with Sub-Aggs — BLOCKER

**Status**: NOT FIXED — fundamental ordinal mismatch

**Problem**: Keyword terms aggregations with sub-aggregations (e.g., `terms` on `customer_gender` + `sum` sub-agg) return empty buckets after upgrade when using the direct reader path. Simple terms (no sub-aggs) works via term frequencies (not star tree). Numeric terms + sub-aggs works correctly.

**Root Cause**: The `GlobalOrdinalsStringTermsAggregator` uses `valuesSource.globalOrdinalsValues(ctx)` and `valuesSource.globalOrdinalsMapping(ctx)` which come from the segment's regular doc values reader (`Lucene912Codec`). But the star tree's dimension values iterator returns ordinals from the star tree's own `SortedSetDocValues` — built by `LiveDocsFilteredDocValuesProducer` with its own ordinal space. These ordinal spaces are different.

For native composite segments (Type A), this works because `Composite912DocValuesReader` IS the segment's doc values reader — ordinals are consistent. For direct-reader segments (Type B), the star tree has its own ordinal space that doesn't match the segment's global ordinals.

**What was tried**:
1. Always calling `collectionStrategy.globalOrdsReady(globalOrds)` in `getStarTreeBucketCollector()` — applied but doesn't fix the ordinal mismatch
2. `LiveDocsFilteredDocValuesProducer` to build correct star tree data — applied but the read-path ordinal mismatch remains

**Current workaround**: The direct reader cache path in `StarTreeQueryHelper.getStarTreeValues()` is DISABLED. Soft-delete segments fall back to normal doc values aggregation. Star tree acceleration only works for clean segments (Type A) where the codec was switched. Background merges will eventually convert all segments to native composite format.

**To fully fix**: Would need to either:
- Map star tree ordinals to segment ordinals at query time (complex — requires building a translation table per segment per query)
- Or use the sidecar approach which avoids this by having the star tree be completely self-contained with its own ordinal space and its own query path that doesn't use `valuesSource.globalOrdinalsValues(ctx)`

### Issue 3: Merge Cleanup for Direct Reader Cache — IMPLEMENTED

**Status**: IMPLEMENTED (but currently unused since direct reader path is disabled)

**Fix Applied**: Added `directReaderMergeCleanupCallback` to `InternalEngine`, dispatched from `afterMerge()` on the FLUSH thread pool. `IndexShard.performDirectReaderCleanup()` compares the cache against current `SegmentInfos` and evicts entries for merged-away segments, closing their file handles.

**Files**:
- MOD: `server/src/main/java/org/opensearch/index/engine/InternalEngine.java` (field at ~178, setter at ~1715, dispatch in `afterMerge` at ~2168)
- MOD: `server/src/main/java/org/opensearch/index/shard/IndexShard.java` (`performDirectReaderCleanup()` method, callback wiring in `upgradeToStarTree()`)

---

## New Files

### StarTreeDirectReader.java

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeDirectReader.java`

**Implements**: `CompositeIndexReader`, `Closeable`

Standalone reader for star tree files that works independently of the segment's declared codec. Uses the same parsing logic as `Composite912DocValuesReader` but without extending `DocValuesProducer` and without requiring a delegate producer.

**Constructor**: `StarTreeDirectReader(Directory directory, SegmentInfo segmentInfo)`
- Opens `.cim` (meta) and `.cid` (data) files using hardcoded empty suffix
- Parses `StarTreeMetadata` entries
- Opens `.cidvd`/`.cidvm` via `LuceneDocValuesProducerFactory`
- Builds `compositeFieldInfos` list

**Methods**:
- `getCompositeIndexFields()` → returns parsed field info list
- `getCompositeIndexValues(CompositeIndexFieldInfo)` → returns `StarTreeValues`
- `close()` → closes all file handles

### LiveDocsFilteredDocValuesProducer.java — CREATED

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/LiveDocsFilteredDocValuesProducer.java`

**Extends**: `DocValuesProducer`

Wraps a `DocValuesProducer` delegate and filters out soft-deleted documents using a `Bits liveDocs` bitset. Remaps document IDs to contiguous space (0 to numLiveDocs-1).

**Constructor**: `LiveDocsFilteredDocValuesProducer(DocValuesProducer delegate, Bits liveDocs, int maxDoc)`
- Builds `remappedToOriginal` array mapping remapped IDs to original doc IDs
- If `liveDocs` is null, creates identity mapping

**Overridden methods**: `getNumeric`, `getSortedNumeric`, `getSorted`, `getSortedSet`, `getBinary` — each wraps the delegate's return value in a filtered wrapper that translates `advanceExact(remappedId)` to `delegate.advanceExact(remappedToOriginal[remappedId])`.

---

## Modified Files

### StarTreeUpgradeService.java

- **Phase 1** (`buildStarTreeDataForSegments`): Skip empty segments (0 live docs). **TODO**: Wrap `DocValuesProducer` in `LiveDocsFilteredDocValuesProducer` for segments with soft deletes.
- **Phase 2** (`rewriteSegmentInfos`): For `docValuesGen != -1` segments: add star tree files to file set but skip codec switch. For clean segments: full codec switch as before.

### IndexShard.java

- Added `starTreeDirectReaderCache` (`ConcurrentHashMap<String, StarTreeDirectReader>`)
- Added `getStarTreeDirectReaderCache()` getter
- Added `populateStarTreeDirectReaderCache()` — reads `SegmentInfos`, finds segments with star tree files but non-Composite912 codec, creates `StarTreeDirectReader` for each
- Called after engine swap in `upgradeToStarTree()`

### StarTreeQueryHelper.java

- Added `getStarTreeValues(ctx, starTree, searchContext)` overload with dual-path resolution
- Original 2-arg overload delegates to new 3-arg with `null` searchContext
- Updated `precomputeLeafUsingStarTree` to pass `SearchContext`

### GlobalOrdinalsStringTermsAggregator.java

- Always call `collectionStrategy.globalOrdsReady(globalOrds)` in `getStarTreeBucketCollector()`, regardless of `parent` being null
- Updated `getStarTreeValues` call to pass `context`

### 5 Other Aggregator Files

Updated `getStarTreeValues(ctx, starTree)` → `getStarTreeValues(ctx, starTree, context)`:
- `DateHistogramAggregator.java`
- `RangeAggregator.java`
- `NumericTermsAggregator.java`
- `MultiTermsAggregator.java`
- `AvgAggregator.java`

---

## Execution Flow

```
Client: POST /my-index/_star_tree/upgrade { star_tree: {...} }
  │
  ▼
TransportStarTreeUpgradeAction.doExecute()
  ├── applyStarTreeMapping() → cluster state update
  ├── super.doExecute() → broadcast to all shards
  │
  ▼ Per shard:
IndexShard.upgradeToStarTree()
  ├── flush(force=true)
  ├── blockOperations(30 min)
  │   ├── flush(waitIfOngoing=true)
  │   ├── Capture SeqNoStats/TranslogStats (Fix 2)
  │   ├── Swap 1: InternalEngine → ReadOnlyEngine
  │   │
  │   ├── Phase 1: buildStarTreeDataForSegments()
  │   │   For each segment:
  │   │   ├── Skip if Composite912Codec
  │   │   ├── Skip if 0 live docs
  │   │   ├── TODO: If liveDocs != null → wrap in LiveDocsFilteredDocValuesProducer
  │   │   ├── TODO: Use numLiveDocs for SegmentWriteState
  │   │   └── Build .cid/.cim/.cidvd/.cidvm
  │   │
  │   ├── Phase 2: rewriteSegmentInfos()
  │   │   For each upgraded segment:
  │   │   ├── If docValuesGen != -1:
  │   │   │   ├── Add star tree files to file set
  │   │   │   └── Keep original codec
  │   │   └── If docValuesGen == -1:
  │   │       ├── Switch codec to Composite912Codec
  │   │       ├── Add star tree files to file set
  │   │       ├── Copy fieldInfosFiles + dvUpdatesFiles (Fix 1)
  │   │       └── Rewrite .si file
  │   │
  │   ├── Swap 2: ReadOnlyEngine → InternalEngine (with codecServiceOverride)
  │   ├── Refresh
  │   └── populateStarTreeDirectReaderCache() for Type B segments
  │
  └── clearStarTreeUpgradeInProgress()
```

---

## Test Results

### 100k docs, ~10k soft deletes (via _delete_by_query), ecommerce schema — PASSING ✅

| Test | Result | Notes |
|------|--------|-------|
| Upgrade succeeds | ✅ PASS | No crashes, all shards successful, 5.061s |
| Doc count preserved | ✅ PASS | 90032 before and after |
| Segments have soft deletes | ✅ PASS | _0–_3 all have deleted_docs > 0, docValuesGen != -1 |
| Keyword terms + sub-agg | ✅ PASS | FEMALE: 45114/11375116.18, MALE: 44918/11340854.77 — exact match |
| All comparison checks | ✅ PASS | doc_count and revenue match exactly before/after |

### 100k docs, no deletes — Native path test ✅

| Test | Result | Notes |
|------|--------|-------|
| Upgrade succeeds | ✅ PASS | All segments codec-switched to Composite912Codec |
| Aggregation matches | ✅ PASS | FEMALE: 50124/12628139.63, MALE: 49876/12602218.65 — exact match |

### 100k docs, ~10k soft deletes — Merge convergence test ❌

| Test | Result | Notes |
|------|--------|-------|
| Upgrade succeeds | ✅ PASS | Soft-delete segments use direct reader, clean segment uses native |
| Aggregation after upgrade | ✅ PASS | Matches baseline exactly |
| Force merge convergence | ❌ FAIL | Force merge didn't merge segments (append_only interference?) |
| Aggregation after merge | ❌ FAIL | Only returns data from the one clean segment |

**Merge convergence issue**: The `_forcemerge` API didn't merge the soft-delete segments. This may be related to the `append_only=true` setting applied during upgrade, or the merge policy not selecting segments with the old codec. Needs separate investigation.

---

## Next Steps

1. **Run debug instrumentation** — Activate with `-Dopensearch.startree.debug=true` in `jvm.options`, run the 1M delete test, and grep logs for `[STARTREE DEBUG]` and `=== ORDINAL DUMP`. This will reveal which pattern (A/B/C/D) we're hitting.
2. **Fix ordinal mismatch based on debug output** — If Pattern B (ordinal mismatch), build a translation table. If Pattern D (ordinals match), the bug is in bucket counting.
3. **Re-enable direct reader path** — Once the root cause is confirmed and fixed
4. **Test crash recovery** — Verify star tree files survive node restart
5. **Test merge convergence** — Verify background merges convert Type B segments to native composite

---

## Debug Instrumentation

Debug logging is activated with JVM flag: `-Dopensearch.startree.debug=true`

### Instrumented Locations

| Location | What It Logs |
|----------|-------------|
| `StarTreeQueryHelper.getStarTreeValues()` | Which path each segment takes (NATIVE / DIRECT / FALLBACK), cache keys, cache hits |
| `GlobalOrdinalsStringTermsAggregator.getStarTreeBucketCollector()` | Full ordinal dump: star tree ordinals vs segment ordinals, translation table |
| `IndexShard.populateStarTreeDirectReaderCache()` | Which segments are cached, their docValuesGen, codec, files |

### New Debug Files

| File | Purpose |
|------|---------|
| `StarTreeDebugUtils.java` | Static utility for dumping ordinal spaces (DELETE after debugging) |

## Debug Findings (from 100k doc test with 5k deletes)

### Observation: Pattern A — Cache Empty

All segments hit `DIRECT_READER_CHECK` → `cacheKeys=[]` → `cacheHit=false` → `FALLBACK`.

**Root cause**: `populateStarTreeDirectReaderCache()` is not populating the cache. No `populateCache:` log lines appear. Either the method isn't being called, or the loop finds no matching segments because the `SegmentInfos` read inside `populateStarTreeDirectReaderCache()` sees the NEW commit (segments_6) where the star tree files are in the file set, but the method checks `commitInfo.info.getCodec().getName()` against `Composite912Codec` — and since ALL segments kept their original codec (Lucene104), none are skipped by the first check. But then the `.cim` file check should find them...

**Additional finding**: No segment takes the NATIVE path. ALL segments have `docValuesGen != -1` (because all had deletes), so the codec switch was skipped for every segment. There are zero Type A segments. This means the entire index has no star tree acceleration at all — every segment falls back to normal aggregation.

**The `terminated_early: true` mystery**: The response shows `terminated_early: true` even though no star tree data is used. This comes from the `StarTreeQueryContext` being set up (because `isStarTreeSupported()` returns true after the mapping update adds composite field types), but then every segment's `tryStarTreePrecompute()` returns false. The `terminated_early` flag is set by a different optimization path (term frequencies for simple terms, or filter rewrite).

### Segments observed

| Segment | dvReaderType | Path | Notes |
|---------|-------------|------|-------|
| _0 | SegmentDocValuesProducer | FALLBACK | Has deletes, codec not switched |
| _1 | SegmentDocValuesProducer | FALLBACK | Has deletes, codec not switched |
| _2 | SegmentDocValuesProducer | FALLBACK | Has deletes, codec not switched |
| _3 | SegmentDocValuesProducer | FALLBACK | Has deletes, codec not switched |
| _4 | SegmentDocValuesProducer | FALLBACK | Has deletes, codec not switched |
| _5 | FieldsReader | FALLBACK | Tombstone segment (0 live docs) |
| _6 | FieldsReader | FALLBACK | Tombstone segment (0 live docs) |

### Next debug step

**RESOLVED**: The `.si` file rewrite fix resolved the cache population issue. The cache is now populated with 5 segments (`cacheKeys=[_0, _1, _2, _3, _4]`).

**NEW FINDING**: Ordinals actually MATCH (Pattern D). `starOrd=0 term=FEMALE → segOrd=0 OK`. The ordinal mismatch hypothesis was wrong.

**NEW ISSUE**: The star tree data includes ALL docs (100k) instead of only live docs (95k). After upgrade, `MALE + FEMALE = 100000` (full doc count) instead of `95000` (live doc count). This means `LiveDocsFilteredDocValuesProducer` is either not being applied, or `segmentReader.getLiveDocs()` returns null (because the `DirectoryReader` opened in `buildStarTreeData()` wraps segments in `SoftDeletesDirectoryReaderWrapper` which already filters — so `getLiveDocs()` returns null at the `SegmentReader` level, and the filtering happens at a higher level).

**Root cause**: The `DirectoryReader.open(directory)` in `buildStarTreeData()` opens with `SoftDeletesDirectoryReaderWrapper` (because OpenSearch configures soft deletes). The `SegmentReader` returned by `directoryReader.leaves()` is already wrapped — `getLiveDocs()` returns null because the wrapper handles filtering. But our `LiveDocsFilteredDocValuesProducer` check is `if (liveDocs != null)` — it's never triggered.

**Fix needed**: Get live docs from `SegmentCommitInfo` directly (via `commitInfo.getDelCount() + commitInfo.getSoftDelCount()`) or use `segmentReader.getLiveDocs()` from the unwrapped reader, or read the soft delete doc values directly to build the liveDocs bitset.

**Fix applied**: Created `buildLiveDocsBitset()` method in `StarTreeUpgradeService` that:
1. Checks `commitInfo.getDelCount()` and `commitInfo.getSoftDelCount()` — returns null if both are 0
2. Starts with all bits set (all live)
3. Applies hard deletes from `segmentReader.getLiveDocs()` (the `.liv` file)
4. Reads `__soft_deletes` numeric doc values directly from `segmentReader.getDocValuesReader()` and clears bits for docs with value=1

**Additional fix**: The `.si` file for skipped segments (docValuesGen != -1) was not being rewritten — only the in-memory `SegmentInfo.setFiles()` was called, but the on-disk `.si` still had the old file set. Added `directory.deleteFile(siFileName)` + `codec.segmentInfoFormat().write()` for skipped segments too.

**Status**: Both fixes compiled and partially tested. The cache population now works (`cacheKeys=[_0, _1, _2, _3, _4]`). Ordinals confirmed matching (Pattern D). The `buildLiveDocsBitset` fix needs further testing with a scenario where soft deletes are properly committed to the original segments (not just in tombstone segments).

### Debug Run 2: After .si Rewrite Fix + buildLiveDocsBitset

**Test**: 100k docs, 5k deletes, ecommerce schema

**Results**:
- Before upgrade: `MALE: 49926, FEMALE: 49749` (total ≈ 95k live docs — correct)
- After upgrade: `MALE: 50115, FEMALE: 49885` (total = 100k — includes deleted docs!)

**What the logs showed**:
```
segment=_0 path=DIRECT values=FOUND    ← cache hit, direct reader working
segment=_4 path=DIRECT values=FOUND    ← cache hit, direct reader working
starOrd=0 term=FEMALE → segOrd=0 OK    ← ordinals match!
starOrd=1 term=MALE → segOrd=1 OK      ← ordinals match!
```

**Diagnosis**: Cache population works. Ordinals match (Pattern D — no translation needed). But the star tree data still includes deleted docs. The `buildLiveDocsBitset()` method was added but `segmentReader.getLiveDocs()` returns null AND the `__soft_deletes` doc values field may not be accessible from the raw `DocValuesProducer` because the soft delete updates are in generation-based files (`_0_2_Lucene90_0.dvd`) that the base `DocValuesProducer` doesn't see.

**Key insight**: The `SegmentReader.getDocValuesReader()` returns the BASE doc values producer (from inside `.cfs`). The soft delete field `__soft_deletes` is stored in the UPDATE files (`_0_2_Lucene90_0.dvd/dvm`), not in the base. The `SegmentDocValuesProducer` (which routes between base and update producers) is what provides `__soft_deletes` — but we're reading from the raw `getDocValuesReader()` which is just the base.

**The real fix**: Read `__soft_deletes` from the `SegmentReader` itself (not from `getDocValuesReader()`). Use `segmentReader.getNumericDocValues("__soft_deletes")` which goes through `SegmentDocValuesProducer` and correctly routes to the update file. Or alternatively, use `ctx.reader().getNumericDocValues("__soft_deletes")` from the leaf reader context.

### Debug Run 3: Inconsistent Test Scenario

**Test**: Same 100k test but on a fresh OpenSearch instance

**Results**:
- Doc count before upgrade: 100000 (not 95000!)
- Segments: `_0` through `_4` show 0 deleted docs, `_5` shows 5000 deleted

**Diagnosis**: The deletes went into a tombstone segment (`_5`) but were NOT applied as soft-delete updates to the original segments. This happens when the delete operations are flushed into a new segment before the soft-delete doc values updates are applied to the original segments. The original segments have `docValuesGen == -1` (no updates), so the codec switch works for them. But the upgrade completed in 0.352s (too fast — no star tree data was built because all segments were skipped as "already Composite912Codec" or had 0 live docs).

**Root cause of inconsistency**: The test script does `flush` after deletes, but whether the soft-delete updates are applied to the original segments depends on timing and the merge policy. Sometimes the updates are applied (creating `docValuesGen != -1`), sometimes they stay only in the tombstone segment.

**Reliable test pattern**: To guarantee soft deletes are applied to original segments:
1. Ingest docs → flush (creates segments with data)
2. Delete docs → flush (creates tombstone segment)
3. **Force merge** or wait for background merge (merges tombstone with data segments, applying soft deletes)
4. OR: Delete docs using `_update` API which directly applies soft-delete DV updates to existing segments

---

## Summary of Bugs Found and Fixed

| # | Bug | Root Cause | Fix | Status |
|---|-----|-----------|-----|--------|
| 1 | Missing file sets on SegmentCommitInfo | Constructor doesn't copy dvUpdatesFiles/fieldInfosFiles | `setFieldInfosFiles()` + `setDocValuesUpdatesFiles()` | ✅ Committed |
| 2 | ReadOnlyEngine translog assertion | null SeqNoStats causes translog open | Capture stats before engine close | ✅ Committed |
| 3 | Star tree file suffix mismatch | `readState.segmentSuffix` != "" for updated segments | Hardcode `starTreeSuffix = ""` | ✅ Committed |
| 4 | Base doc values field number mismatch | Updated FieldInfos vs original .dvm | Guarded by Fix 5 | ✅ Committed |
| 5 | softDeleteCount assertion (BLOCKER) | SegmentDocValuesProducer routing failure | Skip codec switch for docValuesGen != -1 | ✅ Committed |
| 6 | Cache empty (cacheKeys=[]) | .si file not rewritten for skipped segments | Rewrite .si with expanded file set | ✅ Uncommitted |
| 7 | Star tree includes deleted docs | getLiveDocs() returns null on wrapped reader | `buildLiveDocsBitset()` from soft-delete DV field | ⚠️ Partially fixed |
| 8 | buildLiveDocsBitset reads wrong producer | Base DocValuesProducer doesn't have __soft_deletes | Use `segmentReader.getNumericDocValues()` instead | ✅ Fixed |
| 9 | LiveDocsFilteredDocValuesProducer.nextDoc() throws | Star tree builder uses sequential iteration | Implement nextDoc()/advance() in filtered wrappers | ✅ Fixed |

---

## Next Steps

1. **Remove debug instrumentation** — Revert `STAR_TREE_DEBUG = true` to `Boolean.getBoolean(...)`, delete `StarTreeDebugUtils.java`, remove debug log lines.
2. **Run 1M doc test** — Verify at scale with the full `test_1m_deletes_direct_reader.sh` test.
3. **Test merge convergence** — Verify background merges convert Type B segments to native composite and evict direct reader cache entries.
4. **Test crash recovery** — Verify star tree files survive node restart.
5. **Commit and push** — Once all tests pass at scale.

---

## Bug #10: Force Merge Fails with NoSuchFileException on Merged Segment — FIXED

**Status**: ✅ FIXED

### Symptom

After a successful star tree upgrade on clean segments (Type A, codec switched to `Composite912Codec`), calling `_forcemerge?max_num_segments=1` fails with:

```
NoSuchFileException: .../index/_2_Lucene90_0.dvm
```

The shard then enters a failed recovery loop, repeatedly trying to open the merged segment and failing on the same missing file.

### Investigation

1. Listed the index directory after the merge attempt — found that the merged segment `_2` has files with **empty suffix** (`_2.dvd`, `_2.dvm`) but the reader was looking for `_2_Lucene90_0.dvm` (per-field suffixed name).

2. Files actually present for segment `_2`:
   ```
   _2.cid, _2.cidvd, _2.cidvm, _2.cim     ← star tree files (empty suffix) ✓
   _2.dvd, _2.dvm                           ← regular doc values (empty suffix) ✓
   _2.fnm, _2.si, _2.fdt, _2.fdm, etc.     ← other segment files ✓
   _2_Lucene104_0.doc, _2_Lucene104_0.tim   ← postings (per-field suffix) ✓
   _2_Lucene90_0.dvm                        ← DOES NOT EXIST ✗
   ```

3. The merge **writes** doc values with empty suffix because `Composite912DocValuesFormat.fieldsConsumer()` wraps `Lucene90DocValuesFormat` directly (not through `PerFieldDocValuesFormat`). So the writer creates `_2.dvd` and `_2.dvm`.

4. But the **reader** (`Composite912DocValuesFormat.fieldsProducer()`) checks `getPerFieldDocValuesSuffix(state.fieldInfos)` which finds `PerFieldDocValuesFormat.format=Lucene90` and `PerFieldDocValuesFormat.suffix=0` attributes in the merged segment's `FieldInfos`. These attributes are **inherited from the source segments** during the merge — they're stale for the merged segment.

5. The reader then constructs `suffixedState` with suffix `"Lucene90_0"` and calls `delegate.fieldsProducer(suffixedState)` which tries to open `_2_Lucene90_0.dvm` → `NoSuchFileException`.

### Root Cause

`Composite912DocValuesFormat.fieldsProducer()` unconditionally trusts `PerFieldDocValuesFormat` attributes in `FieldInfos` to determine the doc values file suffix. This works for **upgraded segments** (where the codec was switched from Lucene912 to Composite912 without rewriting doc values files — the original per-field files like `_0_Lucene90_0.dvd` still exist). But it fails for **merged segments** produced natively by `Composite912Codec`, where doc values are written with empty suffix but `FieldInfos` still carry the per-field attributes from source segments.

### Fix Applied

Added a file-existence check before using the per-field suffix:

```java
// In Composite912DocValuesFormat.fieldsProducer():
String perFieldSuffix = getPerFieldDocValuesSuffix(state.fieldInfos);
if (perFieldSuffix != null && perFieldSuffixedFileExists(state, perFieldSuffix)) {
    // Upgraded segment: use per-field suffix
    SegmentReadState suffixedState = new SegmentReadState(..., perFieldSuffix);
    regularProducer = delegate.fieldsProducer(suffixedState);
} else {
    // Native Composite912 segment (or merged segment): use empty suffix
    regularProducer = delegate.fieldsProducer(state);
}
```

Helper method:
```java
private static boolean perFieldSuffixedFileExists(SegmentReadState state, String perFieldSuffix) {
    String suffixedMetaFile = IndexFileNames.segmentFileName(
        state.segmentInfo.name, perFieldSuffix, "dvm"
    );
    try {
        state.directory.openInput(suffixedMetaFile, state.context).close();
        return true;
    } catch (FileNotFoundException | NoSuchFileException e) {
        return false;
    } catch (IOException e) {
        return false;
    }
}
```

**File**: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesFormat.java`

### Test Results After Fix

| Test | Result | Notes |
|------|--------|-------|
| Upgrade (2 clean segments, 100k docs) | ✅ PASS | Both segments codec-switched to Composite912 |
| Aggregation after upgrade | ✅ PASS | Exact match with baseline |
| Force merge to 1 segment | ⚠️ PARTIAL | Merge completes (segment `_2` created with star tree), but API reports "flushes are disabled" error |
| Aggregation after merge | ✅ PASS | Exact match with baseline |
| Merged segment has star tree files | ✅ PASS | `_2.cid`, `_2.cim`, `_2.cidvd`, `_2.cidvm` all present |

---

## Bug #11: Force Merge API Reports "flushes are disabled - pending translog recovery" — FIXED

**Status**: ✅ FIXED

### Symptom

After star tree upgrade, calling `_forcemerge` returns:

```json
{
  "_shards": { "total": 1, "successful": 0, "failed": 1 },
  "failures": [{
    "reason": {
      "type": "illegal_state_exception",
      "reason": "[ecom_mixed][0] flushes are disabled - pending translog recovery"
    }
  }]
}
```

Despite this error, the merge **does** execute — a new merged segment (`_2`) is created with correct star tree data. But the old segments (`_0`, `_1`) are not removed from the commit point because the final flush/commit that would clean them up is blocked.

### Investigation

1. Shard state is `STARTED`, no active recoveries, translog has 0 operations
2. Manual `_flush?force=true` succeeds (returns 200 with successful=1)
3. But `_forcemerge` still fails with the same error on retry
4. The merged segment exists on disk and queries return correct results (reading from all 3 segments, with the merged segment containing the union)

### Root Cause

The `upgradeToStarTree()` method creates a new `InternalEngine` via `engineFactory.newReadWriteEngine()`. The `InternalTranslogManager` constructor sets `pendingTranslogRecovery = true` and this flag is only cleared after `recoverFromTranslog()` or `skipTranslogRecovery()` is called. Since `upgradeToStarTree()` creates the engine outside the normal shard recovery flow, neither method was being called — leaving `pendingTranslogRecovery = true` permanently. Any subsequent flush (including the one inside `_forcemerge`) checks `ensureCanFlush()` which throws `IllegalStateException("flushes are disabled - pending translog recovery")`.

### Fix Applied

Added `newEngine.translogManager().skipTranslogRecovery()` immediately after the new `InternalEngine` is created in `upgradeToStarTree()`. This is safe because:
1. We flushed all data before the engine swap (both first flush and second flush inside `blockOperations`)
2. The translog is empty — there's nothing to recover
3. The `skipTranslogRecovery()` call simply sets `pendingTranslogRecovery = false`, enabling flushes

```java
// In IndexShard.upgradeToStarTree(), after creating the new engine:
synchronized (engineMutex) {
    Engine roEngine = currentEngineReference.get();
    newEngine = engineFactory.newReadWriteEngine(newEngineConfig(replicationTracker));
    onNewEngine(newEngine);
    currentEngineReference.set(newEngine);
    IOUtils.close(roEngine);
}
// Skip translog recovery — we flushed all data before the engine swap,
// so the translog is empty.
newEngine.translogManager().skipTranslogRecovery();
```

Additionally, a post-upgrade flush was added in `TransportStarTreeUpgradeAction.shardOperation()` as a safety net:
```java
indexShard.flush(new FlushRequest().force(false).waitIfOngoing(true));
```

**Files**:
- MOD: `server/src/main/java/org/opensearch/index/shard/IndexShard.java` (in `upgradeToStarTree()`, after engine swap)
- MOD: `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java` (in `shardOperation()`)

### Test Results After Fix

| Test | Result | Notes |
|------|--------|-------|
| Force merge after upgrade | ✅ PASS | `successful=1, failed=0` |
| Single segment after merge | ✅ PASS | Only `_2` remains, `_0` and `_1` cleaned up |
| Aggregation after merge | ✅ PASS | Exact match with baseline |
| Star tree in merged segment | ✅ PASS | `.cid/.cim/.cidvd/.cidvm` files present for `_2` |

---

## Updated Summary of Bugs Found and Fixed

| # | Bug | Root Cause | Fix | Status |
|---|-----|-----------|-----|--------|
| 1 | Missing file sets on SegmentCommitInfo | Constructor doesn't copy dvUpdatesFiles/fieldInfosFiles | `setFieldInfosFiles()` + `setDocValuesUpdatesFiles()` | ✅ Committed |
| 2 | ReadOnlyEngine translog assertion | null SeqNoStats causes translog open | Capture stats before engine close | ✅ Committed |
| 3 | Star tree file suffix mismatch | `readState.segmentSuffix` != "" for updated segments | Hardcode `starTreeSuffix = ""` | ✅ Committed |
| 4 | Base doc values field number mismatch | Updated FieldInfos vs original .dvm | Guarded by Fix 5 | ✅ Committed |
| 5 | softDeleteCount assertion (BLOCKER) | SegmentDocValuesProducer routing failure | Skip codec switch for docValuesGen != -1 | ✅ Committed |
| 6 | Cache empty (cacheKeys=[]) | .si file not rewritten for skipped segments | Rewrite .si with expanded file set | ✅ Uncommitted |
| 7 | Star tree includes deleted docs | getLiveDocs() returns null on wrapped reader | `buildLiveDocsBitset()` from soft-delete DV field | ⚠️ Partially fixed |
| 8 | buildLiveDocsBitset reads wrong producer | Base DocValuesProducer doesn't have __soft_deletes | Use `segmentReader.getNumericDocValues()` instead | ✅ Fixed |
| 9 | LiveDocsFilteredDocValuesProducer.nextDoc() throws | Star tree builder uses sequential iteration | Implement nextDoc()/advance() in filtered wrappers | ✅ Fixed |
| 10 | Force merge NoSuchFileException on merged segment | Stale PerFieldDocValuesFormat attributes in merged FieldInfos | `perFieldSuffixedFileExists()` check before using suffix | ✅ Fixed |
| 11 | "flushes are disabled - pending translog recovery" | Engine swap leaves pendingTranslogRecovery=true permanently | `skipTranslogRecovery()` after engine creation | ✅ Fixed |

---

## Updated Next Steps

1. ~~**Fix Bug #11**~~ — ✅ Fixed with `skipTranslogRecovery()`
2. **Test with soft deletes** — Run the mixed-state test with actual soft-delete segments (Type A + Type B) to validate the full hybrid path
3. **Remove debug instrumentation** — Clean up debug logging
4. **Run 1M doc test** — Verify at scale
5. **Commit Bugs #10 + #11 fixes** — Push the `perFieldSuffixedFileExists` and `skipTranslogRecovery` changes

---

## Codec Naming After Merge — VERIFIED CORRECT

### Key Finding

After force merge, the merged segment uses **`Composite104Codec`** — verified by parsing the `segments_N` file (where Lucene stores the per-segment codec name as a length-prefixed string). This is the **same codec** used by natively-created star tree indices.

```
Native star tree index (created with index.composite_index=true):
  segments_3 → codec = "Composite104Codec" (exact match, Lucene string at offset 74)

Upgraded + force-merged index (retroactive upgrade → merge):
  segments_8 → codec = "Composite104Codec" (exact match, Lucene string at offset 74)

Both identical. ✓
```

### The Two Composite Codecs

| Codec | Wraps | Used For | docValuesFormat() |
|-------|-------|----------|-------------------|
| `Composite912Codec` | `Lucene912Codec` (backward compat) | Retroactively upgraded segments (manual `.si` rewrite) | `Composite912DocValuesFormat` |
| `Composite104Codec` | `Lucene104Codec` (current) | Natively written segments (flush/merge after upgrade) | `Composite912DocValuesFormat` |

Both use the **same** `Composite912DocValuesFormat` for reading/writing star tree data. The "912" vs "104" distinction is about which base codec they wrap, not about the doc values format.

### Where the Codec Name is Stored

- **`segments_N` file**: Stores the codec name per segment as a Lucene string. This is what Lucene uses to instantiate the correct codec via SPI (`Codec.forName("Composite104Codec")`) when opening a segment.
- **`.si` file**: Stores segment metadata (doc count, files, diagnostics, etc.) but the codec name is NOT in the `.si` file itself. However, the `.si` file is WRITTEN BY the codec's `SegmentInfoFormat` — so the format of the `.si` file implicitly identifies which codec wrote it (via the header string `Lucene90SegmentInfo`).

### Why Rewriting `.si` Works in Our Upgrade

In `rewriteSegmentInfos()`, we do:
```java
new Composite912Codec().segmentInfoFormat().write(directory, newInfo, IOContext.DEFAULT);
```

This rewrites the `.si` file, but the codec name change is actually committed via:
```java
newSegmentInfos.commit(directory);  // writes segments_N+1
```

The `SegmentCommitInfo` in `newSegmentInfos` references `newInfo` which has `new Composite912Codec()` set as its codec. When `SegmentInfos.commit()` writes `segments_N+1`, it serializes the codec name (`"Composite912Codec"`) for that segment. When Lucene later opens the index, it reads `segments_N`, sees `"Composite912Codec"`, calls `Codec.forName("Composite912Codec")` via SPI, and uses that codec to read the segment.

The `.si` rewrite is needed to update the **file set** (adding star tree files) — not for the codec name itself.

### Segment Lifecycle After Upgrade

```
1. Before upgrade:
   segments_N → _0: codec="Lucene104Codec", _1: codec="Lucene104Codec"

2. After rewriteSegmentInfos() (clean segments):
   segments_N+1 → _0: codec="Composite912Codec", _1: codec="Composite912Codec"
   (.si files rewritten with expanded file set including .cid/.cim/.cidvd/.cidvm)

3. After force merge:
   segments_N+2 → _2: codec="Composite104Codec"
   (IndexWriter uses Composite104Codec from codecServiceOverride)
   (_0 and _1 deleted)

4. Steady state = same as native star tree index:
   All segments use Composite104Codec
```

### The perFieldSuffixedFileExists Fix (Bug #10) — Context

Retroactively upgraded segments (`Composite912Codec`) have per-field suffixed doc values files (`_0_Lucene90_0.dvd`) because they were originally written by `PerFieldDocValuesFormat` inside `Lucene104Codec`.

Merged segments (`Composite104Codec`) have empty-suffix doc values files (`_2.dvd`) because `Composite104Codec.docValuesFormat()` returns `Composite912DocValuesFormat` directly (not through `PerFieldDocValuesFormat`).

When `Composite912DocValuesFormat.fieldsProducer()` opens a merged segment, the `FieldInfos` still carry stale `PerFieldDocValuesFormat` attributes from source segments. The `perFieldSuffixedFileExists()` check prevents using these stale attributes on merged segments where the suffixed files don't exist.


---

## Bug #12: Force Merge Star Tree Includes Deleted Docs (Merge Fallback Path) — FIXED

**Status**: ✅ FIXED

### Symptom

After upgrading an index with soft deletes and then force-merging:
- The upgrade correctly excludes deleted docs (Bug #7 fix works: 90032 docs, not 100k)
- But after force merge, the star tree in the merged segment includes ALL docs (100k)
- Aggregation after merge returns pre-delete counts instead of post-delete counts

### Test Results

```
Before delete:  FEMALE=50124, MALE=49876 (total=100000)
After delete:   FEMALE=45114, MALE=44918 (total=90032)  ← 9968 deleted
After upgrade:  FEMALE=45114, MALE=44918 (total=90032)  ← Bug #7 fix works! ✓
After merge:    FEMALE=50124, MALE=49876 (total=100000) ← WRONG! Includes deleted docs
```

### Segments Before Merge

```
_0: docs=90032, deleted=9968  ← original data segment with soft deletes
_1: docs=0, deleted=9968      ← tombstone segment (all entries are soft-deleted)
```

### Root Cause

During force merge, `Composite912DocValuesWriter.mergeStarTreeFields()` hits the fallback path:
```java
if (starTreeSubsPerField.isEmpty() && compositeMappedFieldTypes.isEmpty() == false) {
    if (hasAllCompositeFields) {
        // Build star tree from mergedFieldProducerMap (raw doc values from merge)
        starTreesBuilder.build(metaOut, dataOut, fieldProducerMapForMerge, compositeDocValuesConsumer);
    }
}
```

The `mergedFieldProducerMap` is populated during `super.merge()` via `addSortedNumericField()` / `addSortedSetField()`. These doc values producers contain ALL documents being merged — including soft-deleted ones. Lucene's merge writes all doc values first, then marks deleted docs in the live docs bitset. The star tree builder doesn't know which docs are deleted.

For the **native star tree merge path** (where source segments have `CompositeIndexReader`), `buildDuringMerge()` merges existing star tree values which already exclude deleted docs. But the **fallback path** (building from raw doc values) has no live docs filtering.

### Why This Doesn't Happen for Native Star Tree Indices

Native star tree indices (created with `index.composite_index=true` from the start) always have star tree data in their segments. During merge, the source segments' `Composite912DocValuesReader` implements `CompositeIndexReader`, so `starTreeSubsPerField` is populated and `buildDuringMerge()` is used — which correctly merges existing star tree values (already excluding deleted docs).

The fallback path is only hit when source segments DON'T have `CompositeIndexReader` — which happens for retroactively upgraded segments where the codec was switched to `Composite912Codec` but the source segments in the merge still use the old codec's doc values reader.

Wait — actually, the upgraded segments DO have `Composite912Codec` and should have `Composite912DocValuesReader` which implements `CompositeIndexReader`. Let me re-examine...

### Deeper Analysis — CONFIRMED via Logs

From the test logs:
```
buildLiveDocsBitset: segment=_0 hardDel=0 softDel=9968 maxDoc=100000, liveBits.cardinality=90032
Phase 1 complete — upgraded: 1, skipped: 1 (tombstone _1 skipped)
Skipping codec switch for segment _0 — has doc values updates (docValuesGen=1)
segment=_0 path=DIRECT values=FOUND  ← upgrade query uses direct reader, returns 90032 ✓
segment=_2 path=NATIVE values=FOUND  ← merge query uses native path, returns 100000 ✗
```

**Confirmed**: After merge, the merged segment `_2` takes the NATIVE path (`Composite912DocValuesReader` as `CompositeIndexReader`). But it returns 100k docs instead of 90032.

**Why the fallback path is taken during merge**: Segment `_0` has `docValuesGen=1` so its codec was NOT switched (stays `Lucene104`). During merge, Lucene opens `_0` with `Lucene104Codec` which does NOT have `Composite912DocValuesReader`. So `mergeState.docValuesProducers[i]` is NOT a `CompositeIndexReader` → `starTreeSubsPerField` is empty → fallback path builds star tree from raw merged doc values.

**Why the merged star tree has 100k docs**: The `mergedFieldProducerMap` (populated during `super.merge()`) contains doc values for ALL documents being merged — including soft-deleted ones. Lucene's merge writes all doc values first, then records which docs are deleted. The star tree builder in the fallback path has no live docs filtering.

**Why the merged segment shows `docs=90032, deleted=19936`**: The merge combines `_0` (100000 maxDoc, 9968 soft-deleted) + `_1` (9968 maxDoc, 9968 soft-deleted). Lucene's merge produces a segment with some docs marked as deleted (the soft-deleted ones that weren't purged because `index.merge.policy` settings or merge behavior).

### Fix Needed

The fallback path in `Composite912DocValuesWriter.mergeStarTreeFields()` needs to filter soft-deleted docs before building the star tree.

**Approach 1 — `mergeState.liveDocs` bitset (FAILED)**: `mergeState.liveDocs` only reflects **hard deletes**, not soft deletes. For soft-deleted docs, `liveDocs.get(docId)` returns `true` — they appear "live" from Lucene's merge perspective. Confirmed via debug: `totalDeadInSource=0` even though 9968 docs are soft-deleted.

**Approach 2 — `SequentialLiveDocsFilteredDocValuesProducer` (FAILED)**: Since `buildMergedLiveDocsBitset()` returns a bitset with all docs marked live (because `mergeState.liveDocs` doesn't reflect soft deletes), the wrapper has no effect.

**Approach 3 — Adjusting `SegmentWriteState.maxDoc` (FAILED)**: The producers still iterate all docs regardless of maxDoc.

**Correct approach**: Read the `__soft_deletes` field from the merged doc values during `super.merge()`. Capture the `__soft_deletes` `NumericDocValues` producer in `addNumericField()` (it's a numeric field with value 1 for deleted docs). Then build the live docs bitset from that field before building the star tree.

Implementation:
1. In `addNumericField()`, check if `field.name.equals("__soft_deletes")` and capture the producer
2. After `super.merge()` completes, iterate the `__soft_deletes` producer to build a `FixedBitSet` of live docs (docs where `__soft_deletes` is absent or 0)
3. Wrap `fieldProducerMapForMerge` with `SequentialLiveDocsFilteredDocValuesProducer` using this bitset

Key insight from debug logging:
```
mergeState.liveDocs[0] = present but totalDeadInSource=0
→ liveDocs reflects hard deletes only, soft deletes pass through as "live"
→ Must read __soft_deletes field to identify soft-deleted docs in merged segment
```

### Impact

- Bug #7 (upgrade path): ✅ FIXED — `buildLiveDocsBitset()` correctly excludes deleted docs
- Bug #12 (merge fallback path): ✅ FIXED — captures `__soft_deletes` producer during `addNumericField()`, builds live docs bitset, wraps merge producers with `LiveDocsFilteredDocValuesProducer` in skip-only mode
- `SequentialLiveDocsFilteredDocValuesProducer` deleted — superseded by skip-only mode in `LiveDocsFilteredDocValuesProducer`


---

## Parallel Star Tree Build (Multithreading)

### Change

`StarTreeUpgradeService.buildStarTreeDataForSegments()` now builds star tree data for multiple segments in parallel using a thread pool. Each segment's build is independent — they read from different segment files and write to different output files.

### Implementation

```java
int parallelism = Math.min(eligibleSegments.size(), Runtime.getRuntime().availableProcessors());
ExecutorService executor = Executors.newFixedThreadPool(parallelism);
for (SegmentCommitInfo commitInfo : eligibleSegments) {
    futures.add(executor.submit(() -> {
        buildStarTreeData(directory, commitInfo, starTreeField, mapperService);
        upgradedSegmentNames.add(commitInfo.info.name);
    }));
}
executor.shutdown();
executor.awaitTermination(60, TimeUnit.MINUTES);
```

Falls back to sequential for single-segment or single-core cases.

### Performance Results

| Dataset | Segments | Sequential | Parallel | Speedup |
|---------|----------|-----------|----------|---------|
| 100k docs | 6 | ~5.4s | ~1.8s | 3x |
| 1M docs + 50k deletes | 5 | ~27s (est) | ~10s | ~2.7x |

---

## Delete Support: Verified at Scale

### Test Results (1M docs + 50k deletes)

Both code paths proven at 1M scale in a single test run:

- **7 segments with `docValuesGen=1`** → served via `StarTreeDirectReader` cache (direct reader path)
- **Remaining segments with `docValuesGen=-1`** → served via native Composite912Codec (codec switch path)
- **Aggregation: ALL VALUES MATCH (before == after)** ✅
- **`terminated_early: True`** — star tree active on all segments ✅
- **0 wrong values during concurrent reads** ✅
- **Upgrade time: 7.5s** (parallel build)

### Java Integration Test

`StarTreeUpgradeWithDocValuesGenIT.testUpgradeWithDocValuesGenNotMinusOne()`:
- Deterministically forces `docValuesGen != -1` via: ingest → flush → delete → flush
- Runs full upgrade API
- Asserts: upgrade succeeds, direct reader cache populated, star tree active, aggregation values match

### How Delete Support Works

1. `rewriteSegmentInfos()` checks `commitInfo.getDocValuesGen() != -1`
2. If true: skips codec switch, adds star tree files to segment file set, rewrites `.si`
3. `populateStarTreeDirectReaderCache()` creates `StarTreeDirectReader` for these segments
4. `StarTreeQueryHelper.getStarTreeValues()` Path 2 looks up direct reader cache by segment name
5. `StarTreeDirectReader` opens `.cid`/`.cim`/`.cidvd`/`.cidvm` directly from directory (bypasses codec)


---

## Recent Changes (Latest Session)

### Bug #12 Fix — Implemented and Verified

The merge fallback path now correctly excludes soft-deleted docs:

1. `Composite912DocValuesWriter.addNumericField()` captures `__soft_deletes` producer during `super.merge()`
2. `buildSoftDeleteLiveDocsBitset()` iterates the captured producer, builds `FixedBitSet` (1=live, 0=deleted)
3. Each merge producer wrapped with `LiveDocsFilteredDocValuesProducer(producer, liveBits)` — skip-only mode
4. Star tree builder sees only live docs via `nextDoc()` filtering

### Cleanup Applied

| Change | Description |
|--------|-------------|
| Removed `SequentialLiveDocsFilteredDocValuesProducer.java` | Dead code — superseded by skip-only mode in `LiveDocsFilteredDocValuesProducer` |
| Removed `buildMergedLiveDocsBitset()` from `Composite912DocValuesWriter` | Unused — only handled hard deletes which don't apply here |
| Removed `wrapWithLiveDocsFilter()` from `Composite912DocValuesWriter` | Unused — referenced the 3-arg constructor with maxDoc |
| Removed debug `logger.debug()` calls | Two debug log statements removed from merge path |
| Removed `Logger`/`LogManager` imports and field | No longer needed after debug removal |
| Removed `Bits` import | No longer used after removing `buildMergedLiveDocsBitset` |

### Direct Reader Cache Cleanup Fix

**Problem**: `performDirectReaderCleanup()` used `SegmentInfos.readLatestCommit()` but `afterMerge()` fires before the commit. Stale entries were never evicted.

**Fix**: Changed `directReaderMergeCleanupCallback` from `Runnable` to `Consumer<Set<String>>`. The `afterMerge(OnGoingMerge merge)` callback now passes the merged-away segment names directly (from `merge.getMergedSegments()`). `performDirectReaderCleanup(Set<String> mergedAwaySegments)` simply evicts those specific entries — no disk reads needed.

**Files changed**:
- `InternalEngine.java`: Field type `Runnable` → `Consumer<Set<String>>`, setter signature updated, `afterMerge` extracts segment names from `merge.getMergedSegments()`
- `IndexShard.java`: `performDirectReaderCleanup()` now accepts `Set<String>` parameter, iterates and evicts directly

### New Integration Tests

| Test | What It Verifies |
|------|-----------------|
| `StarTreeUpgradeForceMergeIT` | Mixed codec merge (Lucene912 + Composite912), soft-delete filtering in merged star tree, star tree files present, aggregation correctness, star tree active (`terminatedEarly`), direct reader cache cleanup after merge |
| `StarTreeUpgradeStalePerFieldIT` | Bug #10 regression — stale PerField attributes don't cause `NoSuchFileException` after merge, second-generation merge (merged + native segment) also works |

### Updated Bug Status

| # | Bug | Status |
|---|-----|--------|
| 10 | `NoSuchFileException: _2_Lucene90_0.dvm` | ✅ FIXED + IT (`StarTreeUpgradeStalePerFieldIT`) |
| 11 | "flushes are disabled - pending translog recovery" | ✅ FIXED + IT (`StarTreeUpgradeNodeRestartIT` covers indirectly) |
| 12 | Merged star tree includes soft-deleted docs | ✅ FIXED + IT (`StarTreeUpgradeForceMergeIT`) |
