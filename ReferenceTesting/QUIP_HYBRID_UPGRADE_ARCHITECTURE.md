# Star Tree Hybrid Upgrade — Full Architecture & Method Reference

## Overview

This document traces every file, class, method, and execution flow involved in the hybrid star tree upgrade feature. It covers:
1. **Working architecture** — how the upgrade, merge, and query paths work end-to-end
2. **Problems encountered** — what broke and why
3. **Fixes applied** — what we changed in each file
4. **Testing** — how we validate correctness using `_delete_by_query`

The hybrid approach handles two segment types:
- **Clean segments** (no soft deletes, `docValuesGen == -1`): Full codec switch to `Composite912Codec`
- **Soft-delete segments** (`docValuesGen != -1`): Star tree built with live docs filtering, codec NOT switched. Background merges converge to native `Composite104Codec`.

---

## File 1: TransportStarTreeUpgradeAction.java

**Path**: `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java`

**Extends**: `TransportBroadcastByNodeAction<StarTreeUpgradeRequest, StarTreeUpgradeResponse, ShardStarTreeUpgradeResult>`

### What TransportBroadcastByNodeAction Does

Groups target shards by node, sends one transport request per node, executes `shardOperation()` on each local shard sequentially, collects results, and calls `newResponse()` to build the final response.

### Methods

#### `doExecute(Task, StarTreeUpgradeRequest, ActionListener<StarTreeUpgradeResponse>)`

**Called by**: Transport framework when the action is invoked

**What it does**:
1. Resolves concrete index names via `indexNameExpressionResolver.concreteIndexNames()`
2. Checks idempotency: if ALL indices already have star tree config → skips mapping update
3. Validates no index has a DIFFERENT star tree config
4. Calls `submitMappingUpdate()` to add star tree field to cluster state
5. On mapping ack → calls `super.doExecute()` which triggers the broadcast

#### `shardOperation(StarTreeUpgradeRequest, ShardRouting)` — MODIFIED

**Called by**: Broadcast framework on each node, once per local shard

**What it does**:
1. Gets `IndexShard` from `IndicesService`
2. Checks mapping propagation: throws if composite field types empty
3. Calls `indexShard.upgradeToStarTree(request.getStarTreeField())`
4. **NEW**: Calls `indexShard.flush(new FlushRequest().force(false).waitIfOngoing(true))` — post-upgrade flush to ensure clean engine state (Bug #11 fix)

**Returns**: `ShardStarTreeUpgradeResult(shardId, isPrimary)`

#### `submitMappingUpdate(StarTreeUpgradeRequest, String[], ActionListener<Boolean>)`

Submits cluster state update task with `Priority.HIGH`. Uses `StarTreeUpgradeMappingExecutor` to apply the mapping change on the master node.

#### `buildCompleteMappingSource(StarTreeUpgradeRequest, IndexMetadata)` — Static

Builds complete mapping JSON including existing properties + new composite star tree field. Required because `StarTreeMapper.Builder.getDimension()` validates dimension fields by looking them up in the current mapping source.

### Inner Class: StarTreeUpgradeMappingExecutor

**What it does** (per index):
1. Creates `MapperService`, sets `allowCompositeFieldWithoutSettings = true`
2. Merges existing mappings with `MergeReason.MAPPING_RECOVERY`
3. Merges star tree mapping with `MergeReason.STAR_TREE_UPGRADE` (bypasses "no new composite fields" restriction)
4. Validates via `CompositeIndexValidator`
5. Force-sets `index.append_only.enabled = true`
6. Updates `IndexMetadata` with new mapping + settings

---

## File 2: IndexShard.java

**Path**: `server/src/main/java/org/opensearch/index/shard/IndexShard.java`

**Extends**: `AbstractIndexShardComponent` implements `IndicesClusterStateService.Shard`

### Key Fields

| Field | Type | Purpose |
|-------|------|---------|
| `currentEngineReference` | `AtomicReference<Engine>` | Current engine (swapped during upgrade) |
| `engineMutex` | `Object` | Synchronizes engine swaps |
| `indexShardOperationPermits` | `IndexShardOperationPermits` | Blocks writes during upgrade |
| `codecServiceOverride` | `volatile CodecService` | **NEW** — injects Composite912Codec into new engine |
| `starTreeUpgradeInProgress` | `AtomicBoolean` | **NEW** — prevents concurrent upgrades |
| `starTreeDirectReaderCache` | `ConcurrentHashMap<String, StarTreeDirectReader>` | **NEW** — cache for soft-delete segment readers |

### Method: `upgradeToStarTree(StarTreeField)` — NEW, MODIFIED

**Called by**: `TransportStarTreeUpgradeAction.shardOperation()`

**Full execution**:
1. `verifyActive()` — checks shard state is STARTED
2. `starTreeUpgradeInProgress.compareAndSet(false, true)` — atomic guard
3. `flush(force=true)` — first flush, commits in-memory buffer
4. `blockOperations(30 min, lambda)` — blocks all writes. Lambda:
   - `flush(waitIfOngoing=true)` — second flush for stragglers
   - Capture `SeqNoStats` / `TranslogStats` from live engine
   - **Swap 1**: InternalEngine → ReadOnlyEngine (reads continue during upgrade)
   - `codecServiceOverride = engineConfigFactory.newDefaultCodecService(...)` — creates codec with Composite912
   - **Phase 1**: `StarTreeUpgradeService.buildStarTreeDataForSegments(directory, starTreeField, mapperService)`
   - **Phase 2**: `StarTreeUpgradeService.rewriteSegmentInfos(directory, upgradedSegments)`
   - **Swap 2**: ReadOnlyEngine → InternalEngine (from upgraded segments_N+1)
   - **NEW**: `newEngine.translogManager().skipTranslogRecovery()` — clears pending recovery state (Bug #11 fix)
   - `newEngine.refresh("star-tree-upgrade")` — warms searcher
   - `populateStarTreeDirectReaderCache()` — caches readers for soft-delete segments
5. `starTreeUpgradeInProgress.set(false)`

**Returns**: `int` — number of segments upgraded

### Why `skipTranslogRecovery()` Is Needed (Bug #11)

`InternalTranslogManager` constructor sets `pendingTranslogRecovery = true`. This flag blocks ALL flushes via `ensureCanFlush()`. Normally cleared by `recoverFromTranslog()` during shard recovery. But `upgradeToStarTree()` creates the engine outside the recovery flow — so the flag was never cleared. `skipTranslogRecovery()` sets it to `false`, enabling flushes.

---

## File 3: StarTreeUpgradeService.java

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeUpgradeService.java`

**Status**: NEW FILE — utility class with static methods

### Method: `buildStarTreeDataForSegments(Directory, StarTreeField, MapperService)`

**Phase 1** — builds star tree files for each eligible segment:

For each `SegmentCommitInfo`:
1. Skip if already `Composite912Codec`
2. Skip if 0 live docs (tombstone segment)
3. `buildLiveDocsBitset(segmentReader, commitInfo)` — builds live docs from hard deletes + `__soft_deletes`
4. If liveDocs != null: wrap `DocValuesProducer` in `LiveDocsFilteredDocValuesProducer` (remap mode)
5. Build `fieldProducerMap` (dimensions + metrics + `_doc_count`)
6. Create `SegmentWriteState` with `numLiveDocs` (not maxDoc)
7. Open `IndexOutput` for `.cid` and `.cim` files
8. Create `DocValuesConsumer` for `.cidvd` and `.cidvm` files
9. `StarTreesBuilder.build(metaOut, dataOut, fieldProducerMap, consumer)` — builds tree
10. Write EOF marker + CodecUtil footer

### Method: `buildLiveDocsBitset(SegmentReader, SegmentCommitInfo)`

Builds a `FixedBitSet` of live docs by:
1. Checking `commitInfo.getDelCount()` + `commitInfo.getSoftDelCount()` — returns null if both 0
2. Starting with all bits set
3. Applying hard deletes from `segmentReader.getLiveDocs()` (`.liv` file)
4. Reading `__soft_deletes` from `segmentReader.getNumericDocValues("__soft_deletes")` — clears bits for docs with value=1

**Why `segmentReader.getNumericDocValues()` instead of `getDocValuesReader()`**: The base `DocValuesProducer` (from `.cfs`) doesn't have `__soft_deletes` — it's in generation-based update files (`_0_2_Lucene90_0.dvd`). `getNumericDocValues()` goes through `SegmentDocValuesProducer` which routes to the correct file.

### Method: `rewriteSegmentInfos(Directory, Set<String>)`

**Phase 2** — rewrites `segments_N` with codec changes:

For each upgraded segment:
- **If `docValuesGen != -1`** (soft deletes): Add star tree files to file set, rewrite `.si` with expanded file set, but keep original codec (`Lucene104`)
- **If `docValuesGen == -1`** (clean): Switch codec to `Composite912Codec`, add star tree files, rewrite `.si`, copy `fieldInfosFiles` + `docValuesUpdatesFiles`

Commits new `segments_N+1` atomically.

---

## File 4: Composite912DocValuesFormat.java — MODIFIED

**Path**: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesFormat.java`

**Extends**: `DocValuesFormat`

### Method: `fieldsProducer(SegmentReadState)` — Modified

**Before**: Unconditionally used `PerFieldDocValuesFormat` attributes to construct file suffix.

**After** (Bug #10 fix):
```java
String perFieldSuffix = getPerFieldDocValuesSuffix(state.fieldInfos);
if (perFieldSuffix != null && perFieldSuffixedFileExists(state, perFieldSuffix)) {
    // Upgraded segment: per-field suffix exists on disk
    regularProducer = delegate.fieldsProducer(suffixedState);
} else {
    // Native/merged segment: empty suffix
    regularProducer = delegate.fieldsProducer(state);
}
return new Composite912DocValuesReader(regularProducer, state);
```

**Why**: Merged segments write doc values with empty suffix (`_2.dvm`) but inherit stale `PerFieldDocValuesFormat` attributes from source segments. Without the file-existence check, the reader crashes with `NoSuchFileException`.

---

## File 5: Composite912DocValuesWriter.java — MODIFIED

**Path**: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesWriter.java`

**Extends**: `DocValuesConsumer`

### What It Does

Writes star tree data during flush and merge. During flush, builds star tree from segment doc values. During merge, either merges existing star tree values (`buildDuringMerge`) or builds from raw merged doc values (fallback path).

### Merge Fallback Path (When Source Segments Don't Have CompositeIndexReader)

This path is hit when source segments have `Lucene104` codec (soft-delete segments where codec wasn't switched).

**Before**: Built star tree from `mergedFieldProducerMap` without filtering → included soft-deleted docs.

**After** (Bug #12 fix):
1. Capture `__soft_deletes` producer in `addNumericField()` during `super.merge()`
2. `buildSoftDeleteLiveDocsBitset()` — iterates captured producer, builds `FixedBitSet`
3. Wrap each merge producer with `LiveDocsFilteredDocValuesProducer` in skip-only mode
4. Star tree builder sees only live docs

### Method: `buildSoftDeleteLiveDocsBitset()` — NEW

```java
private FixedBitSet buildSoftDeleteLiveDocsBitset() throws IOException {
    if (softDeletesProducer == null) return null;
    int mergedMaxDoc = this.mergeState.get().segmentInfo.maxDoc();
    FixedBitSet liveBits = new FixedBitSet(mergedMaxDoc);
    liveBits.set(0, mergedMaxDoc);
    NumericDocValues softDelDV = softDeletesProducer.getNumeric(softDeletesFieldInfo);
    int doc;
    while ((doc = softDelDV.nextDoc()) != NO_MORE_DOCS) {
        if (softDelDV.longValue() == 1) liveBits.clear(doc);
    }
    if (liveBits.cardinality() == mergedMaxDoc) return null;
    return liveBits;
}
```

### Key Insight: Why `mergeState.liveDocs` Doesn't Work

`mergeState.liveDocs[i]` only reflects **hard deletes**. Soft-deleted docs appear "live". Confirmed via debug logging: `totalDeadInSource=0` even with 19936 soft-deleted docs. The `__soft_deletes` field captured during `addNumericField()` is the only source of truth.

---

## File 6: LiveDocsFilteredDocValuesProducer.java — MODIFIED

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/LiveDocsFilteredDocValuesProducer.java`

**Extends**: `DocValuesProducer`

### Two Modes

| Mode | Constructor | Used By | How It Works |
|------|------------|---------|-------------|
| **Remap** | `(delegate, liveDocs, maxDoc)` | Upgrade path (Phase 1) | Builds `remappedToOriginal[]`, uses `advanceExact()` on delegate |
| **Skip-only** | `(delegate, liveDocs)` | Merge fallback path | `remappedToOriginal = null`, uses `nextDoc()` on delegate, skips deleted |

**Why two modes**: The upgrade path has full `DocValuesProducer` instances that support `advanceExact()`. The merge path has `DocValuesConsumer$2` anonymous classes that only support sequential `nextDoc()`.

### Skip-Only Mode Implementation

```java
if (remappedToOriginal == null) {
    return new SortedNumericDocValues() {
        @Override
        public int nextDoc() throws IOException {
            int doc;
            while ((doc = inner.nextDoc()) != NO_MORE_DOCS) {
                if (liveDocs.get(doc)) return doc; // live → return
            }
            return NO_MORE_DOCS;
        }
        @Override
        public boolean advanceExact(int target) throws IOException {
            throw new UnsupportedOperationException("merge path: sequential only");
        }
        // ... nextValue(), docValueCount(), docID(), cost() delegate to inner
    };
}
```

---

## File 7: Composite912DocValuesReader.java

**Path**: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesReader.java`

**Extends**: `DocValuesProducer`, implements `CompositeIndexReader`

### What It Does

Reads star tree data (`.cid`, `.cim`, `.cidvd`, `.cidvm`) from a segment. Provides `StarTreeValues` to the query engine for star tree acceleration.

### Key Behavior: `starTreeDir` Fallback

For upgraded segments, star tree files are outside `.cfs` (written by Phase 1). The reader tries `readState.directory` first, falls back to `readState.segmentInfo.dir` if `.cim` not found.

### Key Behavior: `starTreeSuffix = ""`

Star tree files always use empty suffix regardless of `readState.segmentSuffix`. This is because Phase 1 writes them with empty suffix, even for segments with `docValuesGen != -1` where Lucene sets the suffix to the generation value.

---

## Codec Hierarchy and Naming

| Codec | Wraps | Used For | docValuesFormat() |
|-------|-------|----------|-------------------|
| `Composite912Codec` | `Lucene912Codec` | Retroactively upgraded segments | `Composite912DocValuesFormat` |
| `Composite104Codec` | `Lucene104Codec` | Natively written segments (flush/merge) | `Composite912DocValuesFormat` |
| `Lucene104Codec` | — | Standard OpenSearch segments | `Lucene90DocValuesFormat` via PerField |

Both composite codecs use the same `Composite912DocValuesFormat`. The "912" vs "104" is about the base codec wrapper.

### Where Codec Name Is Stored

- **`segments_N`**: Per-segment codec name (e.g., `Composite104Codec`). Used by Lucene's `Codec.forName()` via SPI.
- **`.si` file**: Segment metadata (doc count, files, diagnostics). Does NOT store codec name directly.

### Segment Lifecycle

```
Before upgrade:     segments_N → codec = "Lucene104Codec"
After upgrade:      segments_N+1 → codec = "Composite912Codec" (clean) or "Lucene104Codec" (soft-del)
After force merge:  segments_N+2 → codec = "Composite104Codec" (same as native star tree)
```

---

## Complete Execution Flow

### Upgrade Path

```
POST /my-index/_star_tree/upgrade { star_tree: {...} }
  │
  ▼
TransportStarTreeUpgradeAction.doExecute()
  ├── submitMappingUpdate() → cluster state update (adds composite field to mapping)
  ├── super.doExecute() → broadcast to all shards
  │
  ▼ Per shard:
shardOperation()
  ├── indexShard.upgradeToStarTree(starTreeField)
  │    ├── flush(force=true)
  │    ├── blockOperations(30 min)
  │    │    ├── flush(waitIfOngoing=true)
  │    │    ├── Capture SeqNoStats/TranslogStats
  │    │    ├── Swap 1: InternalEngine → ReadOnlyEngine (reads continue)
  │    │    │
  │    │    ├── Phase 1: buildStarTreeDataForSegments()
  │    │    │    For each segment:
  │    │    │    ├── Skip if Composite912Codec or 0 live docs
  │    │    │    ├── buildLiveDocsBitset() → FixedBitSet from __soft_deletes
  │    │    │    ├── Wrap DocValuesProducer in LiveDocsFilteredDocValuesProducer (remap mode)
  │    │    │    ├── StarTreesBuilder.build() → writes .cid/.cim/.cidvd/.cidvm
  │    │    │    └── Star tree contains ONLY live docs ✓
  │    │    │
  │    │    ├── Phase 2: rewriteSegmentInfos()
  │    │    │    ├── docValuesGen != -1: add files to set, keep Lucene104 codec
  │    │    │    └── docValuesGen == -1: switch to Composite912Codec, rewrite .si
  │    │    │
  │    │    ├── Swap 2: ReadOnlyEngine → InternalEngine
  │    │    ├── skipTranslogRecovery() ← enables flushes
  │    │    ├── Refresh
  │    │    └── populateStarTreeDirectReaderCache()
  │    │
  │    └── Returns: segments upgraded count
  │
  └── indexShard.flush() ← safety net
```

### Force Merge Path

```
POST /my-index/_forcemerge?max_num_segments=1
  │
  ▼
IndexWriter.forceMerge() → selects all segments for merge
  │
  ▼
SegmentMerger.mergeDocValues()
  │
  ▼
Composite912DocValuesWriter.merge(mergeState)
  ├── super.merge(mergeState) — writes all doc values (including soft-deleted)
  │    ├── addNumericField("__soft_deletes", producer) → CAPTURED
  │    ├── addSortedNumericField("taxful_total_price", producer) → mergedFieldProducerMap
  │    └── addSortedSetField("customer_gender", producer) → mergedFieldProducerMap
  │
  ├── mergeStarTreeFields(mergeState)
  │    ├── Check source segments for CompositeIndexReader
  │    │    ├── Source has Composite912Codec → starTreeSubsPerField populated → buildDuringMerge()
  │    │    └── Source has Lucene104Codec → NOT found → fallback path ↓
  │    │
  │    ├── [Fallback path — source segments don't have star tree reader]
  │    ├── hasAllCompositeFields check → true (all dims/metrics in mergedFieldProducerMap)
  │    ├── buildSoftDeleteLiveDocsBitset()
  │    │    ├── Iterate __soft_deletes producer: docs with value=1 → clear bit
  │    │    └── Returns FixedBitSet (cardinality = live doc count)
  │    ├── Wrap each producer: LiveDocsFilteredDocValuesProducer(producer, liveBits)
  │    │    └── Skip-only mode: nextDoc() skips deleted docs
  │    └── StarTreesBuilder.build() → star tree from live docs only ✓
  │
  └── Merged segment written with Composite104Codec + star tree
```

### Query Path (Post-Merge)

```
Search query → IndexShard.acquireSearcher()
  │
  ▼
For each segment in reader:
  ├── Lucene reads segments_N → codec = "Composite104Codec"
  ├── ServiceLoader → Composite104Codec class
  ├── Composite104Codec.docValuesFormat() → Composite912DocValuesFormat
  ├── Composite912DocValuesFormat.fieldsProducer(readState)
  │    ├── getPerFieldDocValuesSuffix(fieldInfos)
  │    │    └── Merged segment: attributes found but perFieldSuffixedFileExists() → false
  │    │        → use empty suffix (Bug #10 fix)
  │    └── Returns Composite912DocValuesReader
  │         ├── Opens .cid/.cim from directory (empty suffix)
  │         ├── Opens .cidvd/.cidvm via compositeDocValuesProducer
  │         └── Provides StarTreeValues to query engine
  │
  ▼
StarTreeQueryHelper.getStarTreeValues()
  ├── reader.getDocValuesReader() instanceof CompositeIndexReader → YES (Composite104Codec)
  └── Returns StarTreeValues → star tree acceleration (terminated_early=true)
```

---

## Testing with `_delete_by_query`

### Why `_delete_by_query`

`_delete_by_query` applies soft-delete doc values updates directly to existing segments, creating `docValuesGen != -1`. This:
- Prevents the codec switch (Fix 5 guard in `rewriteSegmentInfos`)
- Forces the merge into the fallback path (source segments don't have `CompositeIndexReader`)
- Exercises the full `buildLiveDocsBitset()` → `LiveDocsFilteredDocValuesProducer` path

### Test Script: `test_1m_soft_delete.sh`

```bash
# 1. Create index (1 shard, no auto-merge)
# 2. Ingest 1M docs with explicit _id
# 3. Flush → creates data segments
# 4. Delete ~100k docs via _delete_by_query (total_quantity <= 2)
# 5. Flush → commits soft deletes to segments
# 6. Verify segments have deleted_docs > 0
# 7. Record BASELINE aggregation
# 8. UPGRADE (timed) → star tree built with live docs filtering
# 9. Verify aggregation matches baseline
# 10. FORCE MERGE (timed) → fallback path with __soft_deletes filtering
# 11. Verify single segment, aggregation matches baseline
# 12. Verify terminated_early=true (star tree active)
```

### Results (1M docs, ~100k soft deletes, 6 segments)

| Metric | Value |
|--------|-------|
| Upgrade time | 23.3s |
| Force merge time | 18.2s |
| Live docs after delete | 899,796 |
| Aggregation after upgrade | ✅ Exact match (899,796) |
| Aggregation after merge | ✅ Exact match (899,796) |
| Merged segment | `_6: docs=899796, deleted=0` |
| Star tree active | `terminated_early: true` |
| Merged codec | `Composite104Codec` |

---

## Summary of All Bugs Fixed

| # | Bug | Root Cause | Fix | File |
|---|-----|-----------|-----|------|
| 10 | `NoSuchFileException: _2_Lucene90_0.dvm` | Stale PerField attributes in merged FieldInfos | `perFieldSuffixedFileExists()` check | `Composite912DocValuesFormat.java` |
| 11 | "flushes are disabled - pending translog recovery" | `pendingTranslogRecovery=true` never cleared | `skipTranslogRecovery()` after engine creation | `IndexShard.java` |
| 12 | Merged star tree includes soft-deleted docs | `mergeState.liveDocs` only has hard deletes | Capture `__soft_deletes`, build bitset, skip-only filter | `Composite912DocValuesWriter.java` + `LiveDocsFilteredDocValuesProducer.java` |

---

## Parallel Star Tree Build

### Change

`StarTreeUpgradeService.buildStarTreeDataForSegments()` now builds star tree data for multiple segments concurrently using a fixed thread pool. Each segment's build is independent — they read from different segment files and write to different output files (named by segment).

### Implementation

```java
int parallelism = Math.min(eligibleSegments.size(), Runtime.getRuntime().availableProcessors());
ExecutorService executor = Executors.newFixedThreadPool(parallelism);
for (SegmentCommitInfo commitInfo : eligibleSegments) {
    futures.add(executor.submit(() -> buildStarTreeData(...)));
}
executor.shutdown();
executor.awaitTermination(60, TimeUnit.MINUTES);
```

Falls back to sequential for single-segment or single-core cases.

### Performance

| Dataset | Segments | Sequential | Parallel | Speedup |
|---------|----------|-----------|----------|---------|
| 100k docs | 6 | ~5.4s | ~1.8s | 3x |
| 1M docs + 50k deletes | 5-7 | ~27s | ~8-10s | ~3x |

---

## Delete Support Verification (End-to-End)

### Problem

When segments have `docValuesGen != -1` (soft deletes applied as in-place doc values updates), switching the codec to `Composite912Codec` causes a cascade of errors culminating in `AssertionError: softDeleteCount doesn't match`. Full error chain documented in `DELETE_SUPPORT_INVESTIGATION.md`.

### Solution

The hybrid approach handles both segment types:

| Segment Type | Codec Switch | Star Tree Read Path |
|-------------|-------------|-------------------|
| `docValuesGen == -1` | ✅ Switch to Composite912Codec | Native `Composite912DocValuesReader` |
| `docValuesGen != -1` | ❌ Skip | `StarTreeDirectReader` via cache |

### Verification at 1M Scale

Test: `test_1m_full_verification.sh` — 1M docs + 50k deletes

Logs confirmed 7 segments with `docValuesGen=1` served via direct reader cache:
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
- Upgrade: 7.5s (parallel), successful
- Aggregation: ALL VALUES MATCH (before == after)
- `terminated_early: True` — star tree active on all segments
- 0 wrong values during concurrent reads
- Doc count: 950,000 (correct)

### Java Integration Test

`StarTreeUpgradeWithDocValuesGenIT.testUpgradeWithDocValuesGenNotMinusOne()`:
- Deterministically forces `docValuesGen != -1` via: ingest → flush → delete → flush
- Asserts: upgrade succeeds, direct reader cache populated, star tree active, aggregation exact match

### Reads During Upgrade with Deletes

`test_reads_correctness_with_deletes.sh` — 100k docs + 5k deletes, concurrent reads:
- 14/16 reads returned exact correct values
- 2/16 reads returned empty (engine swap window)
- 0 reads returned wrong values
- Deleted docs never included in any response


---

## Recent Changes (Latest Session)

### Bug #12 — FIXED and Verified

Implementation in `Composite912DocValuesWriter`:
- Captures `__soft_deletes` producer in `addNumericField()` during `super.merge()`
- `buildSoftDeleteLiveDocsBitset()` builds `FixedBitSet` from captured producer
- Wraps merge producers with `LiveDocsFilteredDocValuesProducer(producer, liveBits)` — skip-only mode
- `SequentialLiveDocsFilteredDocValuesProducer.java` DELETED (dead code)
- Debug logging removed from `Composite912DocValuesWriter`
- Unused methods removed: `buildMergedLiveDocsBitset()`, `wrapWithLiveDocsFilter()`

### Direct Reader Cache Cleanup — Fixed

**Problem**: `performDirectReaderCleanup()` used `SegmentInfos.readLatestCommit()` but `afterMerge()` fires before commit → stale entries never evicted.

**Fix**: `directReaderMergeCleanupCallback` changed from `Runnable` to `Consumer<Set<String>>`. `afterMerge()` passes merged-away segment names from `merge.getMergedSegments()`. Cleanup evicts those entries directly — no disk reads.

### New Integration Tests Added

| Test Class | Scenario |
|-----------|----------|
| `StarTreeUpgradeForceMergeIT` | Mixed codec merge (Lucene912 + Composite912), soft-delete filtering, star tree files, aggregation correctness, `terminatedEarly`, cache cleanup |
| `StarTreeUpgradeStalePerFieldIT` | Bug #10 regression — stale PerField attributes after merge, second-generation merge |

### Final Bug Status

| # | Bug | Root Cause | Fix | File | Test |
|---|-----|-----------|-----|------|------|
| 10 | `NoSuchFileException: _2_Lucene90_0.dvm` | Stale PerField attributes in merged FieldInfos | `perFieldSuffixedFileExists()` check | `Composite912DocValuesFormat.java` | `StarTreeUpgradeStalePerFieldIT` |
| 11 | "flushes are disabled - pending translog recovery" | `pendingTranslogRecovery=true` never cleared | `skipTranslogRecovery()` after engine creation | `IndexShard.java` | `StarTreeUpgradeNodeRestartIT` |
| 12 | Merged star tree includes soft-deleted docs | `mergeState.liveDocs` only has hard deletes | Capture `__soft_deletes`, build bitset, skip-only filter | `Composite912DocValuesWriter.java` + `LiveDocsFilteredDocValuesProducer.java` | `StarTreeUpgradeForceMergeIT` |
