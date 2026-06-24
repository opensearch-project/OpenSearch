# Investigation: Force Merge Returns Empty Buckets After Upgrade

## DEFINITIVE ROOT CAUSE (CONFIRMED)

**The post-Phase-2 engine does NOT use Composite104Codec.** Verified directly:

```
[CODEC_CHECK] segment=_0 codec=Lucene104 maxDoc=100
[CODEC_CHECK] segment=_1 codec=Lucene104 maxDoc=10
```

All segments after the upgrade + engine reopen + flush still use `Lucene104` codec. The `CodecService` created for the reopened engine does NOT return `Composite104Codec` despite `mapperService.isCompositeIndexPresent()` being true.

**Consequence**: 
- Force merge output uses `Lucene104Codec` → `Lucene90DocValuesFormat` → no `Composite912DocValuesWriter` → no star tree built during merge
- The merged segment has NO star tree data at all
- But `terminated_early: true` appears in queries — this means the query path is hitting the DirectReader cache for the OLD segments, but after merge those segments are gone and the NEW merged segment has no star tree → empty results

## Fix Required

Bring back `codecServiceOverride` — set it in Phase 2 before engine reopen so the new engine uses `Composite104Codec`. This was the purpose of the original Phase 0 that was removed.

The fix in `IndexShard.upgradeToStarTree()`, inside Phase 2 before engine reopen:
```java
codecServiceOverride = engineConfigFactory.newDefaultCodecService(indexSettings, mapperService, logger);
```

This ensures the new engine uses the composite codec for all future writes AND merges.

### Finding 1: `Composite912DocValuesWriter.merge()` is NEVER called during force merge

Added `java.nio.file.Files.writeString("/tmp/merge_debug.log", ...)` at the very first line of `merge(MergeState)`. The file does not exist after running the full test (upgrade + force merge). This means:

- The merged segment's `DocValuesConsumer` is NOT a `Composite912DocValuesWriter`
- The `Composite912DocValuesFormat.fieldsConsumer()` was never invoked for the merged segment
- The merged segment uses a DIFFERENT codec/format than `Composite104Codec`

### Finding 2: The ordering hypothesis (Possibility 1) is ruled out

The code order is `super.merge()` FIRST, then `mergeCompositeFields()` SECOND. This is correct. But since `merge()` is never called at all, the ordering is irrelevant.

### Root Cause

The merged segment does not use `Composite104Codec`. This means the engine reopened in Phase 2 does NOT have the composite codec as its default.

**Why**: Looking at `CodecService` constructor — it checks `mapperService.isCompositeIndexPresent()` at CodecService construction time. `CodecService` is constructed inside `newEngineConfig()` → `engineConfigFactory.newDefaultCodecService(...)`. This is called from `indexerFactory.createIndexer(newEngineConfig(replicationTracker))` during Phase 2 engine reopen.

At that point `mapperService.isCompositeIndexPresent()` SHOULD return true (mapping was updated before shard operation). Unless there's a timing issue or the mapping hasn't propagated to this shard's MapperService yet.

Actually — `TransportStarTreeUpgradeAction.shardOperation()` verifies `indexShard.mapperService().getCompositeFieldTypes().isEmpty() == false` before calling `upgradeToStarTree()`. So the mapping IS available. The engine reopen in Phase 2 should create a `CodecService` with composite codec.

The issue might be that `newEngineConfig()` caches or reuses the old CodecService. Let me check if `codecServiceOverride` is interfering — no, we removed that usage.

**Alternative explanation**: The `CodecService` IS created with composite codec, but `PerFieldDocValuesFormat` assigns `Composite912DocValuesFormat` only to specific field names (star tree fields), not as the segment-level format. The MERGE goes through the segment-level `DocValuesFormat` which might be `Lucene90DocValuesFormat` for the regular fields, and `Composite912DocValuesFormat` is only used as a sub-format for star tree specific doc values.

In `Composite104Codec`, `docValuesFormat()` returns `Composite912DocValuesFormat` which wraps `Lucene90DocValuesFormat`. The `fieldsConsumer()` creates `Composite912DocValuesWriter(delegate.fieldsConsumer(), ...)` where delegate is `Lucene90DocValuesFormat`. But in `PerFieldDocValuesFormat`, each field is routed to its own format. The MERGE might be going through a `PerFieldDocValuesConsumer` that routes to both `Lucene90` and `Composite912` consumers.

**Actual issue**: In `Composite104Codec`, the `docValuesFormat()` is `Composite912DocValuesFormat` BUT it extends `DocValuesFormat` with `super(delegate.getName())` — which means it registers under the delegate's name (e.g., `"Lucene90"`). In `PerFieldDocValuesFormat`, each field looks up its format by name from attributes. If the star tree fields don't have the right `PerFieldDocValuesFormat.format` attribute, they go through the default format which IS `Composite912DocValuesFormat` but might not trigger `merge()` in the expected way.

## Next Steps

1. Verify that the engine after Phase 2 actually uses `Composite104Codec` by checking segment info of any segment flushed after Phase 2
2. Check if `Composite912DocValuesFormat.fieldsConsumer()` is called during the merge (add file logging there)
3. If `fieldsConsumer()` IS called but `merge()` is not — check if the merge path bypasses DocValuesConsumer entirely for some reason

## Possible Fix (from your earlier suggestion - Possibility 2 fix)

If the root cause is that `__soft_deletes` never routes through our writer, read soft deletes directly from `mergeState` inside `mergeStarTreeFields()`:

```java
private FixedBitSet buildSoftDeleteLiveDocsBitset(MergeState mergeState) throws IOException {
    int mergedMaxDoc = mergeState.segmentInfo.maxDoc();
    FixedBitSet liveBits = new FixedBitSet(mergedMaxDoc);
    liveBits.set(0, mergedMaxDoc);
    boolean anySoftDeletes = false;
    for (int i = 0; i < mergeState.fieldInfos.length; i++) {
        FieldInfo softDelInfo = mergeState.fieldInfos[i].fieldInfo(Lucene.SOFT_DELETES_FIELD);
        if (softDelInfo == null) continue;
        DocValuesProducer producer = mergeState.docValuesProducers[i];
        if (producer == null) continue;
        NumericDocValues softDelDV = producer.getNumeric(softDelInfo);
        if (softDelDV == null) continue;
        DocMap docMap = mergeState.leafDocMaps[i];
        int doc;
        while ((doc = softDelDV.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (softDelDV.longValue() == 1) {
                int mergedDoc = docMap.get(doc);
                if (mergedDoc != -1) {
                    liveBits.clear(mergedDoc);
                    anySoftDeletes = true;
                }
            }
        }
    }
    return anySoftDeletes ? liveBits : null;
}
```

However, this fix only matters if `mergeStarTreeFields()` IS being called. The primary issue is that `merge()` itself is never invoked on our writer.


## CONFIRMED Findings

### 1. The merge fallback path is NOT being used

`System.out.println("[STAR_TREE_MERGE_DEBUG] addNumericField called: ...")` placed at the top of `addNumericField()` with `mergeState.get() != null` check produces **ZERO output** during force merge. This means:

- `Composite912DocValuesWriter.addNumericField()` is never called during the merge, OR
- `mergeState.get()` is null during the merge (unlikely since merge() sets it)

**Most likely**: The source segments after Phase 2 are opened with `Composite104Codec` (because the engine reopened with composite codec via the mapping update). `Composite912DocValuesReader` finds the `.cim` files (added to file sets by `rewriteSegmentInfos()`), so it reports having star tree data. All segments have `CompositeIndexReader` → `allSegmentsHaveStarTree = true` → **`buildDuringMerge()` path is taken** (not the fallback path).

### 2. `buildDuringMerge()` doesn't handle the case correctly

`buildDuringMerge()` directly merges star tree values from source segments. The star tree data in the source segments was built by Phase 1 with soft-delete filtering (correct — 899,796 live docs). But `buildDuringMerge()` may not be aggregating them correctly, or the star tree values read via `Composite912DocValuesReader` for these segments are somehow empty/wrong.

### 3. The query after merge returns `"buckets":[]` with `terminated_early: true`

This means the star tree IS present in the merged segment but contains zero data — empty star tree with no entries. Either:
- `buildDuringMerge()` produced an empty star tree
- Or the star tree values read from source segments during merge were empty

## Hypothesis to verify

The star tree files for the upgraded segments are read by `Composite912DocValuesReader` during merge. But `Composite912DocValuesReader` might be reading them incorrectly for segments whose codec was NOT switched (they still have `Lucene104Codec` in `.si`). The engine's new codec (`Composite104Codec`) is used as the default, but each segment's codec is loaded from its `.si` file. Since `.si` still says `Lucene104Codec`, `Composite912DocValuesReader` would NOT be used for these segments.

Wait — after `rewriteSegmentInfos()`, the `.si` files still say `Lucene104Codec`. So during merge, Lucene opens each source segment with `Lucene104Codec` (not `Composite104Codec`). The `DocValuesReader` for these segments is Lucene's standard `Lucene90DocValuesProducer` — which does NOT implement `CompositeIndexReader`.

This means `allSegmentsHaveStarTree` should be FALSE. The merge SHOULD take the fallback path. But our debug logging shows the fallback path is never hit.

## Possible explanations for no debug output

1. **The force merge produces a DIFFERENT `Composite912DocValuesWriter` instance** — one where `System.out.println` was not compiled in (stale class file)
2. **The merge is using a different codec entirely** — the merged output segment uses `Composite104Codec` but the source segments use `Lucene104Codec`. The `Composite912DocValuesWriter` for the OUTPUT segment is what we instrumented. During `merge()`, `super.merge()` calls `addNumericField` on the delegate... actually no, `super.merge(mergeState)` handles the merge internally.
3. **The star tree merge path (`buildDuringMerge`) runs without calling `addNumericField`** — it reads star tree values directly from source segments without going through the numeric field path

Actually — I think the issue is #3. `mergeStarTreeFields()` is separate from `super.merge()`. It accesses source segment readers directly via `mergeState.docValuesProducers[i]`. If those producers implement `CompositeIndexReader` (they do — because after engine reopen with composite codec + star tree files in file set, `Composite912DocValuesReader` wraps the producer), then `buildDuringMerge()` is called.

But `buildDuringMerge()` doesn't call `addNumericField`. It directly writes star tree data by merging the star tree values from multiple source segments. So our logging would never fire.

## Root Cause Hypothesis

After Phase 2, the engine uses `Composite104Codec`. When force merge runs:
1. Source segments are opened by IndexWriter — their codec is still `Lucene104Codec` (from `.si`)
2. BUT the `Composite912DocValuesFormat.fieldsProducer()` is what reads doc values (because the engine's PerField routing sends doc values to `Composite912DocValuesFormat`)
3. `Composite912DocValuesReader` finds `.cim` files → star tree data exists → reports as `CompositeIndexReader`
4. `mergeStarTreeFields()` finds `CompositeIndexReader` on all sources → `allSegmentsHaveStarTree = true`
5. `buildDuringMerge()` is called

In `buildDuringMerge()`, it merges star tree values from the source `StarTreeValues`. But here's the problem: the star tree was built for 899,796 live docs (Phase 1 correctly filtered soft deletes). The merged segment's maxDoc is 1,014,968 (all docs including soft-deleted). The star tree builder during merge expects doc IDs in [0, mergedMaxDoc) but the input star tree has doc IDs in [0, numLiveDocs). There's a mismatch that could produce wrong/empty results.

Actually — `buildDuringMerge()` should work correctly because it reads existing star tree NODES and METRICS (pre-aggregated), not individual doc values. It doesn't care about doc IDs.

## Most likely: `buildDuringMerge()` reads empty StarTreeValues

The `Composite912DocValuesReader` for these segments tries to read star tree via the hardcoded empty suffix. The segments have their `.cim` file in the raw directory (outside `.cfs`). The reader checks `readState.directory` (which for compound segments might be the compound directory) then falls back to `readState.segmentInfo.dir`. If the fallback path correctly opens the files, values should be non-empty.

## Next Steps

1. Add logging inside `buildDuringMerge()` to see how many star tree values it receives from source segments
2. Check if `Composite912DocValuesReader.getCompositeIndexValues()` returns non-empty `StarTreeValues` for these segments during merge
3. Verify that `starTreeSubsPerField` map is populated with actual values during `mergeStarTreeFields()`

## Possible Solutions

If the root cause is that `buildDuringMerge()` receives empty star tree values:
- Fix: After Phase 2, the upgraded segments' star tree data should be read via `StarTreeDirectReader` (which works — we verified in the upgrade test). During merge, use `StarTreeDirectReader` instead of relying on `Composite912DocValuesReader`.

If the root cause is a doc ID mismatch during merge:
- Fix: This would be a deeper bug in `buildDuringMerge()` that needs the star tree merge logic fixed.

If the root cause is that `Composite912DocValuesReader` can't find/read the files correctly during merge:
- Fix: Ensure the directory resolution in `Composite912DocValuesReader` handles compound-file segments with external star tree files correctly during the merge reader path.
