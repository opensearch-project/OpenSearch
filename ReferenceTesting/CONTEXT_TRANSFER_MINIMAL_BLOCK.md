# Context Transfer: Star Tree Minimal Block Upgrade

## What Was Built

A "minimal block" star tree retroactive upgrade that keeps writes + reads available during the star tree build phase. Only ~100ms of write blocking during the commit phase.

## Architecture Summary

### Execution Flow (IndexShard.upgradeToStarTree)

```
Phase 1 (NO BLOCKING — writes + reads proceed normally):
  buildStarTreeDataForSegments() → builds .cid/.cim/.cidvd/.cidvm sidecar files
  Uses LiveDocsFilteredDocValuesProducer (remap mode) to filter soft-deleted docs

Phase 2 (~100ms BLOCK):
  blockOperations()
  flush(force=true)
  store().protectFilesFromDeletion(starTreeFiles)  ← prevents IndexFileDeleter cleanup
  close engine
  rewriteSegmentInfos() → adds star tree files to segment file sets, commits segments_N+1
  codecServiceOverride = composite codec  ← ensures future writes/merges use composite
  reopen engine (skipTranslogRecovery + refresh)
  store().unprotectFilesFromDeletion()
  unblockOperations()

Post-commit (NO BLOCKING):
  populateStarTreeDirectReaderCache()
  wire merge cleanup callback
```

### Key Components

| Component | Purpose |
|-----------|---------|
| `Store.StoreDirectory.protectedFiles` | Set of file names that `deleteFile()` silently skips — prevents IndexFileDeleter from cleaning star tree files during engine close |
| `codecServiceOverride` | Field on IndexShard, wired into `newEngineConfig()` — forces new engine to use Composite104Codec for merges/flushes |
| `StarTreeDirectReader` cache | ConcurrentHashMap on IndexShard — serves star tree data for upgraded segments via direct file reading |
| `StarTreeQueryHelper` dual-path | Checks native CompositeIndexReader first, falls back to DirectReader cache |
| `Composite912DocValuesWriter` merge fallback | Captures `__soft_deletes` eagerly in `addNumericField()`, filters with LiveDocsFilteredDocValuesProducer (skip-only mode) |
| `Composite912DocValuesFormat.perFieldSuffixedFileExists()` | Guards against stale PerField attributes in merged segments |

### Why codecServiceOverride Is Needed

`CodecService` is constructed ONCE at IndexShard creation time. It checks `mapperService.isCompositeIndexPresent()` at that moment. Since the shard existed BEFORE the mapping update, its `codecService` has `Lucene104Codec` permanently. `codecServiceOverride` is the only way to make the post-upgrade engine use `Composite104Codec`.

**CONFIRMED**: Without `codecServiceOverride`, post-Phase-2 segments show `codec=Lucene104`. With it, merged segments show `codec=Composite104Codec` in `segments_N`.

### Why ProtectedFileDirectory Is Needed

When IndexWriter closes, IndexFileDeleter garbage-collects unreferenced files. Star tree files built in Phase 1 are unreferenced until `rewriteSegmentInfos()` adds them to segment file sets. The protection prevents deletion during the brief window between engine close and rewriteSegmentInfos commit.

## Test Results (1M docs + 100k soft deletes)

| Metric | Result |
|--------|--------|
| Upgrade time | 5.2s (parallel build across 5 segments) |
| Writes during Phase 1 | 100/100 succeeded |
| Reads during Phase 1 | 10/10 succeeded |
| Post-upgrade aggregation | ✅ Exact match (keyword + numeric) |
| Force merge time | 18s |
| Post-merge codec | Composite104Codec (confirmed via segments_N) |
| Post-merge star tree active | terminated_early=True |
| Post-merge aggregation | ✅ Exact match |
| SortedSetDocValues (keyword fields) | ✅ No corruption (customer_gender, currency both correct) |

## Files Modified

| File | Changes |
|------|---------|
| `IndexShard.java` | `upgradeToStarTree()` with ProtectedFile + codecServiceOverride pattern, `populateStarTreeDirectReaderCache()`, `performDirectReaderCleanup()`, `starTreeDirectReaderCache` field |
| `Store.java` | `protectFilesFromDeletion()`, `unprotectFilesFromDeletion()`, `StoreDirectory.protectedFiles` set, `deleteFile()` override |
| `StarTreeUpgradeService.java` | `buildStarTreeDataForSegments()`, `rewriteSegmentInfos()` (no codec switch), `buildLiveDocsBitset()`, `cleanupStarTreeFiles()`, `getCandidateSegmentNames()` |
| `StarTreeDirectReader.java` | Standalone reader for .cid/.cim/.cidvd/.cidvm files |
| `LiveDocsFilteredDocValuesProducer.java` | Dual-mode: remap (Phase 1) + skip-only (merge fallback) |
| `Composite912DocValuesWriter.java` | Eagerly captures `__soft_deletes` bitset in `addNumericField()`, merge fallback path with LiveDocsFilteredDocValuesProducer |
| `Composite912DocValuesFormat.java` | `perFieldSuffixedFileExists()` guard for stale PerField attributes |
| `Composite912DocValuesReader.java` | `starTreeFilesExist` graceful fallback for missing files |
| `StarTreeQueryHelper.java` | Dual-path: native CompositeIndexReader → DirectReader cache fallback |
| `InternalEngine.java` | `directReaderMergeCleanupCallback` field + `afterMerge()` dispatch |
| `TransportStarTreeUpgradeAction.java` | Mapping update → shard broadcast, `shardOperation()` calls `upgradeToStarTree()` |

## Known Issues / TODO

1. **Docs written during Phase 1 don't have star tree** — they use `Lucene104Codec` (engine hasn't switched yet). They get star tree after force merge.
2. **Debug logging** — `System.out.println` and `[STAR_TREE_MERGE_DEBUG]` / `[CODEC_CHECK]` logging still in code. Remove before PR.
3. **Phase 2 write blocking** — writes queue for ~100ms during engine close/reopen. Not rejected, just delayed.

## Task: Write Architecture Quip

Create `/Users/dwivashu/OpenSearch/ReferenceTesting/QUIP_MINIMAL_BLOCK_ARCHITECTURE.md` following the format of `QUIP_HYBRID_UPGRADE_ARCHITECTURE.md`. Cover:
- Complete execution flow (upgrade + merge + query paths)
- All file changes with method signatures
- How ProtectedFileDirectory works
- How codecServiceOverride is wired
- Test results at 1M scale
- Comparison with hybrid approach (this one keeps writes available during build)
