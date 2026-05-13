# Star Tree Per-Segment Upgrade — Architecture (v2)

## Overview

Retroactively builds star tree indexes on existing OpenSearch indices without force merge. Works with any index, any supported dimension/metric types. The upgrade builds star tree data per-segment directly from doc values, rewrites SegmentInfos and .si files to declare Composite912Codec, and reopens the engine.

## How It Works (3-Phase Flow)

```
Client Request
    │
    ▼
Phase 1: Mapping + Settings Update (cluster-wide, once)
    │  - Adds star tree field to index mapping under "composite" section
    │  - Force-sets index.append_only.enabled=true (bypasses Final restriction)
    │  - Bypasses index.composite_index setting check (it's Final)
    │  - Bypasses CompositeIndexValidator restriction
    │  - Validates dimensions/metrics exist and have compatible types
    │  - Propagates to all nodes via cluster state
    │
    ▼
Phase 2: Per-Segment Star Tree Build (per-shard)
    │  - Flush all in-memory data
    │  - Block operations on the shard
    │  - Close the engine (releases IndexWriter and all file handles)
    │  - For each segment:
    │    ├─ Skip if already Composite912Codec
    │    ├─ Open DirectoryReader → find SegmentReader
    │    ├─ Read doc values for dimensions + metrics
    │    ├─ Build star tree via StarTreesBuilder.build()
    │    └─ Write .cid, .cim, .cidvd, .cidvm files to directory
    │
    ▼
Phase 3: SegmentInfos + .si Rewrite (per-shard)
    │  - For each upgraded segment:
    │    ├─ Create new SegmentInfo with Composite912Codec
    │    ├─ Add star tree files to file set
    │    ├─ Delete old .si file, write new .si with Composite912Codec
    │    └─ Preserve all SegmentCommitInfo metadata
    │  - Commit segments_N+1 atomically
    │  - Create new engine from segments_N+1
    │  - Warm up searcher, unblock operations
    │
    ▼
Response: { "_shards": { "total": N, "successful": N, "failed": 0 } }
```

## Key Design Decisions

### Why not force merge?
Force merge rewrites ALL segment data (stored fields, postings, norms, point values, doc values) into one new segment. For 1M docs with ~20 fields, the star tree build is ~20-30% of the total time — the other 70-80% is Lucene rewriting data unrelated to star tree. Per-segment build only reads the dimension/metric doc values columns and writes star tree files.

### Why close the engine instead of read-only engine?
`resetEngineToGlobalCheckpoint()` creates a new `IndexWriter` that flushes a new commit — this commit doesn't include the star tree files, so the `IndexWriter` deletes them as orphans. Closing the engine entirely means no `IndexWriter` exists during the upgrade, so our files survive.

### Why rewrite .si files?
`SegmentInfos.commit()` writes `segments_N+1` with the new codec name, but the `.si` file on disk still declares the original codec. When the `IndexWriter` opens, it reads the codec from `.si`, not from `segments_N`. We must rewrite `.si` to declare `Composite912Codec`.

### How does Composite912DocValuesFormat handle upgraded segments?
Upgraded segments have doc values files with per-field naming (`_0_Lucene90_0.dvd`) because they were originally written by `Lucene912Codec`'s `PerFieldDocValuesFormat`. `Composite912DocValuesFormat.fieldsProducer()` detects `PerFieldDocValuesFormat.format` and `PerFieldDocValuesFormat.suffix` attributes in `FieldInfos` and creates a `SegmentReadState` with the correct suffix so the delegate opens the right files.

### How does Composite912DocValuesReader find star tree files in compound segments?
For compound file segments (`.cfs`), star tree files are outside the compound file. `Composite912DocValuesReader` tries `readState.directory` first (CompoundDirectory), and if `.cim` is not found, falls back to `readState.segmentInfo.dir` (parent directory).

## Files Modified

| File | Change |
|------|--------|
| `StarTreeUpgradeService.java` | New: per-segment build + SegmentInfos/.si rewrite |
| `IndexShard.java` | New: `upgradeToStarTree(StarTreeField)` with close-engine approach + codecServiceOverride for post-upgrade ingest |
| `TransportStarTreeUpgradeAction.java` | Updated: passes StarTreeField, mapping propagation guard |
| `Composite912DocValuesFormat.java` | New: PerField suffix detection for upgraded segments |
| `Composite912DocValuesReader.java` | New: fallback to segmentInfo.dir for compound file star tree files |

## Post-Upgrade Behavior

After the upgrade completes:
- **Existing segments**: Star tree files are separate files outside `.cfs` (built by Phase 2). `Composite912DocValuesFormat` detects PerField attributes and reads doc values with the correct suffix. `Composite912DocValuesReader` falls back to `segmentInfo.dir` for star tree files in compound segments.
- **New segments** (flushes/merges after upgrade): Star tree data is built natively by `Composite912DocValuesWriter` during flush and packed inside `.cfs`. The engine uses `Composite912Codec` via `codecServiceOverride` set during the upgrade.
- **Queries**: Star tree acceleration works across both segment types (`terminated_early=true`).

## Test Results

| Test | Docs | Shards | Upgrade Time | Query Before | Query After |
|------|------|--------|-------------|-------------|-------------|
| Basic | 5 | 1 | <1s | N/A | 2ms (star tree) |
| 100k | 100,000 | 1 | ~5s | 14ms | 1-2ms |
| 1M | 1,000,000 | 1 | ~27s | 40-100ms | 2-4ms |
| 100k + post-ingest | 105,000 | 1 | ~5s | N/A | 3ms (both segment types) |
| 3-shard | 100,000 | 3 | ~5s | N/A | 2-3ms |
| 3-shard + post-ingest | 110,000 | 3 | ~5s | N/A | 3ms (all shards, both types) |
