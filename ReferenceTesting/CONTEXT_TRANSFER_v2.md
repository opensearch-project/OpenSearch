# Context Transfer — Per-Segment Star Tree Upgrade

## What Was Built
A feature to retroactively add star tree indexes to existing OpenSearch indices without force merge. The upgrade builds star tree data per-segment directly from doc values, rewrites SegmentInfos and .si files to declare Composite912Codec, and reopens the engine.

## Branch
`per-segment-star-tree-upgrade` on `https://github.com/Ashu-tosh27/OpenSearch`

## Files Modified (5 Java files)

### 1. StarTreeUpgradeService.java
Path: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeUpgradeService.java`
- Phase 1: `buildStarTreeData()` — opens DirectoryReader, finds SegmentReader, reads doc values, builds star tree via StarTreesBuilder, writes .cid/.cim/.cidvd/.cidvm
- Phase 2: `rewriteSegmentInfos()` — creates new SegmentCommitInfo with Composite912Codec, adds star tree files to file set, rewrites .si file, commits segments_N+1
- Key: adds `_doc_count` with EmptyDocValuesProducer to fieldProducerMap (implicit metric)

### 2. IndexShard.java
Path: `server/src/main/java/org/opensearch/index/shard/IndexShard.java`
- `upgradeToStarTree(StarTreeField)` — flush → block ops → close engine → StarTreeUpgradeService.upgradeSegments() → create new engine with codecServiceOverride → refresh → unblock
- `starTreeUpgradeInProgress` AtomicBoolean for concurrent upgrade guard
- `codecServiceOverride` set before new engine creation so post-upgrade flushes use Composite912Codec

### 3. TransportStarTreeUpgradeAction.java
Path: `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java`
- `shardOperation()` passes `request.getStarTreeField()` to `indexShard.upgradeToStarTree()`
- Added MapperService propagation guard: checks `getCompositeFieldTypes().isEmpty()` before proceeding
- NOTE: currently broadcasts to ALL shards (primary + replica) — should be filtered to primary only

### 4. Composite912DocValuesFormat.java
Path: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesFormat.java`
- `fieldsProducer()` detects PerField attributes in FieldInfos (`PerFieldDocValuesFormat.format` + `suffix`)
- For upgraded segments: creates SegmentReadState with correct suffix (e.g., "Lucene90_0") so delegate opens right files
- For native segments: uses original direct path (no change)
- `getPerFieldDocValuesSuffix()` helper reads format name + suffix from any FieldInfo

### 5. Composite912DocValuesReader.java
Path: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesReader.java`
- Added `starTreeDir` fallback: tries `readState.directory` first, falls back to `readState.segmentInfo.dir`
- Handles compound file segments where star tree files are outside .cfs
- Also uses `starTreeDir` for the compositeDocValuesProducer SegmentReadState

## Key Issues Encountered & Resolved

1. **_doc_count missing** — StarTreesBuilder expects _doc_count in fieldProducerMap. Fixed by adding EmptyDocValuesProducer.
2. **Compound file (.cfs)** — Star tree files written outside .cfs. Fixed with starTreeDir fallback in reader.
3. **PerField doc values naming** — Upgraded segments use `_0_Lucene90_0.dvd` not `_0.dvd`. Fixed with PerField suffix detection in Composite912DocValuesFormat.
4. **resetEngineToGlobalCheckpoint deletes files** — Its flush creates new commit without star tree files. Fixed by closing engine entirely instead of read-only swap.
5. **.si file not rewritten** — Lucene reads codec from .si, not segments_N. Fixed by deleting old .si and writing new one with Composite912Codec.
6. **Engine not warmed up** — Assertion error on scheduled refresh. Fixed by calling newEngine.refresh() after creation.
7. **Post-upgrade ingest** — New segments didn't use Composite912Codec. Fixed by setting codecServiceOverride before engine creation.

## Test Results
- 5 docs, 100k docs, 1M docs (1 shard): all pass
- 3 shards with 100k docs: all 3 shards upgraded successfully
- Post-upgrade ingest: new segments build star tree natively (inside .cfs)
- Cold query: 40-100ms → 2-4ms on 1M docs
- All existing star tree tests pass (no regressions)

## Pending Items
- `append_only` setting: force-set to `true` during upgrade to prevent updates/deletes that would corrupt star tree data. Bypasses the Final restriction.
- Replica shards: upgrade broadcasts to all shards including replicas. Should be filtered to primary only.
- Spec files (design.md, requirements.md, tasks.md) reference old read-only engine approach — actual implementation uses close-engine approach.
- Spec files should be updated to match the actual implementation.


## Additional Change: append_only enforcement
- `TransportStarTreeUpgradeAction.applyStarTreeMapping()` now force-sets `index.append_only.enabled=true` in the index settings during the cluster state update
- This bypasses the Final restriction (the setting can't be changed via normal settings API)
- After upgrade, updates and deletes are blocked on the index — same as natively created star tree indexes
- The `allowCompositeFieldWithoutSettings` flag still skips the `index.composite_index` check in `ObjectMapper.parseCompositeField()`, but the `append_only` check passes because we set it to `true` in the settings
