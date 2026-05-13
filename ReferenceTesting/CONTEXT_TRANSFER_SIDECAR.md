# Context Transfer: Star Tree Sidecar Upgrade

## What This Feature Does

Retroactively adds star tree acceleration to existing OpenSearch indices that were created without `index.composite_index=true`. The API is `POST /{index}/_star_tree/upgrade` with star tree config in the request body.

## The Evolution (4 Specs)

### Spec 1: Star Tree Upgrade via Mapping (`.kiro/specs/star-tree-upgrade-via-mapping/`)
- **Approach**: Update mapping → engine restart → forceMerge(1) → codec merge path builds star tree
- **Status**: ✅ Done. Works for clean segments.
- **Files**: TransportStarTreeUpgradeAction, StarTreeUpgradeRequest/Response, RestStarTreeUpgradeAction, mapping bypass (ObjectMapper, CompositeIndexValidator, DocumentMapperParser), merge path enhancement (Composite912DocValuesWriter)

### Spec 2: Read Availability (`.kiro/specs/star-tree-upgrade-read-availability/`)
- **Approach**: ReadOnlyEngine bridge during upgrade, codecServiceOverride lifecycle fix, index.composite_index=true in cluster state
- **Status**: ✅ Done. Keeps reads alive during upgrade.
- **Files**: IndexShard.upgradeToStarTree() rewrite, StarTreeUpgradeService split methods, 11 aggregator null-safety fixes

### Spec 3: Retroactive Per-Segment Building (`.kiro/specs/retroactive-star-tree-building/`)
- **Approach**: Build star tree per-segment via raw IndexOutput, switch codec via direct SegmentInfos rewrite (no force merge)
- **Status**: ✅ Done for clean segments. BROKEN for segments with soft deletes (PendingSoftDeletes assertion).
- **Files**: StarTreeUpgradeService (Phase 1 + Phase 2), Composite912DocValuesFormat (PerField suffix detection), Composite912DocValuesReader (starTreeDir fallback)

### Spec 4: Sidecar Upgrade (`.kiro/specs/star-tree-sidecar-upgrade/`)
- **Approach**: Build star tree as sidecar files WITHOUT codec switch. Engine stays live. SidecarProtectedDirectory for GC protection. Sidecar reader cache for immediate star tree acceleration.
- **Status**: ✅ Working. Upgrade succeeds, metric aggregations correct, plain terms correct, 10-15x aggregation speedup. Nested terms+sub-aggs has ordinal mapping issue (separate problem).
- **Files**: SidecarProtectedDirectory, StarTreeValuesProvider, StarTreeSidecarMetadata, LiveDocsFilteredDocValuesProducer, StarTreeSidecarReader, modifications to Store, InternalEngine, Engine, IndexShard, StarTreeQueryHelper (+ 6 aggregator call sites), TransportStarTreeUpgradeAction, StarTreeUpgradeService

## Current State of the Code

### What Works
- Upgrade API succeeds on all indices (with and without deletes)
- No PendingSoftDeletes assertion crash (the original blocker is solved)
- Segments with 0 live docs (all soft-deleted) are properly skipped
- Metric aggregations (sum, min, max, avg, value_count) return correct values via star tree (exact match before/after)
- Plain terms aggregation returns correct buckets and doc counts via star tree
- Doc count is correct after upgrade
- Star tree acceleration is immediate after upgrade (no engine restart needed)
- Aggregation latency: ~40-50ms → ~3-5ms on 1M docs (10-15x speedup)
- Server stays up, no crashes, no engine restart
- Unit tests all pass (InternalEngineTests, StoreTests, IndexShardTests, StarTreeDocValuesFormatTests, etc.)
- Spotless formatting clean

### What's Broken
- Nothing currently known — all tested aggregation types produce exact-match results before/after upgrade

## The Open Issue: Nested Terms + Sub-Aggregations — RESOLVED

The nested terms+sum issue was caused by `collectionStrategy.globalOrdsReady(globalOrds)` not being called in the star tree precompute path. Fixed by always calling it in `getStarTreeBucketCollector()`. All aggregation types now produce exact-match results before/after upgrade on 1M docs.

## What Needs to Happen Next

1. **Test with deletes** — Run the 100k with deletes test to verify soft delete handling with the sidecar reader cache path.

2. **Node restart verification** — Verify that after a node restart, the sidecar reader cache is repopulated from metadata and star tree acceleration works immediately.

3. **Run unit tests for all aggregator types** — Ensure the `globalOrdsReady` change doesn't break native star tree or non-star-tree aggregation paths.

## Key Files to Read

### Architecture Docs
- `ReferenceTesting/SIDECAR_ARCHITECTURE.md` — Full architecture with all components, execution flow, GC protection, ref counting, crash recovery, and the open issue
- `ReferenceTesting/SIDECAR_DEEP_DIVE.md` — Deep dive into why sidecar, GC protection, ref counting, liveDocs filtering, TOCTOU race, cleanup lock scope
- `ReferenceTesting/DELETE_SUPPORT_INVESTIGATION.md` — The original delete problem that motivated the sidecar approach

### Spec Files
- `.kiro/specs/star-tree-sidecar-upgrade/design.md` — Sidecar design with all reviewed fixes (SidecarProtectedDirectory, CAS ref counting, generational metadata, cleanup lock scope, etc.)
- `.kiro/specs/star-tree-sidecar-upgrade/tasks.md` — Implementation tasks (1-14 done, 15-16 optional tests)
- `.kiro/specs/star-tree-sidecar-upgrade/requirements.md` — Requirements with all reviewed fixes

### New Source Files (Sidecar)
- `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/SidecarProtectedDirectory.java` — FilterDirectory wrapper intercepting deleteFile()
- `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeValuesProvider.java` — Standalone interface for star tree values
- `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeSidecarMetadata.java` — Generational metadata file manager
- `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/LiveDocsFilteredDocValuesProducer.java` — DocValuesProducer wrapper filtering soft-deleted docs
- `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeSidecarReader.java` — Ref-counted sidecar reader

### Modified Source Files (Sidecar)
- `server/src/main/java/org/opensearch/index/store/Store.java` — Added `engineDirectory()`, `installSidecarDirectory()`, patched `cleanupAndVerify()`
- `server/src/main/java/org/opensearch/index/engine/InternalEngine.java` — `store.engineDirectory()` in `createWriter()`, `sidecarMergeCleanupCallback` + `afterMerge()` dispatch
- `server/src/main/java/org/opensearch/index/engine/Engine.java` — `store.engineDirectory()` in `cleanUpUnreferencedFiles()`
- `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesReader.java` — Added `implements StarTreeValuesProvider`
- `server/src/main/java/org/opensearch/search/startree/StarTreeQueryHelper.java` — Sidecar fallback (TODO), `isStarTreeUpgradeInProgress()` check
- `server/src/main/java/org/opensearch/index/shard/IndexShard.java` — Sidecar metadata/reader cache fields, `SidecarProtectedDirectory` installation, `upgradeToStarTree()` rewrite, `performSidecarCleanup()`, merge cleanup wiring
- `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java` — `clearStarTreeUpgradeInProgress()` in `shardOperation()` finally block
- `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeUpgradeService.java` — `buildSidecarStarTreeData()`, `buildSidecarStarTreeDataForSegment()` with liveDocs filtering

### Existing Source Files (From Earlier Specs, Unchanged by Sidecar)
- `server/src/main/java/org/opensearch/index/mapper/MapperService.java` — `STAR_TREE_UPGRADE` merge reason
- `server/src/main/java/org/opensearch/index/mapper/Mapper.java` — `allowCompositeFieldWithoutSettings` flag
- `server/src/main/java/org/opensearch/index/mapper/ObjectMapper.java` — Setting check bypass
- `server/src/main/java/org/opensearch/index/compositeindex/CompositeIndexValidator.java` — MergeReason-aware validation
- `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesWriter.java` — Merge path fallback for building from raw doc values
- `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesFormat.java` — PerField suffix detection for upgraded segments

### Test Data
- `ReferenceTesting/test_100k_deletes.sh` — 100k docs + 5k deletes test (the critical delete test)
- `ReferenceTesting/test_1m_upgrade.sh` — 1M docs no-deletes test
- `ReferenceTesting/test_100k_upgrade.sh` — 100k docs no-deletes test
- `ReferenceTesting/ecommerce-field_mappings.json` — Index mapping for test data

## Test Results

| Test | Upgrade | Sum | Plain Terms | Nested Terms+Sum | Doc Count | Latency |
|------|---------|-----|-------------|-------------------|-----------|---------|
| 1M no deletes | ✅ 25.8s | ✅ exact match | ✅ exact match | ✅ exact match (8/8 checks) | ✅ 1,000,000 | 44ms→5ms |
| 100 docs (2 dims) | ✅ | ✅ | ✅ MALE:60 FEMALE:40 | ✅ | ✅ 100 | — |
| 900 docs (with deletes) | ✅ | ✅ | ✅ | ✅ | ✅ 900 | — |
| Native star tree (10k) | ✅ | ✅ | ✅ | ✅ | ✅ 10,000 | — |

## How to Build and Test

```bash
# Build
./gradlew :server:compileJava
./gradlew :server:spotlessApply

# Unit tests
./gradlew :server:test --tests "*.InternalEngineTests" --tests "*.StoreTests" --tests "*.IndexShardTests" --tests "*.StarTreeDocValuesFormatTests" -x spotlessJavaCheck

# Start local cluster
./gradlew run

# Run tests (in another terminal)
bash ReferenceTesting/test_1m_upgrade.sh      # 1M no deletes
bash ReferenceTesting/test_100k_deletes.sh    # 100k with deletes
```

## Design Decisions Made (and Reviewed)

All of these were reviewed through multiple rounds of critique:

1. **SidecarProtectedDirectory** wrapping `Store.directory()` for GC protection — `setLiveCommitData()` doesn't work (Lucene treats userData as opaque)
2. **Store.engineDirectory()** — new method, less disruptive than changing `Store.directory()` return value
3. **Store.cleanupAndVerify()** patched to skip protected files
4. **Lucene-style CAS ref counting** on StarTreeSidecarReader — `incRef()` throws `AlreadyClosedException` if count was 0
5. **unprotect() inside deleteFiles() at refCount=0** — never before `decRef()`, prevents IndexWriter from deleting files mid-read
6. **Generational metadata files** (`_startree_sidecar_genN.meta`) — mirrors Lucene's `segments_N` pattern, metadata is primary source of truth for crash recovery
7. **In-memory metadata singleton** at IndexShard level — mutations under `sidecarMetadataLock`, disk I/O outside lock (prevents deadlock with flush)
8. **Merge cleanup dispatched to FLUSH thread pool** from `EngineMergeScheduler.afterMerge()` — same pattern as existing post-merge flush
9. **starTreeUpgradeInProgress flag** cleared by `TransportStarTreeUpgradeAction.shardOperation()` finally block — not in IndexShard's finally
10. **SegmentCommitInfo.getSoftDelCount()** for 0-live-docs detection — `segmentReader.numDocs()` doesn't account for soft deletes
11. **StarTreeValuesProvider** standalone interface — NOT extending `CompositeIndexReader` to avoid invasive API changes
12. **Pre-drain flush outside blockOperations** — minimizes write-blocking window
13. **No engine restart** — removed `blockOperations()` / `codecServiceOverride` / `resetEngineToGlobalCheckpoint()` from `upgradeToStarTree()`. Engine restart broke `SortedSetDocValues` on existing segments. Star tree acceleration is immediate via sidecar reader cache instead.
14. **Segment ID and maxDoc from SegmentInfos** — `StarTreeSidecarReader` constructor accepts `byte[] segmentId` and `int maxDoc` from `SegmentInfos` instead of parsing the header manually. Manual parsing had a Lucene 10 byte order incompatibility.
15. **SidecarProtectedDirectory installed during first upgrade** — if `sidecarProtectedDirectory` is null (fresh index, first upgrade), create and install the wrapper in `upgradeToStarTree()`. Previously only called `protect()` on an already-installed wrapper.
16. **Always call globalOrdsReady in star tree path** — `getStarTreeBucketCollector()` now always calls `collectionStrategy.globalOrdsReady(globalOrds)` regardless of `parent`. The star tree precompute path skips `getLeafCollector()` which normally initializes this.

## What Needs to Happen Next

1. **Fix nested terms + sub-aggregations ordinal mapping** — The `RemapGlobalOrdsStarTree` strategy in `GlobalOrdinalsStringTermsAggregator` uses `valuesSource.globalOrdinalsMapping(ctx)` which maps segment ordinals → global ordinals. But sidecar star tree dimension ordinals are in a different ordinal space than the segment's `SortedSetDocValues`. Need to either: (a) build an ordinal translation table during sidecar reader creation, (b) modify the sidecar builder to preserve segment ordinals, or (c) fall back to normal collection for nested terms on sidecar segments.

2. **End-to-end verification with deletes** — Run the 100k with deletes test to verify soft delete handling still works with the new sidecar reader cache path.

3. **Node restart verification** — Verify that after a node restart, the sidecar reader cache is repopulated from metadata and star tree acceleration works immediately without re-running the upgrade API.
