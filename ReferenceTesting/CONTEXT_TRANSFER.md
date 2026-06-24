# Context Transfer: Star Tree Upgrade via Mapping

## What This Feature Does

Adds `POST /{index}/_star_tree/upgrade` API that retroactively builds star tree indexes on existing OpenSearch indices without reindexing. User provides dimensions and metrics in the request body. The API updates the mapping, restarts the engine with the composite codec, and force merges to build star tree data from raw doc values.

## Current Status

Feature is **working end-to-end**. Tested with ecommerce dataset (4675 docs) and 100K generated docs. All existing star tree tests pass. The star tree data is built correctly and aggregation queries use it.

## Critical Files to Read First

1. `ReferenceTesting/DEBUGGING_LOG.md` — All 6 bugs found and fixed during testing
2. `ReferenceTesting/DESIGN.md` — Full architecture with diagrams
3. `ReferenceTesting/FILE_CHANGES.md` — Per-file breakdown of all changes
4. `.kiro/specs/star-tree-upgrade-via-mapping/design.md` — Original design spec

## The 6 New Files

All in `server/src/main/java/org/opensearch/action/admin/indices/startree/`:
- `StarTreeUpgradeAction.java` — Action type (trivial)
- `StarTreeUpgradeRequest.java` — Request with star tree config parsing
- `StarTreeUpgradeResponse.java` — Response (thin BroadcastResponse wrapper)
- `ShardStarTreeUpgradeResult.java` — Per-shard result (Writeable)
- `TransportStarTreeUpgradeAction.java` — **Main orchestrator** (most complex)
- `RestStarTreeUpgradeAction.java` — REST handler (trivial)

## The 8 Modified Files — WHY Each Change Was Made

### 1. `MapperService.java` — Added `STAR_TREE_UPGRADE` to MergeReason enum
**Why:** We need a way to signal "this is a star tree upgrade" through the mapping merge pipeline. The MergeReason is checked by CompositeIndexValidator and could be checked by ObjectMapper to decide whether to bypass setting checks. Without this, there's no way to distinguish an upgrade merge from a normal mapping update.

### 2. `Mapper.java` — Added `allowCompositeFieldWithoutSettings` flag to ParserContext
**Why:** `ObjectMapper.parseCompositeField()` checks `index.composite_index` setting (which is Final — can't be changed). We can't pass MergeReason through the parser because ParserContext doesn't carry it. So we added a boolean flag that the transport action sets before parsing. This is the bridge between "the transport action knows this is an upgrade" and "the parser needs to skip the setting check."

### 3. `ObjectMapper.java` — Wrapped setting checks with the bypass flag
**Why:** The two checks (`IS_COMPOSITE_INDEX_SETTING` and `INDEX_APPEND_ONLY_ENABLED_SETTING`) in `parseCompositeField()` are the gates that prevent adding star tree fields to existing indices. We wrap them with `if (parserContext.isAllowCompositeFieldWithoutSettings() == false)` so they're skipped during upgrade. The rest of the parsing (field validation, type checking) still runs.

### 4. `DocumentMapperParser.java` — Added setter for the bypass flag
**Why:** The transport action needs to set the bypass flag on the ParserContext, but ParserContext is created inside DocumentMapperParser. So we added `setAllowCompositeFieldWithoutSettings()` to DocumentMapperParser, which propagates the flag to every ParserContext it creates. This is how the transport action's intent reaches the parser.

### 5. `CompositeIndexValidator.java` — Added 5-arg validate() with MergeReason
**Why:** The 4-arg `validate()` blocks adding new composite fields during mapping updates ("Composite fields must be specified during index creation"). For the upgrade path, we need to bypass this check while still running `StarTreeValidator.validate()` (which checks dims/metrics against existing fields). The new overload accepts MergeReason and skips the restriction for `STAR_TREE_UPGRADE`.

### 6. `Composite912DocValuesWriter.java` — Added merge fallback path
**Why:** This is the core of the feature. The existing `mergeStarTreeFields()` only merges existing star tree data from source segments. Pre-upgrade segments have no star tree data, so the merge produces nothing. The fallback detects this case (starTreeSubsPerField empty, compositeMappedFieldTypes non-empty, all composite fields present in mergedFieldProducerMap) and calls `StarTreesBuilder.build()` with the raw doc values — the same code path used during flush. This is what actually builds the star tree data.

**Key subtleties in this file:**
- `mergedFieldProducerMap` captures doc values producers during `super.merge()` via overridden `addNumericField/addSortedNumericField/addSortedSetField`
- `_doc_count` must be skipped in the `hasAllCompositeFields` check because it's a virtual field not present in real doc values
- `buildFieldProducerMapFromMergeState()` fills in empty producers for missing fields (like `_doc_count`) using the existing `addDocValuesForEmptyField()` pattern

### 7. `IndexShard.java` — Added `upgradeToStarTree()` and `codecServiceOverride`
**Why:** This is the per-shard upgrade method. Three critical problems were solved here:

**Problem 1: CodecService is cached and final.** The `codecService` field is `final`, created at shard init before the mapping update. It doesn't include the composite codec. We can't modify it. Solution: added a volatile `codecServiceOverride` field. `newEngineConfig()` checks the override first. The upgrade method creates a fresh CodecService (which sees the star tree field) and sets it as override, then clears it after the upgrade.

**Problem 2: Engine restart needed for codec selection.** `CodecService` is constructed once per engine. We use `resetEngineToGlobalCheckpoint()` (existing method used for shard failover) to close and reopen the engine. The new engine gets the overridden CodecService with the composite codec.

**Problem 3: forceMerge(1) is a no-op with 1 segment.** After engine restart, if the translog was empty (data fully committed), there's only 1 segment. Lucene's `forceMerge(1)` silently does nothing. Solution: write a no-op (`engine.noOp()`) which adds a tombstone document to the IndexWriter, then flush to create a 2nd segment. Now `forceMerge(1)` has 2 segments to merge and the composite codec's merge path triggers.

### 8. `ActionModule.java` — Registered action and REST handler
**Why:** Standard OpenSearch registration. Without this, the API endpoint doesn't exist.

## The Most Important Change: TransportStarTreeUpgradeAction.java

This file orchestrates everything. Key methods:

- `doExecute()` — Checks idempotency, submits mapping update, then broadcasts to shards
- `buildCompleteMappingSource()` — Builds mapping JSON with existing properties + new composite section. **Critical subtlety:** strips `type` from dimension entries and `name` from config because `StarTreeMapper.Builder` doesn't expect them when `objbuilder` is present (it infers types from field builders)
- `StarTreeUpgradeMappingExecutor` — Cluster state task that creates a MapperService, sets the bypass flag, merges with STAR_TREE_UPGRADE reason, validates, and commits to cluster state
- `shardOperation()` — Calls `indexShard.upgradeToStarTree()`

## Bugs Found During Testing (in order of discovery)

1. **Double XContent wrapping** — `StarTreeField.isFragment()=true` + `XContentHelper.toXContent()` = double object. Fix: use `jsonBuilder()` directly.
2. **Unknown dimension field** — Mapping source had only composite, no properties. `StarTreeMapper.Builder` couldn't find field builders. Fix: include existing properties in mapping source.
3. **Unsupported type:numeric** — `StarTreeField.toXContent()` serializes dimension types, but `StarTreeMapper.Builder` doesn't remove them when `objbuilder` is present. Fix: strip in `buildCompleteMappingSource()`.
4. **CodecService not refreshed** — `final codecService` created before mapping update. Fix: volatile `codecServiceOverride`.
5. **forceMerge no-op** — Single segment after engine restart. Fix: write no-op + flush to create 2nd segment.
6. **_doc_count in hasAllCompositeFields** — Virtual field not in mergedFieldProducerMap. Fix: skip in check.

## How to Test

```bash
./gradlew run  # in separate terminal

# Create index, load data, upgrade
curl -X PUT "localhost:9200/ecommerce" -H 'Content-Type: application/json' -d @ReferenceTesting/ecommerce-field_mappings.json
curl -X POST "localhost:9200/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @ReferenceTesting/ecommerce.ndjson
curl -X POST "localhost:9200/ecommerce/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecommerce_star_tree",
    "ordered_dimensions": [{"name":"customer_gender"},{"name":"currency"},{"name":"day_of_week"},{"name":"order_date"}],
    "metrics": [
      {"name":"taxful_total_price","stats":["sum","avg","min","max","value_count"]},
      {"name":"taxless_total_price","stats":["sum","avg","min","max","value_count"]},
      {"name":"total_quantity","stats":["sum","avg","min","max","value_count"]},
      {"name":"total_unique_products","stats":["sum","avg","value_count"]}
    ]
  }
}'

# Verify
grep "star tree upgrade" build/testclusters/runTask-0/logs/runTask.log
curl -s "localhost:9200/ecommerce/_search?size=0" -H 'Content-Type: application/json' -d '{"aggs":{"t":{"sum":{"field":"taxful_total_price"}}}}'
```

## Known Limitations / Future Work

- 512MB heap `./gradlew run` can't handle 1M+ docs for benchmarking
- No integration tests written yet (optional tasks 8.1-8.4 were skipped)
- No unit tests for the new classes (optional tasks 1.5, 2.4, 4.4 were skipped)
- The `terminated_early: true` in search responses is NOT a reliable indicator of star tree usage — it's set by `terminateAfter`, not star tree
- Benchmark script at `ReferenceTesting/benchmark_1m.sh` is ready but needs server restart between runs
