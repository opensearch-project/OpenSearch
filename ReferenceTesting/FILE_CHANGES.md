# Star Tree Upgrade via Mapping — File-by-File Changes

## New Files (6 files)

### 1. `server/src/main/java/org/opensearch/action/admin/indices/startree/StarTreeUpgradeAction.java`
Action type class. Defines `NAME = "indices:admin/startree/upgrade"` and singleton `INSTANCE`. Extends `ActionType<StarTreeUpgradeResponse>`. Standard OpenSearch action registration pattern.

### 2. `server/src/main/java/org/opensearch/action/admin/indices/startree/StarTreeUpgradeRequest.java`
Request class extending `BroadcastRequest`. Contains:
- `StarTreeField starTreeField` — the star tree configuration
- `parseStarTreeConfig(BytesReference, MediaType)` — static method to parse the REST request body into a `StarTreeField`
- Serialization via XContent bytes (not `Writeable` since `StarTreeField` doesn't implement it)
- Validation: checks dimensions (≥2), metrics, name, duplicate detection, metric stat resolution

### 3. `server/src/main/java/org/opensearch/action/admin/indices/startree/StarTreeUpgradeResponse.java`
Response class extending `BroadcastResponse`. Thin wrapper — the base class handles totalShards, successfulShards, failedShards, and shardFailures.

### 4. `server/src/main/java/org/opensearch/action/admin/indices/startree/ShardStarTreeUpgradeResult.java`
Per-shard result implementing `Writeable`. Contains `ShardId` and `boolean primary`.

### 5. `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java`
The main transport action. Extends `TransportBroadcastByNodeAction`. This is the most complex new file:
- `doExecute()`: Resolves indices, checks idempotency (skip mapping update if star tree already in mapping), submits mapping update via cluster state, then delegates to super for per-shard broadcast
- `shardOperation()`: Calls `indexShard.upgradeToStarTree()`, wraps checked exceptions
- `StarTreeUpgradeMappingExecutor` (inner class): `ClusterStateTaskExecutor` that applies the mapping update — creates a `MapperService`, sets the bypass flag, merges with `STAR_TREE_UPGRADE` reason, validates via `CompositeIndexValidator`
- `buildCompleteMappingSource()`: Builds a complete mapping JSON that includes existing properties + new composite section. Strips `type` from dimensions and `name` from config to match what `StarTreeMapper.Builder` expects
- `hasExistingStarTreeConfig()`: Checks if index already has a composite field in its mapping
- `indicesWithMissingPrimaries()`: Validates all primary shards are available

### 6. `server/src/main/java/org/opensearch/action/admin/indices/startree/RestStarTreeUpgradeAction.java`
REST handler. Registers `POST /{index}/_star_tree/upgrade`. Parses request body via `StarTreeUpgradeRequest.parseStarTreeConfig()`, creates request, delegates to transport action.

---

## Modified Files (6 files)

### 7. `server/src/main/java/org/opensearch/index/mapper/MapperService.java`
**Change**: Added `STAR_TREE_UPGRADE` to the `MergeReason` enum.
```java
STAR_TREE_UPGRADE;  // new value
```
Used to signal bypass of composite index setting checks during the upgrade path.

### 8. `server/src/main/java/org/opensearch/index/mapper/Mapper.java`
**Change**: Added `allowCompositeFieldWithoutSettings` flag to `Mapper.TypeParser.ParserContext`.
```java
private boolean allowCompositeFieldWithoutSettings = false;
// + getter isAllowCompositeFieldWithoutSettings()
// + setter setAllowCompositeFieldWithoutSettings(boolean)
```
This flag is checked by `ObjectMapper.parseCompositeField()` to skip the `IS_COMPOSITE_INDEX_SETTING` check.

### 9. `server/src/main/java/org/opensearch/index/mapper/ObjectMapper.java`
**Change**: Wrapped the `IS_COMPOSITE_INDEX_SETTING` and `INDEX_APPEND_ONLY_ENABLED_SETTING` checks in `parseCompositeField()` with:
```java
if (parserContext.isAllowCompositeFieldWithoutSettings() == false) {
    // existing setting checks
}
```
When the flag is true (set during star tree upgrade), both checks are skipped.

### 10. `server/src/main/java/org/opensearch/index/mapper/DocumentMapperParser.java`
**Change**: Added `allowCompositeFieldWithoutSettings` field and `setAllowCompositeFieldWithoutSettings()` method. The `parserContext()` methods now propagate this flag to every `ParserContext` they create.

### 11. `server/src/main/java/org/opensearch/index/compositeindex/CompositeIndexValidator.java`
**Change**: Added a 5-arg `validate()` overload that accepts `MergeReason`. When `mergeReason == STAR_TREE_UPGRADE`, skips the "no new composite fields during update" check but still runs `StarTreeValidator.validate()` for field compatibility. The existing 4-arg method delegates to the new one with `MAPPING_UPDATE` as default.

### 12. `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesWriter.java`
**Change**: Enhanced `mergeStarTreeFields()` with a fallback path for the upgrade case:
- When `starTreeSubsPerField` is empty (no source segments have star tree data) but `compositeMappedFieldTypes` is non-empty (mapping has star tree config):
  - Checks if ALL required composite fields (excluding `_doc_count`) are present in `mergedFieldProducerMap`
  - If yes: builds star tree from raw doc values via `StarTreesBuilder.build()` (flush path)
  - If no: falls back to `buildDuringMerge()` (no-op, for Lucene test compatibility)
- Added `buildFieldProducerMapFromMergeState()` helper that constructs the field producer map from merged doc values, filling in empty producers for missing fields
- Added `mergedFieldProducerMap` field that captures doc values producers during `super.merge()` via `addNumericField`, `addSortedNumericField`, `addSortedSetField`

### 13. `server/src/main/java/org/opensearch/index/shard/IndexShard.java`
**Change**: Added `upgradeToStarTree()` method and `codecServiceOverride` volatile field:
```java
private volatile CodecService codecServiceOverride;

public void upgradeToStarTree() {
    // 1. Create fresh CodecService with composite codec
    this.codecServiceOverride = engineConfigFactory.newDefaultCodecService(...);
    // 2. Engine restart
    blockOperations(() -> resetEngineToGlobalCheckpoint());
    // 3. Flush to create 2nd segment
    flush(force=true);
    // 4. Force merge through composite codec
    forceMerge(maxNumSegments=1);
}
```
Also modified `newEngineConfig()` to check `codecServiceOverride` before falling back to the original `codecService`.

### 14. `server/src/main/java/org/opensearch/action/ActionModule.java`
**Change**: Registered the new action and REST handler:
- `actions.register(StarTreeUpgradeAction.INSTANCE, TransportStarTreeUpgradeAction.class)`
- `registerHandler.accept(new RestStarTreeUpgradeAction())`
