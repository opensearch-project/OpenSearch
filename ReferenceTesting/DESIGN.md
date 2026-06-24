# Star Tree Upgrade via Mapping — Detailed Design Document

## 1. Problem Statement

OpenSearch supports star tree indexes for accelerating aggregation queries, but they must be configured at index creation time via `index.composite_index=true` and `index.append_only.enabled=true` settings. Both are `Setting.Property.Final` — they cannot be changed after index creation.

This means existing indices with historical data cannot benefit from star tree acceleration without full reindexing, which is expensive and disruptive for large datasets.

## 2. Solution

A new REST API `POST /{index}/_star_tree/upgrade` that:
1. Adds the star tree field to the index mapping (bypassing the Final setting checks)
2. Restarts the engine so the composite codec is selected
3. Force merges all segments to build star tree data from raw doc values

The `index.composite_index` and `index.append_only.enabled` settings are never modified.

## 3. API Specification

### Endpoint
```
POST /{index}/_star_tree/upgrade
```

### Request Body
```json
{
  "star_tree": {
    "name": "my_star_tree",
    "ordered_dimensions": [
      { "name": "field1" },
      { "name": "field2" }
    ],
    "metrics": [
      { "name": "metric_field", "stats": ["sum", "avg", "min", "max", "value_count"] }
    ],
    "max_leaf_docs": 10000,
    "skip_star_node_creation_for_dimensions": []
  }
}
```

### Response
```json
{
  "_shards": {
    "total": 5,
    "successful": 5,
    "failed": 0
  }
}
```

### Constraints
- At least 2 dimensions required
- Dimension fields must exist in the index and be aggregatable (keyword, integer, long, date, etc.)
- Metric fields must be numeric (integer, long, float, double, half_float, etc.)
- Index must not already have a star tree configuration
- All primary shards must be available

## 4. Architecture

### Component Interaction Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Client Request                              │
│              POST /ecommerce/_star_tree/upgrade                     │
│              { "star_tree": { ... config ... } }                    │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                   RestStarTreeUpgradeAction                          │
│  - Parses request body → StarTreeField                               │
│  - Creates StarTreeUpgradeRequest(indices, starTreeField)            │
│  - Delegates to TransportStarTreeUpgradeAction                       │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│               TransportStarTreeUpgradeAction                         │
│               (extends TransportBroadcastByNodeAction)               │
│                                                                      │
│  doExecute():                                                        │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ 1. Resolve indices, validate primary shards available          │  │
│  │ 2. Check idempotency (skip if star tree already in mapping)    │  │
│  │ 3. Submit mapping update to cluster state                      │  │
│  │    └─ StarTreeUpgradeMappingExecutor (inner class)             │  │
│  │       ├─ Creates MapperService for target index                │  │
│  │       ├─ Sets allowCompositeFieldWithoutSettings = true        │  │
│  │       ├─ Merges mapping with MergeReason.STAR_TREE_UPGRADE     │  │
│  │       ├─ Calls CompositeIndexValidator.validate(STAR_TREE_UPG) │  │
│  │       └─ Commits updated mapping to cluster state              │  │
│  │ 4. Wait for ack from all nodes                                 │  │
│  │ 5. Broadcast shardOperation() to all nodes                     │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  shardOperation() (per primary shard):                               │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ Calls indexShard.upgradeToStarTree()                           │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    IndexShard.upgradeToStarTree()                     │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │ 1. Create fresh CodecService (sees star tree → composite codec) │ │
│  │    └─ Set as codecServiceOverride (volatile, checked by         │ │
│  │       newEngineConfig instead of the final codecService)        │ │
│  │                                                                 │ │
│  │ 2. Block operations                                             │ │
│  │    └─ resetEngineToGlobalCheckpoint()                           │ │
│  │       ├─ Flush existing data                                    │ │
│  │       ├─ Close old engine                                       │ │
│  │       ├─ Create new engine with composite codec                 │ │
│  │       └─ Replay translog                                        │ │
│  │    └─ Unblock operations                                        │ │
│  │                                                                 │ │
│  │ 3. Flush (force=true) → creates 2nd segment                    │ │
│  │    (needed because forceMerge(1) is no-op with 1 segment)      │ │
│  │                                                                 │ │
│  │ 4. forceMerge(maxNumSegments=1)                                 │ │
│  │    └─ Lucene merges all segments using composite codec          │ │
│  │       └─ Composite912DocValuesWriter.merge()                    │ │
│  │          └─ mergeStarTreeFields()                               │ │
│  │             └─ Detects: no star tree data in source segments    │ │
│  │             └─ Falls back to build() from raw doc values        │ │
│  │             └─ StarTreesBuilder builds star tree data           │ │
│  │             └─ Writes .cid, .cim, .cidvd, .cidvm files         │ │
│  │                                                                 │ │
│  │ 5. Clear codecServiceOverride                                   │ │
│  └─────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

## 5. Bypass Mechanism — How Settings Checks Are Skipped

### The Problem
Two checks block adding star tree fields to existing indices:
1. `ObjectMapper.parseCompositeField()` checks `IS_COMPOSITE_INDEX_SETTING` (Final setting)
2. `CompositeIndexValidator.validate()` blocks "addition of new composite fields during update"

### The Solution — Two-Layer Bypass

**Layer 1: ParserContext flag**
```
Mapper.TypeParser.ParserContext
  └─ allowCompositeFieldWithoutSettings = false (default)
  └─ Set to true by DocumentMapperParser when TransportStarTreeUpgradeAction
     calls mapperService.documentMapperParser().setAllowCompositeFieldWithoutSettings(true)

ObjectMapper.parseCompositeField()
  └─ if (parserContext.isAllowCompositeFieldWithoutSettings() == false) {
       // check IS_COMPOSITE_INDEX_SETTING → throw if false
       // check INDEX_APPEND_ONLY_ENABLED_SETTING → throw if false
     }
     // When flag is true, both checks are skipped
```

**Layer 2: MergeReason check**
```
CompositeIndexValidator.validate(..., MergeReason mergeReason)
  └─ if (mergeReason == STAR_TREE_UPGRADE) {
       // Skip "no new composite fields during update" check
       // Still run StarTreeValidator.validate() for field compatibility
       return;
     }
```

### What's NOT Bypassed
- `StarTreeValidator.validate()` — always runs, validates dimensions/metrics against existing fields
- Field type compatibility checks — dimensions must be aggregatable, metrics must be numeric
- Star tree limits — max dimensions, max metrics, max fields (from index settings)

## 6. Codec Selection — How the Composite Codec Gets Picked Up

```
CodecService constructor:
  if (mapperService.isCompositeIndexPresent()) {
    → compositeCodecFactory.getCompositeIndexCodecs(...)  // includes Composite912Codec
  } else {
    → PerFieldMappingPostingFormatCodec(...)              // standard codec
  }

MapperService.isCompositeIndexPresent():
  return this.mapper != null && !getCompositeFieldTypes().isEmpty()
  // Checks if composite field types exist in the mapping — NO setting check
```

The original `codecService` (created at shard init) doesn't have the composite codec because the mapping didn't have star tree fields yet. The `codecServiceOverride` mechanism provides a fresh `CodecService` that sees the updated mapping.

```
IndexShard.newEngineConfig():
  engineConfigFactory.newCodecServiceOrDefault(
    indexSettings, mapperService, logger,
    codecServiceOverride != null ? codecServiceOverride : codecService  // ← check override first
  )
```

## 7. Merge Path — How Star Tree Data Gets Built

```
Composite912DocValuesWriter.mergeStarTreeFields(mergeState):

  1. Collect existing StarTreeValues from source segments
     └─ For each segment's DocValuesProducer:
        └─ Check if it's a CompositeIndexReader
        └─ If yes, get StarTreeValues → add to starTreeSubsPerField

  2. Decision:
     ┌─ starTreeSubsPerField is NOT empty (normal merge, all segments have star tree)
     │  └─ buildDuringMerge() — merges existing star tree data
     │
     ├─ starTreeSubsPerField IS empty AND compositeMappedFieldTypes NOT empty (UPGRADE CASE)
     │  └─ Check: all composite fields (except _doc_count) in mergedFieldProducerMap?
     │     ├─ YES → buildFieldProducerMapFromMergeState()
     │     │        └─ Copy mergedFieldProducerMap (captured during super.merge())
     │     │        └─ Add empty producers for missing fields (_doc_count, etc.)
     │     │        └─ StarTreesBuilder.build() — same as flush path
     │     │
     │     └─ NO → buildDuringMerge() with empty map (no-op, for test compatibility)
     │
     └─ starTreeSubsPerField IS empty AND compositeMappedFieldTypes IS empty
        └─ buildDuringMerge() with empty map (no-op, no star tree config)
```

### How mergedFieldProducerMap Gets Populated
During `super.merge()`, Lucene calls `addNumericField()`, `addSortedNumericField()`, `addSortedSetField()` for each field in the merged segment. Our overrides capture the `DocValuesProducer` for each composite field:

```java
if (mergeState.get() != null) {
    if (compositeFieldSet.contains(field.name)) {
        mergedFieldProducerMap.put(field.name, valuesProducer);
    }
}
```

## 8. Idempotency & Error Handling

### Idempotency
- If star tree already in mapping AND all segments have star tree data → return success (no work)
- If star tree already in mapping BUT some segments lack star tree data → skip mapping update, run force merge (partial retry)
- If star tree NOT in mapping → full upgrade (mapping update + engine restart + force merge)

### Error Handling
- Invalid config (missing dims/metrics) → rejected before any changes
- Dimension/metric field doesn't exist → rejected by StarTreeValidator during mapping update
- Mapping update fails → no side effects (cluster state not modified)
- Force merge fails on a shard → reported in response, mapping stays (retry will complete it)
- Concurrent upgrade → second request blocked by shard operation permits

## 9. Supported Field Types

### Dimensions
| Type | Doc Values Type | Notes |
|------|----------------|-------|
| keyword | SORTED_SET | Ordinal-based dimension |
| integer, long, short, byte | SORTED_NUMERIC | Numeric dimension |
| float, double, half_float | SORTED_NUMERIC | Numeric dimension |
| date | SORTED_NUMERIC | Date dimension with calendar intervals |
| ip | SORTED_NUMERIC | IP dimension |
| unsigned_long | SORTED_NUMERIC | Unsigned long dimension |

### Metrics
Any numeric field type: integer, long, float, double, half_float, short, byte, unsigned_long

### Stats
sum, avg, min, max, value_count — with automatic base/derived metric resolution (e.g., avg requires sum + value_count)
