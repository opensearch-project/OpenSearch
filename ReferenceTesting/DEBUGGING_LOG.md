# Star Tree Upgrade Feature - Debugging Log

## Bug 1: JSON Generation Error - Double Object Wrapping
**Error:** `Can not start an object, expecting field name (context: Object)`
**Root Cause:** `StarTreeField.toXContent()` calls `builder.startObject()`, but `XContentHelper.toXContent()` also wraps the output in `startObject()/endObject()` because `StarTreeField.isFragment()` returns true (default). This caused a double-object wrapping.
**Fix:** In `StarTreeUpgradeRequest.serializeStarTreeFieldToBytes()` and `TransportStarTreeUpgradeAction.buildCompleteMappingSource()`, use `XContentFactory.jsonBuilder()` directly instead of `XContentHelper.toXContent()` to avoid the double wrapping.
**Files Changed:**
- `server/src/main/java/org/opensearch/action/admin/indices/startree/StarTreeUpgradeRequest.java`
- `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java`

## Bug 2: Unknown Dimension Field Error
**Error:** `Failed to parse mapping [_doc]: unknown dimension field [customer_gender]`
**Root Cause:** `buildStarTreeMappingSource()` created a mapping with only the `composite` section (no `properties`). When `MapperService.merge()` parsed this, `StarTreeMapper.Builder.getDimension()` tried to find dimension fields in `objbuilder.mappersBuilders`, which was empty because properties weren't in the mapping source.
**Fix:** Created `buildCompleteMappingSource()` that includes the existing properties from `IndexMetadata` alongside the new composite section, so `StarTreeMapper.Builder` can find dimension/metric field builders during validation.
**Files Changed:**
- `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java`

## Bug 3: Unsupported Parameters - type:numeric in Dimensions
**Error:** `Star tree mapping definition has unsupported parameters: [type : numeric]`
**Root Cause:** `StarTreeField.toXContent()` serializes `"type": "numeric"` for each `NumericDimension`. When the mapping source includes properties (so `objbuilder` is present), `StarTreeMapper.Builder.getDimension()` doesn't remove the `type` field from the dimension map (it only does so when `objbuilder` is null). `checkNoRemainingFields()` then rejects the leftover `type`.
**Fix:** Strip `type` from each dimension entry and `name` from the top-level config map in `buildCompleteMappingSource()` before putting it in the mapping source.
**Files Changed:**
- `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java`

## Bug 4: Force Merge No-Op with Single Segment
**Error:** Star tree data not built in segments (NPE when querying: `starTreeValues is null`)
**Root Cause:** Three issues combined:
1. The `CodecService` used by the new engine was the original one (created before the mapping update), which didn't include the composite codec. Fixed by adding a volatile `codecServiceOverride` field.
2. `forceMerge(1)` is a no-op when there's only 1 segment — Lucene doesn't rewrite a single segment.
3. After `resetEngineToGlobalCheckpoint()`, if the translog was already committed, the engine opens with just 1 segment.
**Fix:** 
1. Added volatile `codecServiceOverride` field to `IndexShard`. In `upgradeToStarTree()`, create a fresh `CodecService` and set it as override. `newEngineConfig()` checks this override.
2. After engine restart, call `flush(force=true)` to create a second (empty) segment, then `forceMerge(1)` to merge both segments through the composite codec.
**Files Changed:**
- `server/src/main/java/org/opensearch/index/shard/IndexShard.java`

## Bug 5: Merge Path Fallback Too Permissive (found during checkpoint)
**Error:** `UnsupportedOperationException` from `EmptyDocValuesProducer.getSortedNumeric()` in Lucene base tests
**Root Cause:** The condition `mergedFieldProducerMap.isEmpty() == false` was too permissive — it triggered the star tree build fallback whenever ANY field matched a composite field name. Lucene tests with generic field names could accidentally match.
**Fix:** Changed the condition to verify ALL required composite fields are present in `mergedFieldProducerMap` before attempting the fallback build.
**Files Changed:**
- `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesWriter.java`

## Bug 6: hasAllCompositeFields check fails due to _doc_count
**Error:** "NOT all composite fields present in mergedFieldProducerMap, skipping build"
**Root Cause:** `CompositeMappedFieldType.fields()` includes `_doc_count` (a virtual metric added by `StarTreeMapper.Builder.buildMetrics()`). But `_doc_count` doesn't exist as a real doc values field in the merged segment, so `mergedFieldProducerMap` doesn't contain it. The `hasAllCompositeFields` check fails.
**Fix:** Skip `_doc_count` (via `DocCountFieldMapper.NAME`) in the `hasAllCompositeFields` check, since it's a virtual field handled separately by `addDocValuesForEmptyField()` in `buildFieldProducerMapFromMergeState()`.
**Files Changed:**
- `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesWriter.java`

## Test Commands
```bash
# Start OpenSearch
./gradlew run

# Create index (no star tree)
curl -X PUT "localhost:9200/ecommerce" -H 'Content-Type: application/json' -d @ReferenceTesting/ecommerce-field_mappings.json

# Bulk index data
curl -X POST "localhost:9200/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @ReferenceTesting/ecommerce.ndjson

# Upgrade to star tree
curl -X POST "localhost:9200/ecommerce/_star_tree/upgrade" -H 'Content-Type: application/json' -d '{
  "star_tree": {
    "name": "ecommerce_star_tree",
    "ordered_dimensions": [
      {"name": "customer_gender"},
      {"name": "currency"},
      {"name": "day_of_week"},
      {"name": "order_date"}
    ],
    "metrics": [
      {"name": "taxful_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "taxless_total_price", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_quantity", "stats": ["sum", "avg", "min", "max", "value_count"]},
      {"name": "total_unique_products", "stats": ["sum", "avg", "value_count"]}
    ]
  }
}'

# Verify mapping
curl -s "localhost:9200/ecommerce/_mapping" | python3 -m json.tool

# Test aggregation
curl -X POST "localhost:9200/ecommerce/_search?size=0" -H 'Content-Type: application/json' -d '{
  "aggs": {
    "revenue_by_gender": {
      "terms": {"field": "customer_gender"},
      "aggs": {
        "total_revenue": {"sum": {"field": "taxful_total_price"}}
      }
    }
  }
}'
```
