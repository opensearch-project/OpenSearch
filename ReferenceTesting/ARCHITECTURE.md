# Star Tree Upgrade via Mapping — Architecture & Design

## Overview

This feature adds a REST API `POST /{index}/_star_tree/upgrade` that retroactively builds star tree indexes on existing OpenSearch indices. It works with any index, any supported dimension types (keyword, integer, long, date, etc.), and any metric stats (sum, avg, min, max, value_count).

Nothing is hardcoded — the user specifies dimensions and metrics in the API request body, and the system validates them against the index's existing field mappings.

## How It Works (3-Phase Flow)

```
Client Request
    │
    ▼
Phase 1: Mapping Update (cluster-wide, once)
    │  - Adds star tree field to index mapping under "composite" section
    │  - Bypasses index.composite_index setting check (it's Final, can't be changed)
    │  - Bypasses CompositeIndexValidator "no new composite fields" restriction
    │  - Validates dimensions/metrics exist and have compatible types
    │  - Propagates to all nodes via cluster state
    │
    ▼
Phase 2: Engine Restart (per-shard)
    │  - Creates fresh CodecService that sees the star tree field → selects composite codec
    │  - Closes and reopens the engine via resetEngineToGlobalCheckpoint()
    │  - Flushes to create a second segment (needed for force merge to trigger)
    │
    ▼
Phase 3: Force Merge (per-shard)
    │  - forceMerge(1) merges all segments through the composite codec
    │  - Composite912DocValuesWriter.mergeStarTreeFields() detects no existing star tree data
    │  - Falls back to building star tree from raw doc values (same as flush path)
    │  - Star tree data files (.cid, .cim, .cidvd, .cidvm) are written
    │
    ▼
Response: { "_shards": { "total": N, "successful": N, "failed": 0 } }
```

## Key Design Decisions

### Why not change index.composite_index setting?
It's `Setting.Property.Final` — cannot be changed after index creation. Instead, we bypass the two specific validation checks that gate on it:
1. `ObjectMapper.parseCompositeField()` — setting check during mapping parsing
2. `CompositeIndexValidator` — "no new composite fields" check during mapping updates

### Why does CodecService need a fresh instance?
`CodecService` is constructed once when the engine starts. It checks `mapperService.isCompositeIndexPresent()` at construction time. The original `codecService` was created before the mapping update, so it doesn't include the composite codec. We use a volatile `codecServiceOverride` to provide a fresh instance without modifying the `final` field.

### Why flush before force merge?
After `resetEngineToGlobalCheckpoint()`, if the translog was already committed, the engine opens with a single segment. Lucene's `forceMerge(1)` is a no-op with 1 segment. Flushing creates a second (empty) segment so the merge actually triggers.

### Why build from raw doc values instead of using buildDuringMerge()?
The existing `buildDuringMerge()` path expects `StarTreeValues` from source segments via `CompositeIndexReader`. Pre-upgrade segments don't have star tree data, so there's nothing to merge. The fallback uses the flush-path `StarTreesBuilder.build()` which constructs star trees from the raw doc values of the merged segment.

## Generality — Not Hardcoded

The feature is fully generic:

- **Any dimensions**: keyword, integer, long, date (with calendar intervals), half_float, float, double, short, byte, ip, unsigned_long — any field type that supports doc values and is aggregatable
- **Any metrics**: sum, avg, min, max, value_count — any combination, with automatic base/derived metric resolution
- **Any number of dimensions/metrics**: subject to the existing star tree limits (configurable via `index.composite_index.star_tree.max_fields`, `max_dimensions`, `max_base_metrics`)
- **Any index**: works on any existing index regardless of when it was created or what settings it has
- **Multiple indices**: the API accepts index patterns (e.g., `logs-*`)

The validation is the same as the normal star tree create-index flow — `StarTreeValidator` checks that dimension fields exist, are aggregatable, and have compatible types. Metric fields must be numeric.
