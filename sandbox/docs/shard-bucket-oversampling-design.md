# Per-Shard Bucket Oversampling for Top-K Aggregations

## Problem

When a PPL query does `stats ... by group_key | sort - agg_col | head N`, each shard independently computes ALL groups and sends them to the coordinator. The coordinator merges, sorts, and returns top-N. This is wasteful — shards compute groups that will never appear in the final top-N.

## Solution

Oversample on each shard: compute all groups but only ship the top `N × factor` rows (sorted by the same key) to the coordinator. The coordinator merges the oversized per-shard results and returns the final top-N. This bounds network transfer and reduce-stage memory without sacrificing correctness (assuming the factor is large enough).

## Design

### Phase 1: Marking (pre-CBO rule)

A pre-CBO `OpenSearchAggregateShardBucketRule` fires on every `OpenSearchSort` in the plan tree:

```
Pattern: OpenSearchSort → (optional Project) → OpenSearchAggregate(SINGLE)
```

The rule:
1. Verifies the Sort has collation (not a bare LIMIT)
2. Finds the Aggregate below (skipping one optional Project)
3. Rejects join/union subtrees (multi-table)
4. Rejects single-shard topologies (no split will happen)
5. Computes `shardSize = ceil(max(coordLimit, DEFAULT) × factor) + DEFAULT`
6. Attaches a `ShardBucketHint` to the Aggregate

The hint carries: `shardSize`, synthetic collation, sort expressions remapped to agg output.

**No plan modification** — just a hint. The actual Sort insertion happens later.

### Phase 2: Split (existing infrastructure)

`OpenSearchAggregateSplitRule` splits `Aggregate(SINGLE)` → `Aggregate(FINAL) + ER + Aggregate(PARTIAL)` as it does today. The hint is preserved on the FINAL aggregate.

### Phase 3: Rewrite (post-Volcano)

`DistributedAggregateRewriter` runs after CBO picks the best plan. When it sees a FINAL aggregate with a `ShardBucketHint`:

1. Finds the corresponding PARTIAL aggregate (below the ExchangeReducer)
2. Inserts an `OpenSearchSort(perPartition=true, collation, fetch=shardSize)` between the PARTIAL aggregate and the ER
3. The per-partition Sort limits each shard's output to `shardSize` rows sorted by the aggregation result

The substrait emitted for the shard fragment becomes: `Sort(fetch=shardSize) → Aggregate(PARTIAL) → Scan`.

### Phase 4: HLL (approx_count_distinct) — state shipping

`approx_count_distinct` can't be split into PARTIAL/FINAL by the existing split rule because its intermediate state is an HLL sketch (complex binary blob). For this specific function:

- Register a `StateShippingUdaf` wrapper around `approx_distinct_udaf()` only
- The wrapper's PARTIAL mode serializes the HLL state as IPC-encoded Binary
- The FINAL mode (via `state_finalize` scalar UDF) deserializes and merges

All other aggregates (SUM, AVG, COUNT, MIN, MAX, STDDEV, VAR) use the existing split/reduce infrastructure on main — no wrapping needed.

### Index Setting

```
index.analytics.shard_bucket_oversampling_factor (double, default 1.5, dynamic)
```

Stored on `OpenSearchTableScan` at planning time. Factor of 0 disables oversampling.

## Plan shapes

### Before (no oversampling)
```
Sort(sort, fetch=10)
  Aggregate(FINAL)
    ExchangeReducer
      Aggregate(PARTIAL)
        TableScan
```

### After (with oversampling, factor=1.5)
```
Sort(sort, fetch=10)
  Aggregate(FINAL)
    ExchangeReducer
      Sort(sort, fetch=25, perPartition=true)   ← inserted by rewriter
        Aggregate(PARTIAL)
          TableScan
```

## Files changed

| File | Change |
|------|--------|
| `AnalyticsApproximationSettings.java` | New setting class |
| `OpenSearchTableScan.java` | Store `shardBucketFactor` |
| `ShardBucketHint.java` | Hint record (shardSize, collation, sortExprs) |
| `OpenSearchAggregateShardBucketRule.java` | Pre-CBO rule: mark hint |
| `DistributedAggregateRewriter.java` | Post-Volcano: insert per-partition Sort |
| `OpenSearchSort.java` | Add `perPartition` flag |
| `state_shipping.rs` | Wrap `approx_distinct` only |
| `state_finalize.rs` | Finalize UDF for HLL only |
| `ShardBucketOversamplingIT.java` | Integration tests |
