# Fix: Star Tree Upgrade forceMerge Was Silently a No-Op

## The Bug

`IndexShard.upgradeToStarTree()` used `forceMerge(true, 1, false, true, false, null)` with `upgrade=true` (4th param). This was supposed to force Lucene to rewrite all segments through the composite codec, even if there was only one segment.

It didn't work. The merge never fired. Star tree data was never built.

## Root Cause

The `upgrade=true` flag sets `OpenSearchMergePolicy.setUpgradeInProgress(true, false)`. During `findForcedMerges()`, the policy iterates all segments and calls `shouldUpgrade(info)` for each one:

```java
private boolean shouldUpgrade(SegmentCommitInfo info) {
    org.apache.lucene.util.Version old = info.info.getVersion();
    org.apache.lucene.util.Version cur = Version.CURRENT.luceneVersion;
    if (cur.major > old.major) return true;
    if (upgradeOnlyAncientSegments == false && cur.minor > old.minor) return true;
    return false;  // <-- segments written by same version land here
}
```

This method compares Lucene versions. It's designed for the `_upgrade` API that rewrites segments from older Lucene formats. In our case, the segments were just written by the same OpenSearch build — same Lucene version. `shouldUpgrade()` returns `false` for every segment.

The result: `spec.merges` is empty, `upgradeInProgress` resets to `false`, falls through to the delegate's `findForcedMerges()` which sees 1 segment and `maxSegmentCount=1` — nothing to do. Silent no-op.

## The Fix

Replaced the `upgrade=true` approach with the noOp tombstone technique:

```java
// Before (broken):
getEngine().forceMerge(true, 1, false, true, false, null);

// After (working):
final Engine engine = getEngine();
final long seqNo = getLocalCheckpoint() + 1;
engine.noOp(
    new Engine.NoOp(seqNo, getOperationPrimaryTerm(),
        Engine.Operation.Origin.PRIMARY, System.nanoTime(), "star-tree-upgrade")
);
engine.flush(false, true);
engine.forceMerge(true, 1, false, false, false, null);
```

### How the noOp approach works

1. After engine restart, the translog may be empty (data fully committed), leaving only 1 segment.
2. `engine.noOp()` writes a tombstone document to the IndexWriter via `addDocument()`. This is a soft-deleted noop tombstone — it doesn't affect search results or doc counts.
3. `engine.flush()` commits the tombstone as a new segment. Now there are 2 segments.
4. `forceMerge(1)` with `upgrade=false` uses the regular delegate merge policy. It sees 2 segments and needs to merge down to 1. The merge runs through `Composite912DocValuesWriter`, which detects the upgrade case (no existing star tree data + composite mapping present + all required fields in doc values) and calls `StarTreesBuilder.build()` to construct the star tree from raw doc values.

### Why noOp specifically

- It's the lightest-weight write operation available — just a tombstone with a seq_no
- It's soft-deleted, so it doesn't affect doc counts or search results
- It's an existing pattern used by the replication system for gap-filling
- It guarantees a new document in the IndexWriter, which creates a new segment on flush

## File Changed

`server/src/main/java/org/opensearch/index/shard/IndexShard.java` — `upgradeToStarTree()` method only.

## Verification

Tested with 100K docs on a fresh cluster:
- Upgrade API returns HTTP 200 with successful shards
- Server logs confirm "star tree upgrade completed successfully — star tree data built"
- Aggregation queries use the star tree (cold: 21ms vs 36ms without, warm: 2ms vs 4-5ms)
