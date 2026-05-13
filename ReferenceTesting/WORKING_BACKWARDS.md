# Retroactive Star Tree Indexing for OpenSearch

## Press Release

**OpenSearch Introduces Retroactive Star Tree Upgrade — Accelerate Aggregation Queries on Existing Indices Without Reindexing**

*Cluster operators can now add star tree indexes to historical data in-place, reducing aggregation query latency by orders of magnitude without downtime or data migration.*

OpenSearch today announced the Retroactive Star Tree Upgrade feature, which allows cluster operators to add star tree acceleration to existing indices that were created before star tree support was enabled — or before the feature existed at all. Previously, benefiting from star tree indexes required creating a new index with `index.composite_index=true` and reindexing all data, a process that is expensive, time-consuming, and disruptive for large datasets.

With the new `POST /{index}/_star_tree/upgrade` API, operators provide a star tree configuration (dimensions, metrics, and optional build parameters) in the request body, and OpenSearch handles the rest: updating the index mapping, building star tree data structures on each segment, and switching the codec — all while maintaining read availability throughout the upgrade window.

"Our customers have petabytes of historical data in OpenSearch indices that were created before star tree was available," said the OpenSearch team. "Asking them to reindex is not practical. This feature lets them unlock star tree acceleration on existing data with a single API call, with zero downtime for search operations."

---

## FAQ

### Customer FAQ

**Q: What problem does this solve?**
Star tree indexes dramatically accelerate aggregation queries (sum, avg, min, max, value_count) by pre-computing partial aggregations at index time. However, until now, star tree could only be configured at index creation time via `index.composite_index=true` (a Final setting that cannot be changed after creation). This meant existing indices with historical data could not benefit from star tree acceleration without full reindexing.

**Q: How do I use it?**
Send a POST request to the star tree upgrade API with your desired configuration:

```bash
POST /my-index/_star_tree/upgrade
{
  "star_tree": {
    "name": "my_star_tree",
    "ordered_dimensions": [
      { "name": "timestamp" },
      { "name": "status" }
    ],
    "metrics": [
      { "name": "response_time", "stats": ["sum", "avg", "min", "max", "value_count"] }
    ],
    "max_leaf_docs": 10000
  }
}
```

The API returns per-shard results indicating success or failure.

**Q: Are reads available during the upgrade?**
Yes. The upgrade swaps the read-write engine to a read-only engine during the upgrade window. Search and read operations continue serving from the last committed segment state. Writes are blocked for the duration of the upgrade on each shard.

**Q: How long does the upgrade take?**
It depends on the data volume and number of segments. The upgrade operates per-segment: it reads doc values, builds star tree data structures, and rewrites segment metadata. For a 1M document index with ~20 fields, the per-segment star tree build takes roughly 20-30% of the time a full force merge would take, since it only builds star tree structures rather than rewriting all segment data (stored fields, postings, norms, point values, etc.).

**Q: Is the upgrade safe to retry?**
Yes. The upgrade is idempotent:
- If the index already has star tree configured and all segments have star tree data → returns success with no work done.
- If the mapping was updated but some segments lack star tree data (partial failure) → skips the mapping update and completes the per-segment upgrade.
- Concurrent upgrade requests on the same shard are rejected.

**Q: What happens if the upgrade fails partway through?**
The upgrade is designed for partial failure resilience:
- Segments that fail during star tree data generation are skipped; only successfully upgraded segments get the codec switch.
- The original `segments_N` commit remains intact if the SegmentInfos rewrite fails (atomic commit semantics).
- Orphaned star tree files from failed segments are cleaned up.
- The shard is always left in a consistent, readable state.

**Q: What are the constraints on the star tree configuration?**
- At least 2 dimensions are required.
- Dimension fields must exist in the index and be aggregatable (keyword, integer, long, date, ip, float, double, etc.).
- Metric fields must be numeric (integer, long, float, double, half_float, short, byte, unsigned_long).
- The index must not already have a star tree configuration.
- All primary shards must be available.

**Q: Does this modify the `index.composite_index` setting?**
Yes. The upgrade sets `index.composite_index=true` and `index.append_only.enabled=true` in the cluster state so that the metadata is truthful and subsequent engine creations (node restarts, shard relocations) use the composite codec natively.

---

### Technical FAQ

**Q: How does the upgrade bypass the Final setting checks?**
Two bypass mechanisms work together:
1. A `allowCompositeFieldWithoutSettings` flag on `Mapper.TypeParser.ParserContext` skips the `IS_COMPOSITE_INDEX_SETTING` and `INDEX_APPEND_ONLY_ENABLED_SETTING` checks in `ObjectMapper.parseCompositeField()`.
2. A new `STAR_TREE_UPGRADE` value in `MapperService.MergeReason` tells `CompositeIndexValidator` to skip the "no new composite fields during update" restriction while still running `StarTreeValidator.validate()` for field compatibility.

**Q: Why not use force merge?**
The initial implementation (Spec 1) used force merge, which rewrites ALL segment data into one new segment. For 1M docs with ~20 fields, the star tree build is only ~20-30% of the total time — the other 70-80% is Lucene rewriting data unrelated to star tree. The per-segment approach (Spec 3) builds star tree data directly on each segment and switches the codec via a direct SegmentInfos rewrite, avoiding the unnecessary data rewrite.

**Q: Why not use IndexUpgrader?**
`IndexUpgrader` opens an `IndexWriter` internally, which acquires a write lock (conflicting with the ReadOnlyEngine), has no way to discover star tree files written in Phase 1 (they're not in any committed file set yet), and upgrades ALL segments including those that failed in Phase 1 (creating corrupt state).

**Q: How does the ReadOnlyEngine bridge work?**
The upgrade atomically swaps `currentEngineReference` from InternalEngine → ReadOnlyEngine (with `obtainLock=false`), performs the two-phase upgrade, then atomically swaps back to a new InternalEngine. The ReadOnlyEngine holds a `DirectoryReader` snapshot of `segments_N` that is unaffected by Phase 2's `segments_N+1` commit. At no point does `currentEngineReference` become null.

**Q: How is the codec picked up after the upgrade?**
A `codecServiceOverride` volatile field on IndexShard provides a fresh `CodecService` that sees the updated mapping (with composite field types). This override is checked by `newEngineConfig()` before falling back to the stale final `codecService`. The override persists after the upgrade so that `resetEngineToGlobalCheckpoint()` also uses the composite codec. It is cleared after the first post-upgrade engine reset confirms the persistent `index.composite_index=true` setting took effect.

---

## Appendix: Implementation Summary

### Evolution Across Three Specs

The feature was developed iteratively across three Kiro specs:

**Spec 1: Star Tree Upgrade via Mapping** — Established the core upgrade flow: mapping update (bypassing Final setting checks) → engine restart → force merge. Introduced the `STAR_TREE_UPGRADE` merge reason, `allowCompositeFieldWithoutSettings` bypass flag, the merge path enhancement in `Composite912DocValuesWriter` to build star trees from raw doc values when no source segments have star tree data, and the full transport/REST action infrastructure.

**Spec 2: Star Tree Upgrade Read Availability** — Replaced the "close engine → upgrade → reopen" pattern with the ReadOnlyEngine bridge pattern (InternalEngine → ReadOnlyEngine → InternalEngine), ensuring `currentEngineReference` is never null and reads remain available. Also fixed the `codecServiceOverride` lifecycle (persist after upgrade, don't clear), split `StarTreeUpgradeService.upgradeSegments()` into `buildStarTreeDataForSegments()` + `rewriteSegmentInfos()` for engine lifecycle interleaving, and set `index.composite_index=true` in cluster state during the mapping update.

**Spec 3: Retroactive Star Tree Building** — Replaced the force merge approach with a direct per-segment upgrade: Phase 1 builds star tree files using standard Lucene doc values APIs and raw `IndexOutput`, Phase 2 rewrites SegmentInfos to switch the codec declaration and include star tree files. This is dramatically faster than force merge since it only builds star tree structures rather than rewriting all segment data.

### Files Changed

**New files (7):**
- `StarTreeUpgradeAction.java` — Action type registration
- `StarTreeUpgradeRequest.java` — Request with star tree config parsing
- `StarTreeUpgradeResponse.java` — Broadcast response wrapper
- `ShardStarTreeUpgradeResult.java` — Per-shard result
- `TransportStarTreeUpgradeAction.java` — Transport action with mapping update orchestration
- `RestStarTreeUpgradeAction.java` — REST handler for `POST /{index}/_star_tree/upgrade`
- `StarTreeUpgradeService.java` — Two-phase per-segment upgrade logic

**Modified files (7):**
- `MapperService.java` — Added `STAR_TREE_UPGRADE` merge reason
- `Mapper.java` — Added `allowCompositeFieldWithoutSettings` flag to ParserContext
- `ObjectMapper.java` — Wrapped setting checks with bypass flag
- `DocumentMapperParser.java` — Propagated bypass flag to ParserContext
- `CompositeIndexValidator.java` — Added merge reason-aware validation overload
- `Composite912DocValuesWriter.java` — Enhanced merge path to build from raw doc values
- `IndexShard.java` — Added `upgradeToStarTree()` with ReadOnlyEngine bridge, `codecServiceOverride`, concurrent upgrade guard

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Per-segment upgrade over force merge | 3-5x faster — only builds star tree data, doesn't rewrite all segment data |
| Direct SegmentInfos rewrite over IndexUpgrader | No write lock conflict with ReadOnlyEngine, full control over which segments get codec switch |
| ReadOnlyEngine bridge over engine close | Maintains read availability during upgrade (eliminates 27+ second read blackout for large indices) |
| `obtainLock=false` on ReadOnlyEngine | Allows Phase 2 to acquire the write lock for SegmentInfos rewrite |
| `codecServiceOverride` persistence | Ensures `resetEngineToGlobalCheckpoint()` uses composite codec; cleared after persistent setting confirmed |
| `index.composite_index=true` in cluster state | Makes metadata truthful; subsequent engine creations use composite codec natively without override |
| Bypass via ParserContext flag + MergeReason | Minimal, targeted bypass — still validates field compatibility via StarTreeValidator |
| Cleanup targets all candidate segments | Failed segments may have partial star tree files that also need cleanup |
