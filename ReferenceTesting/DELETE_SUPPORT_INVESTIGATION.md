# Star Tree Upgrade — Delete Support: RESOLVED

## Summary

The star tree upgrade works correctly with indices that have prior deletes. Both code paths are proven at 1M scale:

1. **Segments with `docValuesGen == -1`** → full codec switch to Composite912Codec → native star tree read path
2. **Segments with `docValuesGen != -1`** → codec NOT switched, star tree files built as sidecar → `StarTreeDirectReader` cache serves queries

## Test Results

### 1M docs + 50k deletes (test_1m_full_verification.sh)
- Upgrade: successful (7.5s with parallel build)
- Aggregation: ALL VALUES MATCH (before == after)
- `terminated_early: True` — star tree active
- 7 segments with `docValuesGen=1` served via direct reader cache
- 0 wrong values during concurrent reads
- Doc count: 950,000 (correct)

### Java integration test (StarTreeUpgradeWithDocValuesGenIT)
- Deterministically forces `docValuesGen != -1` via: ingest → flush → delete → flush
- Upgrade: successful
- Direct reader cache: populated (not empty)
- Star tree: active (`terminated_early=true`)
- Aggregation: exact match before vs after

## How It Works

When `rewriteSegmentInfos()` encounters a segment with `docValuesGen != -1`:
1. Skips the codec switch (avoids the field number mismatch crash)
2. Adds star tree files to the segment's file set (prevents IndexWriter GC)
3. Rewrites `.si` to persist the expanded file set
4. `populateStarTreeDirectReaderCache()` creates a `StarTreeDirectReader` for the segment
5. `StarTreeQueryHelper.getStarTreeValues()` looks up the direct reader cache (Path 2) when the native codec path (Path 1) doesn't apply

## Why Codec Switch Fails for docValuesGen != -1

When soft deletes modify a committed segment, Lucene writes generation-based update files with UPDATED field numbers. The base `.dvm` inside `.cfs` retains ORIGINAL field numbers. Switching to `Composite912Codec` causes `Lucene90DocValuesProducer` to be initialized with updated field infos that don't match the base `.dvm` → `CorruptIndexException` or `softDeleteCount` assertion failure.

The direct reader cache bypasses this entirely — `StarTreeDirectReader` opens the star tree files (`.cid`, `.cim`, `.cidvd`, `.cidvm`) directly from the directory without going through the segment's codec.

## Files Involved

| File | Role |
|------|------|
| `StarTreeUpgradeService.rewriteSegmentInfos()` | Skips codec switch for `docValuesGen != -1` segments |
| `StarTreeDirectReader.java` | Opens star tree files directly from directory |
| `StarTreeQueryHelper.getStarTreeValues()` | Path 2: looks up direct reader cache by segment name |
| `IndexShard.populateStarTreeDirectReaderCache()` | Creates readers for non-codec-switched segments |
| `IndexShard.starTreeDirectReaderCache` | ConcurrentHashMap holding readers |
