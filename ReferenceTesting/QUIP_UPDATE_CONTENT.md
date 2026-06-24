# Context Transfer — Star Tree Upgrade: Delete Support + Parallel Build

## What This Chat Accomplished

This session investigated and resolved delete support for the per-segment star tree upgrade, and added parallel star tree building across segments.

---

## Starting Point

The per-segment star tree upgrade worked perfectly for indices WITHOUT deletes. When deletes existed, the upgrade crashed during engine restart with file naming errors. The user asked to investigate and fix this.

## Branch

`hybrid_approach` on `github.com/Ashu-tosh27/OpenSearch`

## Commits Made

1. `61e0d00` — Parallel star tree build + delete support verification (code + tests)
2. `9529396` — Update delete support investigation doc
3. `3b36c4e` — Update QUIP hybrid architecture doc

---

## The Delete Problem (5 Cascading Errors)

When an index has had document deletions (soft deletes), Lucene creates generation-based update files (`_0_1.fnm`, `_0_1_Lucene90_0.dvd/dvm`) with UPDATED field numbers. The base doc values file inside `.cfs` retains ORIGINAL field numbers. Switching the codec to `Composite912Codec` on these segments causes a cascade of failures:

### Error 1: Missing file sets
```
no_such_file_exception: _0_1.fnm
```
New `SegmentCommitInfo` had empty `dvUpdatesFiles`/`fieldInfosFiles` maps.
**Fix**: Copy them via `setFieldInfosFiles()` + `setDocValuesUpdatesFiles()`.

### Error 2: ReadOnlyEngine translog assertion
```
AssertionError: multiple translogs instances should not be opened at the same time
```
ReadOnlyEngine tried to open translog with active delete state.
**Fix**: Pass pre-captured `SeqNoStats`/`TranslogStats` to ReadOnlyEngine constructor.

### Error 3: Star tree file suffix mismatch
```
no_such_file_exception: _0_1.cim
```
`Composite912DocValuesReader` used `readState.segmentSuffix` (generation value) for star tree files.
**Fix**: Hardcode empty suffix `""` for star tree file operations.

### Error 4: Field number mismatch
```
CorruptIndexException: Invalid field number: 1 (resource=_0.cfs [slice=_0_Lucene90_0.dvm])
```
`Lucene90DocValuesProducer` initialized with UPDATED field infos that don't match base `.dvm`.
**Fix attempted**: Read ORIGINAL field infos from `.cfs`. Resolved this error but exposed Error 5.

### Error 5: softDeleteCount assertion (FUNDAMENTAL BLOCKER)
```
AssertionError: softDeleteCount doesn't match 767 != 0
```
`PendingSoftDeletes` reads `__soft_deletes` through `CodecReader` → `SegmentDocValuesProducer` → our producer. The `__soft_deletes` field doesn't exist in original field infos → null → counts 0.

**What we tried**: Try-catch null guards, field number mapping wrapper, reading original field infos from `.cfs`, using `Composite104Codec` instead of `Composite912Codec`. None resolved Error 5.

**Root cause**: `SegmentDocValuesProducer` routes by field number from UPDATED field infos. Our producer (initialized with original field infos) has different field numbers. The routing is incompatible and cannot be fixed without modifying Lucene internals.

---

## The Resolution: Hybrid Approach

Instead of trying to make the codec switch work for segments with soft deletes:

1. **Skip codec switch** for segments where `docValuesGen != -1` in `rewriteSegmentInfos()`
2. **Still build star tree data** for those segments
3. **Add star tree files to segment's file set** (prevents IndexWriter GC)
4. **Rewrite `.si` file** to persist expanded file set
5. **Populate `StarTreeDirectReader` cache** after engine restart
6. **`StarTreeQueryHelper.getStarTreeValues()` Path 2** looks up direct reader cache by segment name

The `StarTreeDirectReader` opens `.cid`/`.cim`/`.cidvd`/`.cidvm` directly from the directory, completely bypassing the segment's codec.

---

## Parallel Star Tree Build

### What Changed

`StarTreeUpgradeService.buildStarTreeDataForSegments()` now uses a thread pool to build star tree data for multiple segments concurrently.

### Thread Count

`Math.min(eligibleSegments.size(), Runtime.getRuntime().availableProcessors())` — never more threads than segments, never more than CPU cores.

### Performance

| Dataset | Segments | Threads | Sequential | Parallel | Speedup |
|---------|----------|---------|-----------|----------|---------|
| 100k docs | 6 | 6 | 5.4s | 1.8s | 3x |
| 1M + 50k deletes | 5 | 5 | ~27s | 10s | 2.7x |
| 1M + 50k deletes | 7 | 7 | ~27s | 7.5s | 3.6x |

---

## Verification

### 1M Scale Test (test_1m_full_verification.sh)

1M docs + 50k deletes. Logs confirmed BOTH paths exercised:
- 7 segments with `docValuesGen=1` → direct reader cache path
- Remaining segments with `docValuesGen=-1` → native codec switch path

Results:
- Upgrade: 7.5s, successful
- Aggregation: ALL VALUES MATCH (before == after)
- `terminated_early: True` — star tree active
- 0 wrong values during concurrent reads
- Doc count: 950,000 (correct)

### Java Integration Test (StarTreeUpgradeWithDocValuesGenIT)

Deterministically forces `docValuesGen != -1` via: ingest → flush → delete → flush.
- Verifies topology setup
- Runs full upgrade API
- Asserts: upgrade succeeds, direct reader cache populated, star tree active, aggregation exact match

### Reads During Upgrade with Deletes

100k docs + 5k deletes, concurrent reads during upgrade:
- 14/16 reads returned exact correct values
- 2/16 reads returned empty (engine swap window)
- 0 reads returned wrong values
- Deleted docs never included in any response

---

## Files Changed (This Session)

| File | Change |
|------|--------|
| `StarTreeUpgradeService.java` | Parallel build via `ExecutorService` + `ConcurrentHashMap` |
| `StarTreeUpgradeWithDocValuesGenIT.java` | NEW — Java integration test for `docValuesGen != -1` |
| `DELETE_SUPPORT_INVESTIGATION.md` | Full error chain + resolution documentation |
| `HYBRID_ARCHITECTURE.md` | Added parallel build + delete verification sections |
| `QUIP_HYBRID_UPGRADE_ARCHITECTURE.md` | Added parallel build + delete verification sections |
| `test_1m_deletes.sh` | NEW — 1M test with deletes |
| `test_1m_full_verification.sh` | NEW — 1M test with correctness comparison + concurrent reads |
| `test_100k_deletes.sh` | NEW — 100k test with deletes |
| `test_forced_docvaluesgen.sh` | NEW — Forces docValuesGen topology via REST |
| `test_forced_docvaluesgen_v2.sh` | NEW — Fixed version with numeric dimensions |
| `test_reads_correctness_with_deletes.sh` | NEW — Verifies exact values during upgrade |
| `test_reads_during_upgrade_with_deletes.sh` | NEW — Concurrent reads during upgrade |
| `test_reads_after_deletes.sh` | NEW — Reads after deletes on upgraded index |

---

## Key Insights for Future Reference

1. **`docValuesGen != -1` is hard to reproduce via REST API** — OpenSearch creates tombstone segments for deletes, not in-place DV updates. It only happens naturally during Lucene's internal merge operations with large datasets. The Java integration test forces it deterministically via: ingest → flush → delete → flush.

2. **The codec switch is fundamentally incompatible with `docValuesGen != -1`** — The field number mismatch between original and updated field infos cannot be resolved at the `fieldsProducer()` level. The hybrid approach (skip codec switch + direct reader cache) is the correct solution.

3. **`Composite912DocValuesReader` must use empty suffix for star tree files** — When `fieldInfosGen != -1`, Lucene sets `readState.segmentSuffix` to the generation value. Star tree files are always written with empty suffix.

4. **ReadOnlyEngine needs pre-captured stats when deletes exist** — Without them, it tries to open the translog which has active delete state → assertion failure.

5. **The `append_only` setting blocks post-upgrade deletes** — After upgrade, the index becomes append-only (required for composite indices). Deletes BEFORE upgrade are supported; deletes AFTER upgrade are blocked by the setting.

6. **Parallel build is safe** — Each segment's build reads from different segment files and writes to different output files. The only shared resource is the `Directory` for file creation, which is thread-safe in Lucene.

---

## What's NOT Done

- The Quip document at `https://quip-amazon.com/b1BzAJNwy7CQ` needs manual updating (auth expired). The content to add is described above.
- Bug #12 (merge path soft deletes) is still open — documented in `HYBRID_ARCHITECTURE.md`.
