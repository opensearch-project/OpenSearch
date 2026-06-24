# Investigation: NoSuchFileException `_0.cidvd` with Soft Deletes

## CONFIRMED: Root Cause

**Verified experimentally** (minimal 100-doc reproduction):

1. Segment `_0` is a compound segment with soft deletes (`docValuesGen != -1`)
2. Files on disk: `_0.cfe, _0.cfs, _0.si, _0_1.fnm, _0_1_Lucene90_0.dvd, _0_1_Lucene90_0.dvm`
3. `rewriteSegmentInfos()` adds `_0.cidvd` (and `.cid/.cim/.cidvm`) to `_0`'s file set → commits `segments_6`
4. `_0.cidvd` does NOT exist on disk (Phase 1 hasn't run yet)
5. Engine reopens → `DirectoryReader.open(indexWriter)` fails with `NoSuchFileException: _0.cidvd`

**The error source is `InternalEngine.createReaderManager()` line 631**: `DirectoryReader.open(documentIndexWriter.getAccumulatingIndexWriter())` which opens NRT readers on the IndexWriter. During NRT reader construction, Lucene opens each segment and reads its files. Since `_0.cidvd` is in `_0`'s file set but doesn't exist, the open fails.

**Why no-deletes passes**: In the no-deletes test, segments are compound files without generation files (`docValuesGen == -1`). Lucene's NRT reader construction for compound segments without doc values updates apparently doesn't trigger the same file-existence validation for extra files in the file set. The exact mechanism needs further investigation, but the compound-file + no-generation path behaves differently.

## Conclusion

**Forward declaration (Option A) is incompatible with soft-delete segments.** You cannot add files to `SegmentInfo.files()` that don't exist on disk — Lucene will try to open them during NRT reader construction.

## Recommended Next Step

Revert to the hybrid approach's execution order: **Build files first, then commit references, then reopen engine.** This is what the working hybrid approach does (ReadOnlyEngine swap pattern). The challenge remains: how to prevent IndexFileDeleter from deleting the files during the engine close before rewriteSegmentInfos.

---

## Root Cause Analysis

### What is segment `_0`?

Segment `_0` is the **first data segment** in the index. In the soft-deletes test:
1. Docs are indexed → flush → segment `_0` created with `Lucene104Codec`
2. Docs are deleted → soft deletes applied to segment `_0`
3. Flush after delete → `_0` now has `docValuesGen != -1`, with generation files like `_0_1.fnm`, `_0_1_Lucene90_0.dvd/dvm`

`_0` is a **pre-existing segment**, NOT a new one created after the mapping update.

---

### The Forward Declaration Flow

The current implementation does:

```
1. getCandidateSegmentNames() → finds _0 (among others)
2. blockOperations()
3.   flush(force=true)              — commits pending ops
4.   close engine                   — IndexWriter releases lock
5.   rewriteSegmentInfos()         — adds _0.cidvd to _0's file set in segments_N+1
6.   reopen engine                  — new IndexWriter opens on segments_N+1
7.   skipTranslogRecovery()
8.   refresh("star-tree-upgrade-warmup")
9. unblockOperations()
10. buildStarTreeDataForSegments() — Phase 1 builds actual files in background
```

### The Problem: Forward-declared files don't exist when engine reopens

At step 6, the engine reopens on `segments_N+1` which declares `_0.cidvd` (and `.cid`, `.cim`, `.cidvm`) in segment `_0`'s file set — but these files **do not yet exist on disk** (Phase 1 hasn't run).

### Why this fails with soft deletes but not without

**Hypothesis 1: `IndexWriter.filesExist()` assertion (JUnit tests only)**

In Lucene 10.4, `IndexWriter` has an assertion `assert filesExist(toSync)` that verifies all files in a segment's file set exist on disk. This runs ONLY with assertions enabled (JUnit tests use `-ea`).

- **No-deletes test**: Runs as a bash script against a live cluster → no assertions → `filesExist()` never triggers
- **Soft-deletes test**: Runs as a JUnit integration test → assertions enabled → `filesExist()` catches missing `_0.cidvd`

If both are bash scripts, proceed to Hypothesis 2.

**Hypothesis 2: Composite codec reads during engine warm-up**

After the engine reopens (step 6), the codec service is built from the CURRENT `MapperService` which now has composite field types. The `newEngineConfig()` creates a codec service that returns `Composite104Codec`.

When `refresh("star-tree-upgrade-warmup")` (step 8) opens NRT readers, the IndexWriter needs to read existing segments. For the `SoftDeletesRetentionMergePolicy` to evaluate retention, it opens readers on segments with pending deletes.

In the **no-deletes case**: No soft deletes → no retention evaluation → no reader opened for `_0` → no `Composite912DocValuesReader` triggered.

In the **soft-deletes case**: Segment `_0` has `docValuesGen != -1`. When the IndexWriter opens, it may apply the retention merge policy which opens a reader on `_0`. If this reader path uses the **writer's codec** (`Composite104Codec`) instead of the **segment's stored codec** (`Lucene104Codec`), then `Composite912DocValuesFormat.fieldsProducer()` is called, which creates `Composite912DocValuesReader`, which tries to open `_0.cim`/`_0.cidvd`...

But wait — `Composite912DocValuesReader` has a graceful fallback (`starTreeFilesExist = false`). So if the error occurs HERE, the fallback should work.

**Hypothesis 3 (MOST LIKELY): `IndexWriter` file existence check in `IndexFileDeleter`**

When `IndexWriter` opens in APPEND mode, `IndexFileDeleter` enumerates ALL referenced files across all segment commit infos. It verifies every referenced file exists. This is NOT assertion-only — it's part of `IndexFileDeleter.init()` which throws `NoSuchFileException` if a referenced file is missing.

```java
// In IndexFileDeleter (Lucene):
for (String fileName : segmentInfos.files(true)) {
    if (!directory.fileExists(fileName)) {
        throw new NoSuchFileException(fileName);
    }
}
```

In the **no-deletes case**:
- The bash test runs without the new forward-declaration code (uses the old hybrid approach)
- OR the `SegmentInfo.files()` for compound-file segments doesn't include the forward-declared files because compound segments report files differently

In the **soft-deletes case**:
- Segments with `docValuesGen != -1` are NOT compound files (they have generation files outside `.cfs`)
- `SegmentInfo.files()` returns the full expanded file set INCLUDING the forward-declared `.cidvd`
- `IndexFileDeleter` checks these files → `_0.cidvd` doesn't exist → `NoSuchFileException`

**Hypothesis 4 (ALTERNATIVE): New segment `_0` created by flush with Composite104Codec**

If the `flush(force=true)` at step 3 creates a NEW segment (because there are tombstone documents from deletes in the buffer), that segment would use `Composite104Codec`. The `Composite912DocValuesWriter` creates `.cid`, `.cim`, `.cidvd`, `.cidvm` output files in its constructor for EVERY segment.

But the `Composite912DocValuesWriter` writes star tree data only if `segmentHasCompositeFields` is true AND all composite fields are present in the segment. A tombstone segment wouldn't have the star tree dimension fields, so no star tree data is built. However, the output files ARE created (with headers + empty EOF marker + footers) — they exist on disk.

In this scenario, `_0` would be an existing Lucene104Codec segment whose name happens to collide. But segment names are monotonically increasing in IndexWriter, so if `_0` already exists, a new segment can't be named `_0`.

---

## Why the Hybrid Approach Didn't Have This Problem

The hybrid approach (documented in `QUIP_HYBRID_UPGRADE_ARCHITECTURE.md`) worked because:

1. **Engine was swapped to ReadOnlyEngine** (Swap 1) — no IndexWriter holding a lock or checking files
2. **Phase 1 ran FIRST** — star tree files were built BEFORE any commit referenced them
3. **Phase 2 committed segments_N+1** — by this time, `_0.cidvd` already existed on disk
4. **Engine was swapped back** (Swap 2) — new InternalEngine opened on segments_N+1 where ALL referenced files exist

The execution order was:
```
Hybrid:     Build files → Commit references → Open engine
Forward:    Commit references → Open engine → Build files (LATER)
```

The hybrid approach guarantees that **files exist before being referenced**. The forward-declaration approach intentionally violates this invariant, betting that `Composite912DocValuesReader`'s graceful fallback handles the missing files. But it doesn't account for `IndexWriter`/`IndexFileDeleter` also checking file existence.

---

## The Core Design Flaw

The forward-declaration pattern has a fundamental contradiction:

1. It adds file names to `SegmentInfo.files()` so that `IndexFileDeleter` won't GC them later
2. But `IndexFileDeleter` (or `IndexWriter` assertions) also CHECKS that those files exist when it initializes

You can't forward-declare files that don't exist AND have `IndexWriter` open successfully on that commit.

---

## Possible Fixes

### Fix A: Revert to Hybrid Approach for Soft-Delete Segments

Keep the forward-declaration for clean segments (no soft deletes) but use the old hybrid approach (ReadOnlyEngine swap, build-then-commit) for segments with `docValuesGen != -1`.

**Pro**: Proven to work at scale
**Con**: Re-introduces the full engine swap complexity, longer block window

### Fix B: Create Empty Star Tree Files Before Forward Declaration

Before `rewriteSegmentInfos()`, create minimal valid (but empty) star tree files for each candidate segment:
- `_0.cid` — header + footer only
- `_0.cim` — header + EOF marker (-1) + footer only
- `_0.cidvd` — Lucene90 doc values format header + footer
- `_0.cidvm` — Lucene90 doc values format header + footer

Then `rewriteSegmentInfos()` adds them to file sets. When engine reopens, files exist → no error. `Composite912DocValuesReader` opens them, finds no star tree metadata → returns empty (graceful).

Phase 1 then OVERWRITES these files with actual star tree data.

**Pro**: Preserves the forward-declaration architecture, minimal code change
**Con**: Need to ensure atomic overwrite works (Lucene's Directory might not allow overwrite — need `deleteFile` + `createOutput`). Race condition if IndexWriter reads during overwrite.

### Fix C: Build Files Inside the Block, Reference After (Minimal Hybrid)

Combine the approaches:
```
1. blockOperations()
2.   flush(force=true)
3.   close engine
4.   buildStarTreeDataForSegments()    — files now exist
5.   rewriteSegmentInfos()             — safe to reference
6.   reopen engine                     — all files exist
7. unblockOperations()
```

**Pro**: Simple, correct, no file-existence issues
**Con**: The entire star tree build happens while writes are blocked (same as original approach). Defeats the purpose of "minimal block."

### Fix D: Don't Add Files to SegmentInfo — Use Separate Protection Mechanism

Don't add star tree files to `SegmentInfo.files()`. Instead, use a different mechanism to protect them from `IndexFileDeleter`:
- Use `SidecarProtectedDirectory` (as in the sidecar approach) to intercept `deleteFile()` calls
- Or use `IndexWriter.setLiveCommitData()` to record file names (doesn't prevent deletion though)

This is essentially the "Sidecar" approach from `SIDECAR_ARCHITECTURE.md`.

**Pro**: No `SegmentInfo` modification needed, no IndexWriter file-check issues
**Con**: More complex infrastructure (FilterDirectory, ref counting)

### Fix E: Skip Forward Declaration for Soft-Delete Segments

Only forward-declare (and add to file set) segments that have `docValuesGen == -1`. For segments with soft deletes, use the sidecar/cache approach without adding to file sets:
```
getCandidateSegmentNames() → separate into two lists:
  - cleanSegments (docValuesGen == -1): use forward-declaration + rewriteSegmentInfos
  - dirtySegments (docValuesGen != -1): build files, serve via DirectReader cache, 
    don't add to SegmentInfo.files(), protect via SidecarProtectedDirectory
```

**Pro**: Best of both worlds — fast path for clean segments, safe path for soft-delete segments
**Con**: Two code paths to maintain

---

## Recommended Fix

**Fix B (Create Empty Star Tree Files)** is the simplest change that preserves the forward-declaration architecture:

```java
// In upgradeToStarTree(), before rewriteSegmentInfos:
for (String segName : segmentsToForwardDeclare) {
    StarTreeUpgradeService.createEmptyStarTreeFiles(store().directory(), segName, segmentId);
}
StarTreeUpgradeService.rewriteSegmentInfos(store().directory(), segmentsToForwardDeclare);
```

The `createEmptyStarTreeFiles` method writes minimal valid files that `Composite912DocValuesReader` will parse as "no star tree data" (empty metadata → `starTreeFilesExist` effectively false after parsing).

Phase 1 then deletes and recreates these files with actual data. The key requirement is that Phase 1 must NOT run while any reader has the empty files open (which is safe because Phase 1 opens its own `DirectoryReader`).

**However**, if `Directory.createOutput` doesn't allow overwriting existing files (it throws `FileAlreadyExistsException` in most Lucene `Directory` implementations), you'd need `directory.deleteFile()` + `directory.createOutput()` in Phase 1, which means the file momentarily doesn't exist → race condition.

**Therefore, Fix C (build inside block)** is the SAFEST fix if the performance regression is acceptable. If not, **Fix E (hybrid path per segment type)** gives the best balance.

---

## Key Insight: Why the No-Deletes Test Passes

The no-deletes test (0.227s) runs as a **bash/curl script** against a live OpenSearch cluster. In this context:
1. JVM assertions are DISABLED (production mode)
2. `IndexWriter.filesExist()` assertion doesn't fire
3. `IndexFileDeleter` in Lucene 10.4 may not do the strict existence check in non-assertion mode OR the compound-file segments handle this differently

The soft-deletes test that FAILS is likely either:
- A JUnit integration test (assertions enabled)
- OR running against a cluster started with `-ea` JVM flag
- OR Lucene 10.4's `IndexFileDeleter.init()` does a non-assertion existence check that compound-file segments bypass (compound segments list `.cfs/.cfe/.si` in files(), not individual data files)

**If both tests are bash scripts**: The difference is that no-delete segments are compound files (`.cfs`). The `.cfs` contains all segment data internally. `SegmentInfo.files()` for a compound segment returns `{_0.cfs, _0.cfe, _0.si}`. When we add `_0.cidvd` to this set, IndexFileDeleter still checks it. But... compound segments' Directory is a `CompoundDirectory` that wraps the base directory. The check might succeed for compound entries and fail for non-compound entries. Need to verify.

**Most likely resolution**: The actual difference is that segments with soft deletes have `docValuesGen != -1` which means they have extra generation files listed in `SegmentCommitInfo.files()` (which includes both `SegmentInfo.files()` AND generation-specific files). The `IndexFileDeleter` checks `SegmentCommitInfo.files()`, not just `SegmentInfo.files()`. The forward-declared files ARE in `SegmentInfo.files()` and thus in `SegmentCommitInfo.files()`.

---

## Verification Steps

1. **Confirm the error source**: Add logging to identify whether the `NoSuchFileException` comes from:
   - `IndexWriter` init (wrap engine creation with try-catch, log full stack)
   - `IndexFileDeleter.init()`
   - `Composite912DocValuesReader` (should not, has fallback)
   - Somewhere else

2. **Test Fix B**: Create `StarTreeUpgradeService.createEmptyStarTreeFiles()`:
   ```java
   public static void createEmptyStarTreeFiles(Directory dir, String segName, byte[] segId) {
       // Create minimal .cim file: header + EOF marker (-1L) + footer
       String cimFile = IndexFileNames.segmentFileName(segName, "", META_EXTENSION);
       try (IndexOutput out = dir.createOutput(cimFile, IOContext.DEFAULT)) {
           CodecUtil.writeIndexHeader(out, META_CODEC_NAME, VERSION_CURRENT, segId, "");
           out.writeLong(-1L); // EOF marker
           CodecUtil.writeFooter(out);
       }
       // Create minimal .cid, .cidvd, .cidvm files similarly
   }
   ```

3. **Verify with soft-deletes JUnit test** (e.g., `StarTreeUpgradeMixedSegmentsIT`)

---

## Summary

| Aspect | Hybrid (Working) | Forward Declaration (Broken) |
|--------|-----------------|--------------------------|
| File creation order | Build → Commit → Open engine | Commit → Open engine → Build |
| File existence guarantee | ✅ Files exist when referenced | ❌ Files don't exist when referenced |
| IndexWriter validation | Passes (files exist) | Fails (files missing) |
| Soft delete handling | No issue (ReadOnlyEngine bypass) | Triggers file check failure |
| Write blocking | ~23s (full build) | ~100ms intended (but fails) |
