# Design Spec: Refactor `IndexShard.publishMergedSegment` to take `exec.Segment`

**Consumer:** implementer subagent. Follow this exactly; do not improvise outside the scope.

---

## Goal

Replace the current DFA overload `IndexShard.publishMergedSegment(String segmentName, Collection<String> segmentFiles)` with a properly-typed `publishMergedSegment(org.opensearch.index.engine.exec.Segment)` that mirrors the Lucene `publishMergedSegment(SegmentCommitInfo)` flow — porting every guard, check, and lock semantic — and wire it into the DFA merge-completion path. Keep the design extendable to future `DataFormat`s.

---

## Scope of Changes (exhaustive)

### File 1: `server/src/main/java/org/opensearch/index/shard/IndexShard.java`

**1a. DELETE** the current DFA overload at lines ~2225–2260:
- `public void publishMergedSegment(String segmentName, Collection<String> segmentFiles)`
- `public MergedSegmentCheckpoint computeMergeSegmentCheckpoint(String segmentName, Collection<String> segmentFiles)`

**1b. ADD** a new DFA overload that takes `exec.Segment`:

```java
import org.opensearch.index.engine.exec.Segment;       // DFA segment, NOT the stats Segment
import org.opensearch.index.engine.exec.WriterFileSet;

/**
 * Publishes a merged segment checkpoint to replicas for the DFA (DataFormat Aware) engine path.
 *
 * <p>This is the DFA analog of {@link #publishMergedSegment(SegmentCommitInfo)}. Callers pass
 * the newly produced {@link org.opensearch.index.engine.exec.Segment} returned by the catalog
 * apply step; the method flattens its {@code dfGroupedSearchableFiles} into a single file list,
 * derives a segment identifier from the generation, and constructs a {@link MergedSegmentCheckpoint}
 * with the current catalog version.
 *
 * <p><b>Naming note:</b> the parameter type is {@code org.opensearch.index.engine.exec.Segment}
 * (the DFA catalog segment), NOT {@code org.opensearch.index.engine.Segment} (the stats-API DTO).
 * The two classes have the same simple name but live in different packages.
 *
 * @param mergedSegment the DFA segment record produced by {@code CatalogSnapshotManager.applyMergeResults}
 * @throws IOException if store metadata collection fails
 */
public void publishMergedSegment(Segment mergedSegment) throws IOException {
    assert mergedSegmentPublisher != null;
    mergedSegmentPublisher.publish(this, computeMergeSegmentCheckpoint(mergedSegment));
}

/**
 * DFA analog of {@link #computeMergeSegmentCheckpoint(SegmentCommitInfo)}. Mirrors the Lucene
 * version's lock-and-read pattern: acquires a catalog snapshot reference, collects file metadata
 * via {@link Store#getFileMetadata(Collection)}, and builds the checkpoint while the reference
 * is held.
 */
public MergedSegmentCheckpoint computeMergeSegmentCheckpoint(Segment mergedSegment) throws IOException {
    // Flatten DFA grouped file set -> flat list so Store#getFileMetadata can checksum each file.
    final Collection<String> segmentFiles = flattenSegmentFiles(mergedSegment);
    // Derive a stable string identifier for the merged segment. MergedSegmentCheckpoint uses this
    // only for equality/hashing/logging; downstream replica code does not use it for filesystem I/O.
    final String segmentName = segmentName(mergedSegment);
    try (GatedCloseable<CatalogSnapshot> snapshotCloseable = getCatalogSnapshot()) {
        final CatalogSnapshot catalogSnapshot = snapshotCloseable.get();
        final Map<String, StoreFileMetadata> segmentMetadataMap = store.getFileMetadata(segmentFiles);
        return new MergedSegmentCheckpoint(
            shardId,
            getOperationPrimaryTerm(),
            catalogSnapshot.getVersion(),
            segmentMetadataMap.values().stream().mapToLong(StoreFileMetadata::length).sum(),
            getIndexer().config().getCodec().getName(),
            segmentMetadataMap,
            segmentName
        );
    }
}

/** Flattens a {@link Segment}'s format-grouped file map into a flat collection of file names. */
private static Collection<String> flattenSegmentFiles(Segment mergedSegment) {
    return mergedSegment.dfGroupedSearchableFiles().values().stream()
        .flatMap(wfs -> wfs.files().stream())
        .collect(Collectors.toUnmodifiableList());
}

/** Derives a stable string identifier for a DFA merged segment from its generation. */
private static String segmentName(Segment mergedSegment) {
    return String.valueOf(mergedSegment.generation());
}
```

**1c. ADD** a shared guard method (extracted from `MergedSegmentWarmer.shouldWarm` so both Lucene and DFA paths can use the same decision logic):

```java
/**
 * Returns {@code true} if a merged segment of the given size should be published for replication.
 * Mirrors the guards used by the Lucene {@link MergedSegmentWarmer#shouldWarm} for segrep, so that
 * the DFA and Lucene merged-segment publication paths honor the same feature flags and thresholds.
 *
 * <p>Checks, in order:
 * <ol>
 *   <li>Cluster minimum node version &gt;= {@link Version#V_3_4_0}.</li>
 *   <li>Node setting {@code indices.replication.merges.warmer.enabled} is {@code true}.</li>
 *   <li>Segment size &gt;= {@code indices.replication.merges.warmer.min_segment_size_threshold}.</li>
 * </ol>
 *
 * @param segmentSizeBytes total size in bytes of the merged segment (sum across all data formats)
 * @return whether the segment qualifies for merged-segment pre-copy to replicas
 */
public boolean shouldPublishMergedSegment(long segmentSizeBytes) {
    final Version minNodeVersion = clusterService.state().nodes().getMinNodeVersion();
    if (Version.V_3_4_0.compareTo(minNodeVersion) > 0) {
        return false;
    }
    if (getRecoverySettings().isMergedSegmentReplicationWarmerEnabled() == false) {
        return false;
    }
    final long threshold = getRecoverySettings().getMergedSegmentWarmerMinSegmentSizeThreshold().getBytes();
    if (segmentSizeBytes < threshold) {
        logger.trace(
            "Skipping merged segment publish. Size {}B is less than the configured threshold {}B.",
            segmentSizeBytes, threshold
        );
        return false;
    }
    return true;
}
```

> **Implementer note:** Verify the exact field/method names on `RecoverySettings` by reading `MergedSegmentWarmer.shouldWarm` (line ~101 in `MergedSegmentWarmer.java`). Reuse the identical accessor calls to avoid drift. If `clusterService` is not already a field on `IndexShard`, source the min-node-version the same way `MergedSegmentWarmer` does (look at its constructor).

**1d. OPTIONAL but recommended (for guard-reuse):** Refactor `MergedSegmentWarmer.shouldWarm(SegmentCommitInfo)` to delegate the version/flag/threshold checks to `indexShard.shouldPublishMergedSegment(sci.sizeInBytes())`, keeping only the SCI-null-check guards local. If this widens the diff too much or touches too many Lucene-path files, SKIP this sub-step and leave `MergedSegmentWarmer` untouched — a follow-up cleanup ticket can dedupe later.

---

### File 2: `server/src/main/java/org/opensearch/index/engine/exec/coord/CatalogSnapshotManager.java`

**2a.** Change `applyMergeResults(MergeResult, OneMerge)` to RETURN the newly created `Segment`:

```java
// BEFORE:
public synchronized void applyMergeResults(MergeResult mergeResult, OneMerge oneMerge) throws IOException {
    ...
    Segment segmentToAdd = getSegment(mergeResult.getMergedWriterFileSet());
    ...
    commitNewSnapshot(segmentList);
}

// AFTER:
public synchronized Segment applyMergeResults(MergeResult mergeResult, OneMerge oneMerge) throws IOException {
    ...
    Segment segmentToAdd = getSegment(mergeResult.getMergedWriterFileSet());
    ...
    commitNewSnapshot(segmentList);
    return segmentToAdd;
}
```

**2b.** Update any callers affected by the return-type change:
- `DataFormatAwareEngine.applyMergeChanges` (primary caller — handled in File 3 below)
- Any test that calls `applyMergeResults` and ignores the return value is unaffected (Java auto-discards return values on expression statements).

---

### File 3: `server/src/main/java/org/opensearch/index/engine/DataFormatAwareEngine.java`

**3a.** Modify `applyMergeChanges(MergeResult, OneMerge)` to publish the merged segment. Preserve all existing logging and error handling; add a new publish step between `applyMergeResults` and `refreshListeners`.

```java
// BEFORE (line ~1367):
private void applyMergeChanges(MergeResult mergeResult, OneMerge oneMerge) {
    refreshLock.lock();
    try {
        catalogSnapshotManager.applyMergeResults(mergeResult, oneMerge);
        try (GatedCloseable<CatalogSnapshot> newSnapshotRef = catalogSnapshotManager.acquireSnapshot()) {
            refreshListeners(true, newSnapshotRef.get());
        }
    } catch (Exception ex) {
        try {
            logger.error(() -> new ParameterizedMessage("Merge failed while registering merged files in Snapshot"), ex);
            failEngine("Merge failed while registering merged files in Snapshot", ex);
        } catch (Exception inner) {
            ex.addSuppressed(inner);
        }
        throw new MergeFailedEngineException(shardId, ex);
    } finally {
        refreshLock.unlock();
    }
}

// AFTER:
private void applyMergeChanges(MergeResult mergeResult, OneMerge oneMerge) {
    refreshLock.lock();
    try {
        Segment mergedSegment = catalogSnapshotManager.applyMergeResults(mergeResult, oneMerge);
        // Publish the merged segment for replication BEFORE listeners make readers visible, mirroring
        // the Lucene MergedSegmentWarmer semantics. Failures here must not fail the merge itself.
        maybePublishMergedSegment(mergedSegment);
        try (GatedCloseable<CatalogSnapshot> newSnapshotRef = catalogSnapshotManager.acquireSnapshot()) {
            refreshListeners(true, newSnapshotRef.get());
        }
    } catch (Exception ex) {
        try {
            logger.error(() -> new ParameterizedMessage("Merge failed while registering merged files in Snapshot"), ex);
            failEngine("Merge failed while registering merged files in Snapshot", ex);
        } catch (Exception inner) {
            ex.addSuppressed(inner);
        }
        throw new MergeFailedEngineException(shardId, ex);
    } finally {
        refreshLock.unlock();
    }
}

/**
 * Publishes the merged segment to replicas if the shard has a configured publisher and the segment
 * passes the shared merged-segment replication guards. A failure in publish is logged but does NOT
 * fail the merge (matching Lucene warmer's {@code catch (Throwable t)} semantics).
 */
private void maybePublishMergedSegment(Segment mergedSegment) {
    if (indexShard.getMergedSegmentPublisher() == null) {
        return;
    }
    try {
        long segmentSizeBytes = computeSegmentSizeBytes(mergedSegment);
        if (indexShard.shouldPublishMergedSegment(segmentSizeBytes) == false) {
            return;
        }
        logger.trace("Publishing merged segment generation={} size={}B", mergedSegment.generation(), segmentSizeBytes);
        indexShard.publishMergedSegment(mergedSegment);
    } catch (Throwable t) {
        // Match MergedSegmentWarmer.warm() error handling: log and continue; never fail the merge.
        logger.warn(
            () -> new ParameterizedMessage("Failed to publish merged segment generation={}. Continuing.",
                mergedSegment.generation()),
            t
        );
    }
}

/** Total size in bytes across all DataFormat WriterFileSets of a merged segment. */
private static long computeSegmentSizeBytes(Segment mergedSegment) {
    return mergedSegment.dfGroupedSearchableFiles().values().stream()
        .mapToLong(WriterFileSet::getTotalSize)
        .sum();
}
```

**3b.** Imports to add in `DataFormatAwareEngine.java`:
- `org.opensearch.index.engine.exec.Segment`
- `org.opensearch.index.engine.exec.WriterFileSet`
- (verify `ParameterizedMessage` is already imported; it is used elsewhere in the class)

**3c.** Add a public accessor `IndexShard.getMergedSegmentPublisher()` **only if one does not already exist**. Implementation:
```java
@Nullable
public MergedSegmentPublisher getMergedSegmentPublisher() {
    return mergedSegmentPublisher;
}
```
If such an accessor already exists (check by grep before adding), reuse it.

---

## Guards Mirrored from Lucene Path

| Guard | Lucene location | DFA location |
|---|---|---|
| `mergedSegmentPublisher != null` | `IndexShard.publishMergedSegment` assert (line 2185) | Preserved in new DFA `publishMergedSegment(Segment)` via same assert; additionally guarded in caller via `getMergedSegmentPublisher() == null` early-return |
| Min node version ≥ V_3_4_0 | `MergedSegmentWarmer.shouldWarm` | `IndexShard.shouldPublishMergedSegment` |
| `isMergedSegmentReplicationWarmerEnabled()` | `MergedSegmentWarmer.shouldWarm` | `IndexShard.shouldPublishMergedSegment` |
| Size threshold check | `MergedSegmentWarmer.shouldWarm` | `IndexShard.shouldPublishMergedSegment` |
| Segment identity null checks | `MergedSegmentWarmer.shouldWarm` (`sci.info == null`, `sci.info.dir == null`) | N/A for DFA — `Segment` is a record with non-null file map by construction |
| Throwable caught, merge not failed | `MergedSegmentWarmer.warm` catch block | `DataFormatAwareEngine.maybePublishMergedSegment` catch block |

---

## Extensibility Notes (for future DataFormats)

1. The new `publishMergedSegment(Segment)` signature is format-agnostic by construction — `Segment.dfGroupedSearchableFiles()` already keys by `DataFormat` name, so any future format that produces a `WriterFileSet` automatically flows through.
2. `Segment` is `Writeable`, so it could itself be transported across nodes in future extensions. For now, the transport still uses `MergedSegmentCheckpoint`, but future work can key off the richer `Segment` type without another refactor.
3. The size-threshold guard uses a single aggregated size across all formats. This is intentionally format-agnostic — do NOT per-format-gate.

---

## Known Deferred Items (NOT in scope for this refactor)

1. **Checksum divergence for Lucene files embedded in DFA segments.** `Store.getFileMetadata(Collection<String>)` stamps `Version.CURRENT.luceneVersion` on all files and has no `.si` special case; `Store.getSegmentMetadataMap(SegmentInfos)` has Lucene-version-aware handling. For pure-DFA segments this is a non-issue; for mixed-content segments the implementer should log this as a follow-up concern but not try to fix it here.
2. **Codec field semantic meaninglessness for DFA.** `MergedSegmentCheckpoint.codec` is populated from `getIndexer().config().getCodec().getName()` (a Lucene codec name) for DFA too. The replica does not appear to use it for validation, so leave as-is. Flag as follow-up.
3. **Integration test coverage.** No existing test exercises DFA merged-segment publishing. Add a unit test for `computeMergeSegmentCheckpoint(Segment)` (golden checkpoint assertions) but do NOT add a full cluster IT in this pass.
4. **Lucene warmer dedup (File 1d).** Refactoring `MergedSegmentWarmer.shouldWarm` to delegate to the new shared guard is recommended but may be skipped if it broadens diff scope.

---

## Required Tests (minimum)

Add a test class if one doesn't exist, e.g., `IndexShardMergedSegmentPublishTests.java` or extend the existing `IndexShardTests`:

1. `testPublishMergedSegment_segmentNameDerivedFromGeneration` — construct a `Segment` with known generation, call `computeMergeSegmentCheckpoint`, assert `checkpoint.getSegmentName()` equals `String.valueOf(generation)`.
2. `testPublishMergedSegment_filesFlatteningAcrossFormats` — construct a `Segment` with two `WriterFileSet`s (two different `DataFormat` keys), assert the resulting `metadataMap` contains all files from both.
3. `testShouldPublishMergedSegment_featureFlagDisabled` — disable the setting, assert `shouldPublishMergedSegment` returns false.
4. `testShouldPublishMergedSegment_sizeBelowThreshold` — assert returns false.
5. `testApplyMergeChanges_publishesMergedSegmentOnce` — verify `DataFormatAwareEngine.applyMergeChanges` calls `indexShard.publishMergedSegment(Segment)` exactly once with the `Segment` returned by `applyMergeResults`. Use a mock `MergedSegmentPublisher`.

---

## Non-Goals (do NOT do these)

- Do NOT modify `MergedSegmentCheckpoint` or `ReplicationCheckpoint`.
- Do NOT modify the Lucene `publishMergedSegment(SegmentCommitInfo)` or `computeMergeSegmentCheckpoint(SegmentCommitInfo)` — leave them verbatim.
- Do NOT strip existing comments, Javadoc, or log statements from any file you edit.
- Do NOT remove the `(String, Collection<String>)` DFA helper and then re-add the same shape as a private helper — DELETE it cleanly, everything the new code needs is inlined in `computeMergeSegmentCheckpoint(Segment)`.
- Do NOT commit or create a CR — the user will review the diff first.

---

## Validation

After implementation, run:
```
./gradlew :server:compileJava 2>&1 | tail -n 40
./gradlew :server:test --tests "*IndexShard*" --tests "*MergedSegment*" --tests "*DataFormatAwareEngine*" --tests "*CatalogSnapshotManager*" 2>&1 | tail -n 80
./gradlew spotlessJavaCheck 2>&1 | tail -n 20
```
Capture output for each. If any compile error or test failure, fix before declaring done.
