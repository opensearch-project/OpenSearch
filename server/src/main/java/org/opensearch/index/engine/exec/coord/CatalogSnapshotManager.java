/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.concurrent.GatedConditionalCloseable;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.merge.OneMerge;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.CatalogSnapshotLifecycleListener;
import org.opensearch.index.engine.exec.CommitFileManager;
import org.opensearch.index.engine.exec.FileDeleter;
import org.opensearch.index.engine.exec.FilesListener;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.ShardPath;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the lifecycle of {@link CatalogSnapshot} instances for the composite multi-format engine
 * and coordinates file deletion through an internally owned {@link IndexFileDeleter}.
 *
 * <p>Tracks all live snapshots in a map keyed by generation. When a snapshot's reference count reaches
 * zero (via {@link #decRefAndMaybeDelete}), it is automatically removed from the map and its files
 * are cleaned up through the deleter.</p>
 *
 * <p>The write path (commit) is single-threaded (refresh is serialized per shard), while the read
 * path (acquireSnapshot) is safe for concurrent access via volatile reads and {@code tryIncRef}.</p>
 */
@ExperimentalApi
public class CatalogSnapshotManager implements Closeable {

    private static final Logger logger = LogManager.getLogger(CatalogSnapshotManager.class);

    private volatile CatalogSnapshot latestCatalogSnapshot;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<Long, CatalogSnapshot> catalogSnapshotMap = new ConcurrentHashMap<>();
    private final IndexFileDeleter indexFileDeleter;
    private final CatalogSnapshotDeletionPolicy deletionPolicy;
    private final List<CatalogSnapshotLifecycleListener> snapshotListeners;

    /**
     * Creates a new {@link DataformatAwareCatalogSnapshot} for use in tests.
     * <p>
     * This is a convenience method for test code that cannot access the package-private
     * {@link DataformatAwareCatalogSnapshot} constructor. The returned snapshot is NOT managed
     * by a {@link CatalogSnapshotManager} — its reference count is not tracked, and callers
     * must not rely on ref-counting semantics.
     */
    public static CatalogSnapshot createInitialSnapshot(
        long id,
        long generation,
        long version,
        List<Segment> segments,
        long lastWriterGeneration,
        Map<String, String> userData
    ) {
        return new DataformatAwareCatalogSnapshot(id, generation, version, segments, lastWriterGeneration, userData);
    }

    /**
     * Constructs a new CatalogSnapshotManager from committed snapshots.
     * <p>
     * Typically called after {@link org.opensearch.index.engine.exec.commit.Committer#listCommittedSnapshots()}
     * discovers the surviving commits on startup. For fresh indices, the caller creates a single initial
     * snapshot and passes it as a singleton list.
     *
     * @param committedSnapshots   the committed snapshots, ordered oldest first; must not be empty
     * @param deletionPolicy       decides which committed snapshots to keep
     * @param fileDeleter          per-format deleters for actual file deletion
     * @param filesListeners       per-format listeners notified on file add/delete
     * @param snapshotListeners    listeners notified on snapshot deletion
     * @param shardPath            for orphan cleanup on init, or null if not needed
     * @param commitFileManager    manager for commit-level files (e.g., segments_N), or null if not needed
     */
    public CatalogSnapshotManager(
        List<CatalogSnapshot> committedSnapshots,
        CatalogSnapshotDeletionPolicy deletionPolicy,
        FileDeleter fileDeleter,
        Map<String, FilesListener> filesListeners,
        List<CatalogSnapshotLifecycleListener> snapshotListeners,
        ShardPath shardPath,
        CommitFileManager commitFileManager
    ) throws IOException {
        if (committedSnapshots.isEmpty()) {
            throw new IllegalArgumentException("committedSnapshots must not be empty");
        }
        this.deletionPolicy = deletionPolicy;
        this.snapshotListeners = snapshotListeners;
        this.latestCatalogSnapshot = committedSnapshots.getLast();
        for (CatalogSnapshot cs : committedSnapshots) {
            catalogSnapshotMap.put(cs.getGeneration(), cs);
        }
        this.indexFileDeleter = new IndexFileDeleter(
            deletionPolicy,
            fileDeleter,
            filesListeners,
            committedSnapshots,
            shardPath,
            commitFileManager
        );

        // Notify listeners about the committed snapshot so reader managers
        // are initialized on engine open.
        for (CatalogSnapshotLifecycleListener listener : snapshotListeners) {
            listener.afterRefresh(true, latestCatalogSnapshot);
        }
    }

    /**
     * Advances the catalog generation once at engine-open time so this engine's first commit
     * lands strictly after any prior primary's last commit for the same shard.
     * Prevents {@code RemoteStoreUtils#verifyNoMultipleWriters} collisions on primary relocation.
     */
    public synchronized void bumpGenerationForNewEngineLifecycle() throws IOException {
        commitNewSnapshot(latestCatalogSnapshot.getSegments());
    }

    /**
     * Applies the results of a completed merge to the latest catalog snapshot.
     * Replaces the merged segments with the new merged segment and commits a new snapshot.
     *
     * @param mergeResult the result of the merge containing the merged writer file set
     * @param oneMerge    the merge specification identifying which segments were merged
     * @return the newly created merged {@link Segment}
     * @throws IOException if committing the new snapshot fails
     */
    public synchronized Segment applyMergeResults(MergeResult mergeResult, OneMerge oneMerge) throws IOException {

        List<Segment> segmentList = new ArrayList<>(latestCatalogSnapshot.getSegments());

        Segment segmentToAdd = getSegment(mergeResult.getMergedWriterFileSet());
        Set<Segment> segmentsToRemove = new HashSet<>(oneMerge.getSegmentsToMerge());

        // All source segments must exist in the current snapshot
        assert segmentList.containsAll(segmentsToRemove) : "merge source segments must all exist in the current catalog snapshot";

        // Merged segment generation must not collide with any segment that will be retained
        assert segmentList.stream()
            .filter(s -> segmentsToRemove.contains(s) == false)
            .noneMatch(s -> s.generation() == segmentToAdd.generation()) : "merged segment generation ["
                + segmentToAdd.generation()
                + "] collides with a retained segment generation";

        // Row count conservation: merged output must have the same total rows as the inputs
        assert assertRowCountConservation(segmentsToRemove, segmentToAdd)
            : "merged segment row count must equal sum of source segment row counts";

        boolean inserted = false;
        int newSegIdx = 0;
        for (int segIdx = 0, cnt = segmentList.size(); segIdx < cnt; segIdx++) {
            assert segIdx >= newSegIdx;
            Segment currSegment = segmentList.get(segIdx);
            if (segmentsToRemove.contains(currSegment)) {
                if (!inserted) {
                    segmentList.set(segIdx, segmentToAdd);
                    inserted = true;
                    newSegIdx++;
                }
            } else {
                segmentList.set(newSegIdx, currSegment);
                newSegIdx++;
            }
        }

        // the rest of the segments in list are duplicates, so don't remove from map, only list!
        segmentList.subList(newSegIdx, segmentList.size()).clear();

        // Either we found place to insert segment, or, we did
        // not, but only because all segments we merged became
        // deleted while we are merging, in which case it should
        // be the case that the new segment is also all deleted,
        // we insert it at the beginning if it should not be dropped:
        if (!inserted) {
            segmentList.add(0, segmentToAdd);
        }

        // Commit new catalog snapshot
        commitNewSnapshot(segmentList);
        return segmentToAdd;
    }

    // ---- Refresh path ----

    /**
     * Creates a new CatalogSnapshot from refreshed segments, replacing the current latest.
     * Notifies the IndexFileDeleter of new files. Releases the manager's own reference to
     * the old snapshot — the snapshot will only be fully cleaned up when all references
     * (including any commit references) are released.
     *
     * @param refreshedSegments the segments produced by the latest refresh
     */
    public synchronized void commitNewSnapshot(List<Segment> refreshedSegments) throws IOException {
        if (closed.get()) {
            throw new IllegalStateException("CatalogSnapshotManager is closed");
        }

        // Snapshot generation must advance monotonically — this is the ordering guarantee
        // that readers and the commit path depend on
        long prevGen = latestCatalogSnapshot.getGeneration();

        for (CatalogSnapshotLifecycleListener listener : snapshotListeners) {
            listener.beforeRefresh();
        }

        DataformatAwareCatalogSnapshot newSnapshot;
        try {
            newSnapshot = new DataformatAwareCatalogSnapshot(
                latestCatalogSnapshot.getId() + 1,
                latestCatalogSnapshot.getGeneration() + 1,
                latestCatalogSnapshot.getVersion() + 1,
                refreshedSegments,
                latestCatalogSnapshot.getLastWriterGeneration() + 1,
                latestCatalogSnapshot.getUserData(),
                latestCatalogSnapshot.getLastCommitFileName(),
                latestCatalogSnapshot.getLastCommitGeneration()
            );
        } catch (Exception e) {
            // Construction failed (e.g., OOM) — notify listeners that the refresh did not produce a new snapshot
            // so they can reset any state prepared in beforeRefresh
            for (CatalogSnapshotLifecycleListener listener : snapshotListeners) {
                try {
                    listener.afterRefresh(false, null);
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            throw e;
        }

        // New snapshot generation must be strictly greater than the previous
        assert newSnapshot.getGeneration() > prevGen : "new snapshot generation ["
            + newSnapshot.getGeneration()
            + "] must be > previous ["
            + prevGen
            + "]";
        // New snapshot ID must be strictly greater than the previous
        assert newSnapshot.getId() > latestCatalogSnapshot.getId() : "new snapshot ID ["
            + newSnapshot.getId()
            + "] must be > previous ["
            + latestCatalogSnapshot.getId()
            + "]";

        // Segment generation uniqueness: a generation that appeared in a previous snapshot
        // must not reappear with different files. This prevents generation overlap bugs
        // where a merge output reuses a writer generation, causing file identity confusion.
        assert assertSegmentGenerationFileConsistency(refreshedSegments)
            : "segment generation-to-file mapping is inconsistent with previous snapshots";

        // No duplicate generations within the same snapshot
        assert refreshedSegments.stream().map(Segment::generation).distinct().count() == refreshedSegments.size()
            : "refreshed segments contain duplicate generations";

        // Every segment must have at least one format with files
        assert refreshedSegments.stream().allMatch(s -> s.dfGroupedSearchableFiles().isEmpty() == false)
            : "every segment must have at least one format's files";

        // Every WriterFileSet in every segment must have a positive row count
        assert refreshedSegments.stream().flatMap(s -> s.dfGroupedSearchableFiles().values().stream()).allMatch(wfs -> wfs.numRows() > 0)
            : "every WriterFileSet must have a positive row count";

        // Register file references BEFORE notifying listeners and swapping the snapshot.
        // This ensures that if addFileReferences fails, no listener has been told about
        // the new snapshot and no state has been mutated.
        try {
            indexFileDeleter.addFileReferences(newSnapshot);
        } catch (IOException e) {
            // File reference registration failed — notify listeners that refresh did not complete
            for (CatalogSnapshotLifecycleListener listener : snapshotListeners) {
                try {
                    listener.afterRefresh(false, null);
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            throw new RuntimeException("Failed to add file references for snapshot [gen=" + newSnapshot.getGeneration() + "]", e);
        }

        // Now notify listeners — file references are already registered, so even if a listener
        // fails, the files are tracked and will be cleaned up when the snapshot is deleted.
        List<CatalogSnapshotLifecycleListener> notified = new ArrayList<>();
        try {
            for (CatalogSnapshotLifecycleListener listener : snapshotListeners) {
                listener.afterRefresh(true, newSnapshot);
                notified.add(listener);
            }
        } catch (Exception ex) {
            // A listener failed after file references were registered. The snapshot is tracked
            // by the file deleter but was never made visible as latestCatalogSnapshot.
            // Notify already-notified listeners that the snapshot is being discarded.
            for (CatalogSnapshotLifecycleListener listener : notified) {
                try {
                    listener.onDeleted(newSnapshot);
                } catch (Exception suppressed) {
                    ex.addSuppressed(suppressed);
                }
            }
            // Remove file references since the snapshot will never be used
            try {
                indexFileDeleter.removeFileReferences(newSnapshot);
            } catch (IOException suppressed) {
                ex.addSuppressed(suppressed);
            }
            throw ex;
        }

        catalogSnapshotMap.put(newSnapshot.getGeneration(), newSnapshot);

        CatalogSnapshot oldSnapshot = latestCatalogSnapshot;
        latestCatalogSnapshot = newSnapshot;

        logger.debug("New Catalog Snapshot created: {}", latestCatalogSnapshot);

        // Release the manager's own reference to the old snapshot.
        // The snapshot won't be deleted if the commit path still holds a reference.
        decRefAndMaybeDelete(oldSnapshot);
    }

    /**
     * Replaces the current snapshot with one received from the primary via segment replication.
     * The incoming snapshot is registered with the file deleter (ref counts for its files), then
     * the manager's prior reference is released. Replica-only: does not go through a commit.
     */
    public synchronized void applyReplicationSnapshot(CatalogSnapshot incoming) {
        if (closed.get()) {
            throw new IllegalStateException("CatalogSnapshotManager is closed");
        }
        try {
            indexFileDeleter.addFileReferences(incoming);
        } catch (IOException e) {
            throw new RuntimeException("Failed to add file references for replicated snapshot [gen=" + incoming.getGeneration() + "]", e);
        }
        catalogSnapshotMap.put(incoming.getGeneration(), incoming);
        CatalogSnapshot previous = latestCatalogSnapshot;
        latestCatalogSnapshot = incoming;
        decRefAndMaybeDelete(previous);
    }

    // ---- Acquire path ----

    /**
     * Acquires the current latest snapshot with an incremented reference count.
     * Read-path only — the returned {@link GatedCloseable}'s close will decRef the snapshot.
     *
     * @return a {@link GatedCloseable} wrapping the current {@link CatalogSnapshot}
     * @throws IllegalStateException if the manager or snapshot is already closed
     */
    public GatedCloseable<CatalogSnapshot> acquireSnapshot() {
        if (closed.get()) {
            throw new IllegalStateException("CatalogSnapshotManager is closed");
        }
        final CatalogSnapshot snapshot = latestCatalogSnapshot;
        if (snapshot.tryIncRef() == false) {
            throw new IllegalStateException("CatalogSnapshot [gen=" + snapshot.getGeneration() + "] is already closed");
        }
        return new GatedCloseable<>(snapshot, () -> decRefAndMaybeDelete(snapshot));
    }

    /**
     * Acquires the current latest snapshot for committing (flushing).
     * <p>
     * The caller must call {@code markSuccess()} on the returned handle after a successful commit.
     * On close:
     * <ul>
     *   <li>If successful — registers the snapshot with the deletion policy via {@link IndexFileDeleter#onCommit}</li>
     *   <li>If not successful (failure) — releases the ref via {@link #decRefAndMaybeDelete}</li>
     * </ul>
     *
     * @return a {@link GatedConditionalCloseable} wrapping the current {@link CatalogSnapshot}
     * @throws IllegalStateException if the manager or snapshot is already closed
     */
    public GatedConditionalCloseable<CatalogSnapshot> acquireSnapshotForCommit() {
        if (closed.get()) {
            throw new IllegalStateException("CatalogSnapshotManager is closed");
        }
        final CatalogSnapshot snapshot = latestCatalogSnapshot;
        if (snapshot.tryIncRef() == false) {
            throw new IllegalStateException("CatalogSnapshot [gen=" + snapshot.getGeneration() + "] is already closed");
        }
        return new GatedConditionalCloseable<>(snapshot, () -> {
            try {
                snapshot.markCommitted();
                indexFileDeleter.onCommit(snapshot);
            } catch (IOException e) {
                throw new RuntimeException("Failed to register commit [gen=" + snapshot.getGeneration() + "]", e);
            }
        }, () -> decRefAndMaybeDelete(snapshot));
    }

    // ---- Snapshot protection for _snapshot API / peer recovery ----

    /**
     * Acquire a committed snapshot for _snapshot API or peer recovery.
     * The snapshot won't be deleted until the returned {@link GatedCloseable} is closed.
     * On close, the policy releases the hold and the deleter revisits for cleanup.
     *
     * @param acquiringSafe if true, acquires the safe commit (for peer recovery);
     *                      otherwise the last commit (for _snapshot API)
     */
    public GatedCloseable<CatalogSnapshot> acquireCommittedSnapshot(boolean acquiringSafe) {
        GatedCloseable<CatalogSnapshot> policyRef = deletionPolicy.acquireCommittedSnapshot(acquiringSafe);
        return new GatedCloseable<>(policyRef.get(), () -> {
            try {
                policyRef.close();
                indexFileDeleter.revisitPolicy();
            } catch (IOException e) {
                throw new RuntimeException("Failed to release committed snapshot [gen=" + policyRef.get().getGeneration() + "]", e);
            }
        });
    }

    // ---- Internal ----

    private void decRefAndMaybeDelete(CatalogSnapshot snapshot) {
        final long gen = snapshot.getGeneration();
        if (snapshot.decRef()) {
            catalogSnapshotMap.remove(gen);
            Exception firstException = null;
            try {
                indexFileDeleter.removeFileReferences(snapshot);
            } catch (IOException e) {
                firstException = e;
            }
            for (CatalogSnapshotLifecycleListener listener : snapshotListeners) {
                try {
                    listener.onDeleted(snapshot);
                } catch (IOException e) {
                    if (firstException == null) {
                        firstException = e;
                    } else {
                        firstException.addSuppressed(e);
                    }
                }
            }
            if (firstException != null) {
                throw new RuntimeException("Failed to clean up snapshot [gen=" + gen + "]", firstException);
            }
        }
    }

    /**
     * Builds a {@link Segment} from a map of data format to writer file set entries.
     *
     * @param writerFileSetMap the map of data formats to their corresponding writer file sets
     * @return the constructed segment
     * @throws IllegalArgumentException if the map is empty
     */
    private Segment getSegment(Map<DataFormat, WriterFileSet> writerFileSetMap) {
        if (writerFileSetMap.isEmpty()) {
            throw new IllegalArgumentException("writerFileSetMap must not be empty");
        }
        long generation = writerFileSetMap.values().iterator().next().writerGeneration();
        Segment.Builder segment = Segment.builder(generation);
        for (Map.Entry<DataFormat, WriterFileSet> entry : writerFileSetMap.entrySet()) {
            segment.addSearchableFiles(entry.getKey(), entry.getValue());
        }
        return segment.build();
    }

    /**
     * Closes this manager. Idempotent. DecRefs the current snapshot and cleans up if count reaches zero.
     */
    @Override
    public void close() {
        closed.compareAndSet(false, true);
    }

    /**
     * Asserts that no segment generation in the new snapshot conflicts with a different
     * file set in any existing tracked snapshot. This catches generation overlap bugs
     * where a merge or writer reuses a generation number, causing the catalog to track
     * two different file sets under the same generation — which would lead to data loss
     * when the "wrong" files are deleted.
     */
    private boolean assertSegmentGenerationFileConsistency(List<Segment> newSegments) {
        for (Segment newSeg : newSegments) {
            for (CatalogSnapshot existing : catalogSnapshotMap.values()) {
                for (Segment existingSeg : existing.getSegments()) {
                    if (existingSeg.generation() == newSeg.generation()) {
                        // Same generation — files must be identical per format
                        for (Map.Entry<String, WriterFileSet> entry : newSeg.dfGroupedSearchableFiles().entrySet()) {
                            WriterFileSet existingWfs = existingSeg.dfGroupedSearchableFiles().get(entry.getKey());
                            if (existingWfs != null && existingWfs.files().equals(entry.getValue().files()) == false) {
                                logger.error(
                                    "Generation {} has conflicting files for format [{}]: existing={}, new={}",
                                    newSeg.generation(),
                                    entry.getKey(),
                                    existingWfs.files(),
                                    entry.getValue().files()
                                );
                                return false;
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Asserts that the total row count across all formats in the merged segment equals
     * the total row count across all formats in the source segments. This catches bugs
     * where rows are silently dropped or duplicated during merge.
     */
    private boolean assertRowCountConservation(Set<Segment> sourceSegments, Segment mergedSegment) {
        long sourceRows = 0;
        for (Segment seg : sourceSegments) {
            for (WriterFileSet wfs : seg.dfGroupedSearchableFiles().values()) {
                sourceRows += wfs.numRows();
            }
        }
        long mergedRows = 0;
        for (WriterFileSet wfs : mergedSegment.dfGroupedSearchableFiles().values()) {
            mergedRows += wfs.numRows();
        }
        if (sourceRows != mergedRows) {
            logger.error("Row count mismatch: source segments have {} rows but merged segment has {} rows", sourceRows, mergedRows);
            return false;
        }
        return true;
    }
}
