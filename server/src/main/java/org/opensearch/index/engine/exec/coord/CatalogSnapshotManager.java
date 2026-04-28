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
     * @param fileDeleters         per-format deleters for actual file deletion
     * @param filesListeners       per-format listeners notified on file add/delete
     * @param snapshotListeners    listeners notified on snapshot deletion
     * @param shardPath            for orphan cleanup on init, or null if not needed
     * @param commitFileManager    manager for commit-level files (e.g., segments_N), or null if not needed
     */
    public CatalogSnapshotManager(
        List<CatalogSnapshot> committedSnapshots,
        CatalogSnapshotDeletionPolicy deletionPolicy,
        Map<String, FileDeleter> fileDeleters,
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
            fileDeleters,
            filesListeners,
            committedSnapshots,
            shardPath,
            commitFileManager
        );
    }

    /**
     * Applies the results of a completed merge to the latest catalog snapshot.
     * Replaces the merged segments with the new merged segment and commits a new snapshot.
     *
     * @param mergeResult the result of the merge containing the merged writer file set
     * @param oneMerge    the merge specification identifying which segments were merged
     * @throws IOException if committing the new snapshot fails
     */
    public synchronized void applyMergeResults(MergeResult mergeResult, OneMerge oneMerge) throws IOException {

        List<Segment> segmentList = new ArrayList<>(latestCatalogSnapshot.getSegments());

        Segment segmentToAdd = getSegment(mergeResult.getMergedWriterFileSet());
        Set<Segment> segmentsToRemove = new HashSet<>(oneMerge.getSegmentsToMerge());

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
    public synchronized void commitNewSnapshot(List<Segment> refreshedSegments) {
        if (closed.get()) {
            throw new IllegalStateException("CatalogSnapshotManager is closed");
        }

        // Snapshot generation must advance monotonically — this is the ordering guarantee
        // that readers and the commit path depend on
        long prevGen = latestCatalogSnapshot.getGeneration();

        DataformatAwareCatalogSnapshot newSnapshot = new DataformatAwareCatalogSnapshot(
            latestCatalogSnapshot.getId() + 1,
            latestCatalogSnapshot.getGeneration() + 1,
            latestCatalogSnapshot.getVersion(),
            refreshedSegments,
            latestCatalogSnapshot.getLastWriterGeneration() + 1,
            latestCatalogSnapshot.getUserData()
        );

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

        try {
            indexFileDeleter.addFileReferences(newSnapshot);
        } catch (IOException e) {
            throw new RuntimeException("Failed to add file references for snapshot [gen=" + newSnapshot.getGeneration() + "]", e);
        }
        catalogSnapshotMap.put(newSnapshot.getGeneration(), newSnapshot);

        CatalogSnapshot oldSnapshot = latestCatalogSnapshot;
        latestCatalogSnapshot = newSnapshot;

        logger.trace("New Catalog Snapshot created: {}", latestCatalogSnapshot);

        // Release the manager's own reference to the old snapshot.
        // The snapshot won't be deleted if the commit path still holds a reference.
        decRefAndMaybeDelete(oldSnapshot);
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
            try {
                indexFileDeleter.removeFileReferences(snapshot);
            } catch (IOException e) {
                throw new RuntimeException("Failed to clean up files for snapshot [gen=" + gen + "]", e);
            }
            for (CatalogSnapshotLifecycleListener listener : snapshotListeners) {
                try {
                    listener.onDeleted(snapshot);
                } catch (IOException e) {
                    throw new RuntimeException("Listener failed on snapshot deletion [gen=" + gen + "]", e);
                }
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
}
