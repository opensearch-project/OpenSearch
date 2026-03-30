/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Abstract handler responsible for managing segment merge operations.
 * <p>
 * Subclasses define the merge policy by implementing {@link #findMerges()} and
 * {@link #findForceMerges(int)}, while this base class manages the pending merge
 * queue and lifecycle callbacks.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class MergeHandler {

    private final Deque<OneMerge> mergingSegments = new ArrayDeque<>();
    private final Set<Segment> currentlyMergingSegments = new HashSet<>();
    private final Indexer indexer;
    private final Logger logger;

    /**
     * Creates a new merge handler.
     *
     * @param indexer  the indexer used to acquire catalog snapshots for segment validation
     * @param shardId  the shard this handler is associated with (used for logging)
     */
    public MergeHandler(Indexer indexer, ShardId shardId) {
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.indexer = indexer;
    }

    /**
     * Finds merges that should be executed based on the current segment state.
     *
     * @return a collection of merges to execute, or an empty collection if none are needed
     */
    public abstract Collection<OneMerge> findMerges();

    /**
     * Finds merges required to reduce the number of segments to at most {@code maxSegmentCount}.
     *
     * @param maxSegmentCount the maximum number of segments allowed after merging
     * @return a collection of merges to execute
     */
    public abstract Collection<OneMerge> findForceMerges(int maxSegmentCount);

    /**
     * Updates the set of pending merges. Called to refresh the merge queue
     * when the segment state changes.
     */
    public synchronized void updatePendingMerges() {
        Collection<OneMerge> oneMerges = findMerges();
        for (OneMerge oneMerge : oneMerges) {
            boolean isValidMerge = true;
            for (Segment segment : oneMerge.getSegmentsToMerge()) {
                if (currentlyMergingSegments.contains(segment)) {
                    isValidMerge = false;
                    break;
                }
            }
            if (isValidMerge) {
                registerMerge(oneMerge);
            }
        }
    }

    /**
     * Registers a merge to be executed.
     *
     * @param merge the merge to register
     */
    public synchronized void registerMerge(OneMerge merge) {
        try (GatedCloseable<CatalogSnapshot> catalogSnapshotReleasableRef = indexer.acquireSnapshot()) {
            // Validate segments exist in catalog
            List<Segment> catalogSegments = catalogSnapshotReleasableRef.get().getSegments();
            for (Segment mergeSegment : merge.getSegmentsToMerge()) {
                if (!catalogSegments.contains(mergeSegment)) {
                    return;
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to acquire snapshots", e);
            throw new RuntimeException(e);
        }
        mergingSegments.add(merge);
        currentlyMergingSegments.addAll(merge.getSegmentsToMerge());
        logger.debug(() -> new ParameterizedMessage("Registered merge [{}], mergingSegments: [{}]", merge, mergingSegments));
    }

    /**
     * Returns whether there are any pending merges in the queue.
     *
     * @return {@code true} if there are pending merges
     */
    public synchronized boolean hasPendingMerges() {
        return !mergingSegments.isEmpty();
    }

    /**
     * Retrieves and removes the next pending merge from the queue.
     *
     * @return the next merge to execute, or {@code null} if the queue is empty
     */
    public synchronized OneMerge getNextMerge() {
        if (mergingSegments.isEmpty()) {
            return null;
        }
        return mergingSegments.removeFirst();
    }

    /**
     * Callback invoked when a merge completes successfully.
     *
     * @param oneMerge the merge that finished
     */
    public synchronized void onMergeFinished(OneMerge oneMerge) {
        removeMergingSegments(oneMerge);
        updatePendingMerges();
    }

    /**
     * Callback invoked when a merge fails.
     *
     * @param oneMerge the merge that failed
     */
    public synchronized void onMergeFailure(OneMerge oneMerge) {
        removeMergingSegments(oneMerge);
        logger.warn(() -> new ParameterizedMessage("Merge failed for OneMerge [{}]", oneMerge));
    }

    /**
     * Executes the given merge operation.
     *
     * @param oneMerge the merge to execute
     * @return the result of the merge
     */
    public abstract MergeResult doMerge(OneMerge oneMerge);

    /**
     * Removes the segments belonging to the given merge from the currently-merging set
     * and from the pending queue.
     *
     * @param oneMerge the merge whose segments should be removed
     */
    private synchronized void removeMergingSegments(OneMerge oneMerge) {
        mergingSegments.remove(oneMerge);
        oneMerge.getSegmentsToMerge().forEach(currentlyMergingSegments::remove);
    }

}
