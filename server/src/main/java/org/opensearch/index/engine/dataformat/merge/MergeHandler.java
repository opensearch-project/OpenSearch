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
import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Manages the segment merge queue, lifecycle callbacks, and merge candidate
 * selection via {@link MergePolicy}.
 * <p>
 * Merge execution is delegated to a {@link Merger} provided at construction.
 * Per-format plugins (Parquet, Lucene) implement {@link Merger}
 * only — they don't know about multi-format orchestration.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeHandler {

    private final Deque<OneMerge> pendingMerges = new ArrayDeque<>();
    private final Set<Segment> currentlyMergingSegments = new HashSet<>();
    private final Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier;
    private final MergePolicy mergePolicy;
    private final MergeListener mergeListener;
    private final Merger merger;
    private final Logger logger;
    private final Supplier<Long> generationProvider;
    private final Deleter deleter;

    /**
     * Creates a new merge handler.
     *
     * @param snapshotSupplier   supplier for acquiring catalog snapshots for segment validation
     * @param merger             the merger that performs the actual merge operation
     * @param shardId            the shard this handler is associated with (used for logging)
     * @param mergePolicy        policy that selects merge candidates
     * @param mergeListener      receives callbacks when segments enter or leave the merging state
     * @param generationProvider supplies the writer generation assigned to the merged output
     * @param deleter            supplies per-segment live-docs at merge start; {@code null}
     *                           for engines without deletes (treated as "all rows alive")
     */
    public MergeHandler(
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier,
        Merger merger,
        ShardId shardId,
        MergePolicy mergePolicy,
        MergeListener mergeListener,
        Supplier<Long> generationProvider,
        Deleter deleter
    ) {
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.snapshotSupplier = snapshotSupplier;
        this.mergePolicy = mergePolicy;
        this.mergeListener = mergeListener;
        this.merger = merger;
        this.generationProvider = generationProvider;
        this.deleter = deleter;
    }

    /**
     * Finds merges that should be executed based on the current segment state.
     *
     * @return a collection of merges to execute, or an empty collection if none are needed
     */
    public Collection<OneMerge> findMerges() {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (GatedCloseable<CatalogSnapshot> catalogSnapshotRef = snapshotSupplier.get()) {
            List<Segment> segmentList = catalogSnapshotRef.get().getSegments();
            List<List<Segment>> mergeCandidates = mergePolicy.findMergeCandidates(segmentList);
            for (List<Segment> mergeGroup : mergeCandidates) {
                oneMerges.add(new OneMerge(mergeGroup));
            }
        } catch (Exception e) {
            logger.warn("Failed to acquire snapshots", e);
            throw new RuntimeException(e);
        }
        return oneMerges;
    }

    /**
     * Finds merges required to reduce the number of segments to at most {@code maxSegmentCount}.
     *
     * @param maxSegmentCount the maximum number of segments allowed after merging
     * @return a collection of merges to execute
     */
    public Collection<OneMerge> findForceMerges(int maxSegmentCount) {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (GatedCloseable<CatalogSnapshot> catalogSnapshotRef = snapshotSupplier.get()) {
            List<Segment> segmentList = catalogSnapshotRef.get().getSegments();
            List<List<Segment>> mergeCandidates = mergePolicy.findForceMergeCandidates(segmentList, maxSegmentCount);
            for (List<Segment> mergeGroup : mergeCandidates) {
                oneMerges.add(new OneMerge(mergeGroup));
            }
        } catch (Exception e) {
            logger.warn("Failed to acquire snapshots", e);
            throw new RuntimeException(e);
        }
        return oneMerges;
    }

    /**
     * Updates the set of pending merges. Called to refresh the merge queue
     * when the segment state changes.
     */
    public synchronized void findAndRegisterMerges() {
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
        try (GatedCloseable<CatalogSnapshot> catalogSnapshotRef = snapshotSupplier.get()) {
            List<Segment> catalogSegments = catalogSnapshotRef.get().getSegments();
            for (Segment mergeSegment : merge.getSegmentsToMerge()) {
                if (!catalogSegments.contains(mergeSegment)) {
                    return;
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to acquire snapshots", e);
            throw new RuntimeException(e);
        }
        pendingMerges.add(merge);
        currentlyMergingSegments.addAll(merge.getSegmentsToMerge());
        mergeListener.addMergingSegment(merge.getSegmentsToMerge());
        logger.debug(() -> new ParameterizedMessage("Registered merge [{}], pendingMerges: [{}]", merge, pendingMerges));
    }

    /**
     * Returns whether there are any pending merges in the queue.
     *
     * @return {@code true} if there are pending merges
     */
    public synchronized boolean hasPendingMerges() {
        return !pendingMerges.isEmpty();
    }

    /**
     * Retrieves and removes the next pending merge from the queue.
     *
     * @return the next merge to execute, or {@code null} if the queue is empty
     */
    public synchronized OneMerge getNextMerge() {
        if (pendingMerges.isEmpty()) {
            return null;
        }
        return pendingMerges.removeFirst();
    }

    /**
     * Callback invoked when a merge completes successfully.
     * <p>
     * <b>IMPORTANT:</b> The caller MUST apply the merge result to the catalog
     * (replacing source segments with the merged segment) BEFORE calling this method.
     * This method calls {@link #findAndRegisterMerges()} which reads the catalog to find
     * new merge candidates. If the catalog still contains the old source segments,
     * they may be incorrectly selected for another merge.
     *
     * @param oneMerge the merge that finished
     * @see MergeScheduler — the production caller that enforces this ordering via
     *      {@code applyMergeChanges.accept(mergeResult, oneMerge)} before this call
     */
    public synchronized void onMergeFinished(OneMerge oneMerge) {
        removeMergingSegments(oneMerge);
        findAndRegisterMerges();
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
     * Executes the given merge operation by delegating to the {@link Merger}.
     *
     * @param oneMerge the merge to execute
     * @return the result of the merge
     * @throws IOException if the merge operation fails
     */
    public MergeResult doMerge(OneMerge oneMerge) throws IOException {
        assert oneMerge.getSegmentsToMerge().isEmpty() == false : "merge must have at least one segment";
        long generation = generationProvider.get();
        assert generation > 0 : "merge writer generation must be positive but was: " + generation;
        Map<Long, long[]> liveDocs = deleter == null ? Map.of() : deleter.getLiveDocs(oneMerge.getSegmentsToMerge());
        assert liveDocs != null : "deleter returned null live-docs map";
        MergeInput mergeInput = MergeInput.builder()
            .segments(oneMerge.getSegmentsToMerge())
            .newWriterGeneration(generation)
            .liveDocsPerSegment(liveDocs)
            .build();
        MergeResult result = merger.merge(mergeInput);
        assert result != null : "merger must return a non-null MergeResult";
        assert result.getMergedWriterFileSet().isEmpty() == false : "merge result must contain at least one format's files";
        return result;
    }

    private synchronized void removeMergingSegments(OneMerge oneMerge) {
        pendingMerges.remove(oneMerge);
        oneMerge.getSegmentsToMerge().forEach(currentlyMergingSegments::remove);
        mergeListener.removeMergingSegment(oneMerge.getSegmentsToMerge());
    }

    /**
     * A policy that determines how segments should be merged together.
     * <p>
     * Implementations define the strategy for selecting which segments to merge
     * during both regular background merges and forced merge operations.
     *
     * @opensearch.experimental
     */
    public interface MergePolicy {

        /**
         * Finds groups of segments that are candidates for merging.
         * <p>
         * Each inner list represents a set of segments that should be merged together
         * into a single new segment. The outer list contains all such merge groups.
         *
         * @param segments the current list of segments to evaluate for merging
         * @return a list of segment groups, where each group is a list of segments to be merged together;
         *         returns an empty list if no merges are needed
         * @throws IOException if an I/O error occurs while evaluating segments
         */
        List<List<Segment>> findMergeCandidates(List<Segment> segments) throws IOException;

        /**
         * Finds groups of segments that are candidates for a forced merge operation.
         * <p>
         * A forced merge reduces the total number of segments to at most {@code maxSegmentCount}.
         * Each inner list represents a set of segments that should be merged together
         * into a single new segment.
         *
         * @param segments        the current list of segments to evaluate for merging
         * @param maxSegmentCount the maximum number of segments that should remain after all merges complete
         * @return a list of segment groups, where each group is a list of segments to be merged together;
         *         returns an empty list if the segment count is already within the limit
         * @throws IOException if an I/O error occurs while evaluating segments
         */
        List<List<Segment>> findForceMergeCandidates(List<Segment> segments, int maxSegmentCount) throws IOException;
    }

    /**
     * A listener that is notified when segments begin or finish participating in a merge.
     * <p>
     * Implementations can use these callbacks to track which segments are currently
     * being merged, for example to exclude them from future merge candidate selection.
     *
     * @opensearch.experimental
     */
    public interface MergeListener {

        /**
         * Called when the given segments begin participating in a merge.
         *
         * @param mergingSegments the segments that are now being merged
         */
        void addMergingSegment(Collection<Segment> mergingSegments);

        /**
         * Called when the given segments have finished participating in a merge.
         *
         * @param mergingSegments the segments that are no longer being merged
         */
        void removeMergingSegment(Collection<Segment> mergingSegments);
    }
}
