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
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Manages the segment merge queue, lifecycle callbacks, and merge candidate
 * selection via {@link MergePolicyProvider}.
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
    private final MergePolicyProvider mergePolicy;
    private final Merger merger;
    private final Logger logger;

    /**
     * Creates a new merge handler.
     *
     * @param snapshotSupplier supplier for acquiring catalog snapshots for segment validation
     * @param merger           the merger that performs the actual merge operation
     * @param indexSettings    the index settings used to configure the merge policy
     * @param shardId          the shard this handler is associated with (used for logging)
     */
    public MergeHandler(
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier,
        Merger merger,
        IndexSettings indexSettings,
        ShardId shardId
    ) {
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.snapshotSupplier = snapshotSupplier;
        this.mergePolicy = new DataFormatAwareMergePolicy(indexSettings.getMergePolicy(true), shardId);
        this.merger = merger;
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
        mergePolicy.addMergingSegment(merge.getSegmentsToMerge());
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
     * This method calls {@link #updatePendingMerges()} which reads the catalog to find
     * new merge candidates. If the catalog still contains the old source segments,
     * they may be incorrectly selected for another merge.
     *
     * @param oneMerge the merge that finished
     * @see MergeScheduler — the production caller that enforces this ordering via
     *      {@code applyMergeChanges.accept(mergeResult, oneMerge)} before this call
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
     * Executes the given merge operation by delegating to the {@link Merger}.
     *
     * @param oneMerge the merge to execute
     * @return the result of the merge
     * @throws IOException if the merge operation fails
     */
    public MergeResult doMerge(OneMerge oneMerge) throws IOException {
        List<WriterFileSet> writerFiles = oneMerge.getSegmentsToMerge()
            .stream()
            .flatMap(segment -> segment.dfGroupedSearchableFiles().values().stream())
            .toList();
        MergeInput mergeInput = MergeInput.builder().fileMetadataList(writerFiles).build();
        return merger.merge(mergeInput);
    }

    private synchronized void removeMergingSegments(OneMerge oneMerge) {
        pendingMerges.remove(oneMerge);
        oneMerge.getSegmentsToMerge().forEach(currentlyMergingSegments::remove);
        mergePolicy.removeMergingSegment(oneMerge.getSegmentsToMerge());
    }
}
