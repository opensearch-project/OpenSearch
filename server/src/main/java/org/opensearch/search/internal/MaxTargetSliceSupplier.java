/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSearcher.LeafReaderContextPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Supplier to compute leaf slices based on passed in leaves and max target slice count.
 *
 * Uses threshold-based automatic partitioning: only segments that are too large relative
 * to the target slice size get partitioned. This is simple and efficient.
 *
 * @opensearch.internal
 */
final class MaxTargetSliceSupplier {

    private static final Logger logger = LogManager.getLogger(MaxTargetSliceSupplier.class);

    /**
     * Original method for whole segments - maintains backward compatibility
     */
    static IndexSearcher.LeafSlice[] getSlices(List<LeafReaderContext> leaves, int targetMaxSlice) {
        if (targetMaxSlice <= 0) {
            throw new IllegalArgumentException("MaxTargetSliceSupplier called with unexpected slice count of " + targetMaxSlice);
        }

        // slice count should not exceed the segment count
        int targetSliceCount = Math.min(targetMaxSlice, leaves.size());

        // Make a copy so we can sort:
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

        // Sort by maxDoc, descending:
        sortedLeaves.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

        final List<List<LeafReaderContextPartition>> groupedLeaves = new ArrayList<>(targetSliceCount);
        for (int i = 0; i < targetSliceCount; ++i) {
            groupedLeaves.add(new ArrayList<>());
        }

        PriorityQueue<Group> groupQueue = new PriorityQueue<>();
        for (int i = 0; i < targetSliceCount; i++) {
            groupQueue.offer(new Group(i));
        }

        Group minGroup;
        // LPT algorithm
        for (int i = 0; i < sortedLeaves.size(); ++i) {
            minGroup = groupQueue.poll();
            groupedLeaves.get(minGroup.index).add(LeafReaderContextPartition.createForEntireSegment(sortedLeaves.get(i)));
            minGroup.sum += sortedLeaves.get(i).reader().maxDoc();
            groupQueue.offer(minGroup);
        }

        return groupedLeaves.stream().map(IndexSearcher.LeafSlice::new).toArray(IndexSearcher.LeafSlice[]::new);
    }

    static IndexSearcher.LeafSlice[] getSlicesWithAutoPartitioning(List<LeafReaderContext> leaves, int targetMaxSlice) {
        if (targetMaxSlice <= 0) {
            throw new IllegalArgumentException("MaxTargetSliceSupplier called with unexpected slice count of " + targetMaxSlice);
        }

        if (leaves.isEmpty()) {
            return new IndexSearcher.LeafSlice[0];
        }

        logger.info("=== Starting Automatic Threshold-Based Partitioning ===");
        logger.info("Input: {} segments, target {} slices", leaves.size(), targetMaxSlice);

        // Step 1: Calculate total documents and threshold
        long totalDocs = 0;
        for (LeafReaderContext leaf : leaves) {
            totalDocs += leaf.reader().maxDoc();
        }

        // Threshold: max docs per partition = total / target_slices Threshold - Global Level, Decision threshold - determines WHICH segments to partition
        // long maxDocsPerPartition = totalDocs / targetMaxSlice;

        // With Double
        long maxDocsPerPartition = (long) Math.ceil((double) totalDocs / targetMaxSlice);

        logger.info("");
        logger.info("Total docs: {}, Threshold (max docs per partition): {}", totalDocs, maxDocsPerPartition);
        logger.info("The targetMaxSlice is  {}", targetMaxSlice );
        logger.info("--- Analyzing Segments and Creating Partitions ---");

        // Step 2: Create partitions - split only segments that exceed threshold
        List<LeafReaderContextPartition> partitions = new ArrayList<>();


        //COmment below 2
        int segmentsPartitioned = 0;
        int segmentsKeptWhole = 0;

        for (LeafReaderContext leaf : leaves) {
            int segmentSize = leaf.reader().maxDoc();

            if (segmentSize > maxDocsPerPartition) {
                // Segment is too large - partition it
                // int numPartitions = (int) Math.ceil(segmentSize / maxDocsPerPartition);

                // With Double
                int numPartitions = (int) Math.ceil((double) segmentSize / maxDocsPerPartition);

                // Actual partition size - determines HOW to split a specific segment
                int docsPerPartition = segmentSize / numPartitions;

                logger.info(
                    "Segment {}: {} docs > {} threshold → PARTITION into {} pieces ({} docs each)",
                    leaf.ord,
                    segmentSize,
                    maxDocsPerPartition,
                    numPartitions,
                    docsPerPartition
                );

                for (int i = 0; i < numPartitions; i++) {
                    int startDoc = i * docsPerPartition;
                    int endDoc = (i == numPartitions - 1) ? segmentSize : (i + 1) * docsPerPartition;

                    int actualDocs = endDoc - startDoc;

                    partitions.add(LeafReaderContextPartition.createFromAndTo(leaf, startDoc, endDoc));
                    logger.info(
                        "  → Created partition {} for segment {}: docs [{} to {}), {} docs",
                        i,
                        leaf.ord,
                        startDoc,
                        endDoc,
                        actualDocs
                    );
                }
                segmentsPartitioned++;
            } else {
                // Segment is small enough - keep whole
                partitions.add(LeafReaderContextPartition.createForEntireSegment(leaf));
                logger.info("Segment {}: {} docs ≤ {} threshold → Keep WHOLE", leaf.ord, segmentSize, maxDocsPerPartition);
                segmentsKeptWhole++;
            }
        }

        logger.info("");
        logger.info("Partitioning Summary:");
        logger.info("  Segments partitioned: {}", segmentsPartitioned);
        logger.info("  Segments kept whole: {}", segmentsKeptWhole);
        logger.info("  Total partitions created: {}", partitions.size());
        logger.info("");

        // Step 3: Distribute partitions using LPT
        return distributePartitionsWithLPTLogger(partitions, targetMaxSlice);
    }

    /**
     * Distribute partitions using LPT algorithm while respecting Lucene's constraint
     * that same-segment partitions must be in different slices.
     */
    private static IndexSearcher.LeafSlice[] distributePartitionsWithLPT(
        List<LeafReaderContextPartition> partitions,
        int targetMaxSlice
    ) {
        if (partitions.isEmpty()) {
            return new IndexSearcher.LeafSlice[0];
        }

        // Determine slice count - cannot exceed partition count
        int sliceCount = Math.min(targetMaxSlice, partitions.size());

        // Sort partitions by size descending (LPT requirement)
        List<LeafReaderContextPartition> sortedPartitions = new ArrayList<>(partitions);
        sortedPartitions.sort(Collections.reverseOrder(Comparator.comparingInt(p -> getPartitionDocCount(p))));

        // Initialize slices
        PriorityQueue<GroupWithSegmentTracking> sliceHeap = new PriorityQueue<>();
        for (int i = 0; i < sliceCount; i++) {
            sliceHeap.offer(new GroupWithSegmentTracking(i));
        }

        // Assign each partition using LPT with segment constraint
        for (LeafReaderContextPartition partition : sortedPartitions) {
            int segmentOrd = partition.ctx.ord;
            int docCount = getPartitionDocCount(partition);

            // Find least-loaded slice that doesn't have this segment yet
            List<GroupWithSegmentTracking> allSlices = new ArrayList<>();
            while (!sliceHeap.isEmpty()) {
                allSlices.add(sliceHeap.poll());
            }

            GroupWithSegmentTracking targetSlice = null;
            for (GroupWithSegmentTracking slice : allSlices) {
                if (!slice.hasSegment(segmentOrd)) {
                    targetSlice = slice;
                    break; // Already sorted by load
                }
            }

            if (targetSlice == null) {
                // Fallback: not enough slices for the partitions created
                logger.warn("FALLBACK TRIGGERED: Insufficient slices to distribute partitions");
                logger.warn("Segment {} needs a slice but all {} slices already contain this segment", segmentOrd, sliceCount);
                logger.warn("Reverting to whole-segment distribution");

                // Fall back to whole segment distribution
                Set<LeafReaderContext> uniqueSegments = new HashSet<>();
                for (LeafReaderContextPartition p : sortedPartitions) {
                    uniqueSegments.add(p.ctx);
                }
                return getSlices(new ArrayList<>(uniqueSegments), targetMaxSlice);
            }

            // Assign partition to target slice
            targetSlice.addPartition(partition, segmentOrd, docCount);

            // Put all slices back
            for (GroupWithSegmentTracking slice : allSlices) {
                sliceHeap.offer(slice);
            }
        }

        // Convert to array
        List<IndexSearcher.LeafSlice> result = new ArrayList<>();
        while (!sliceHeap.isEmpty()) {
            GroupWithSegmentTracking group = sliceHeap.poll();
            if (!group.partitions.isEmpty()) {
                result.add(new IndexSearcher.LeafSlice(group.partitions));
            }
        }

        return result.toArray(new IndexSearcher.LeafSlice[0]);
    }

    /**
     * Distribute partitions using LPT algorithm while respecting Lucene's constraint
     * that same-segment partitions must be in different slices.
     */
    private static IndexSearcher.LeafSlice[] distributePartitionsWithLPTLogger(List<LeafReaderContextPartition> partitions, int targetMaxSlice) {
        if (partitions.isEmpty()) {
            return new IndexSearcher.LeafSlice[0];
        }

        logger.info("--- LPT Distribution Phase ---");

        // Determine slice count - cannot exceed partition count
        int sliceCount = Math.min(targetMaxSlice, partitions.size());
        logger.info("Effective slice count: {} (min of target={} and partitions={})", sliceCount, targetMaxSlice, partitions.size());

        // Sort partitions by size descending (LPT requirement)
        List<LeafReaderContextPartition> sortedPartitions = new ArrayList<>(partitions);
        sortedPartitions.sort(Collections.reverseOrder(Comparator.comparingInt(p -> getPartitionDocCount(p))));

        logger.info("");
        logger.info("Partitions sorted by size (descending):");
        for (int i = 0; i < sortedPartitions.size(); i++) {
            LeafReaderContextPartition p = sortedPartitions.get(i);
            int docs = getPartitionDocCount(p);
            String partitionType = (p.maxDocId == Integer.MAX_VALUE) ? "WHOLE" : "PART";
            logger.info("  [{}] Segment {} ({}): {} docs", i, p.ctx.ord, partitionType, docs);
        }

        // Initialize slices
        PriorityQueue<GroupWithSegmentTracking> sliceHeap = new PriorityQueue<>();
        for (int i = 0; i < sliceCount; i++) {
            sliceHeap.offer(new GroupWithSegmentTracking(i));
        }

        logger.info("");
        logger.info("--- Assigning Partitions to Slices (LPT Algorithm) ---");

        // Assign each partition using LPT with segment constraint
        int assignmentNumber = 0;
        for (LeafReaderContextPartition partition : sortedPartitions) {
            int segmentOrd = partition.ctx.ord;
            int docCount = getPartitionDocCount(partition);
            String partitionType = (partition.maxDocId == Integer.MAX_VALUE) ? "WHOLE" : "PART";

            // Find least-loaded slice that doesn't have this segment yet
            List<GroupWithSegmentTracking> allSlices = new ArrayList<>();
            while (!sliceHeap.isEmpty()) {
                allSlices.add(sliceHeap.poll());
            }

            // Log current state
            logger.info("");
            logger.info("Assignment #{}: Segment {} ({}) with {} docs", ++assignmentNumber, segmentOrd, partitionType, docCount);
            logger.info("  Current slice loads:");
            for (GroupWithSegmentTracking slice : allSlices) {
                logger.info("    Slice {}: {} docs, has segments: {}", slice.index, slice.docCountSum, slice.segmentOrdinals);
            }

            GroupWithSegmentTracking targetSlice = null;
            for (GroupWithSegmentTracking slice : allSlices) {
                if (!slice.hasSegment(segmentOrd)) {
                    targetSlice = slice;
                    break; // Already sorted by load
                }
            }

            if (targetSlice == null) {
                // This means we don't have enough slices for the partitions created
                logger.warn("FALLBACK TRIGGERED: Insufficient slices to distribute partitions");
                logger.warn("  Segment {} needs a slice but all {} slices already contain this segment", segmentOrd, sliceCount);
                logger.warn("  Reverting to whole-segment distribution");

                // Put slices back
                for (GroupWithSegmentTracking slice : allSlices) {
                    sliceHeap.offer(slice);
                }

                // Fall back to whole segment distribution
                Set<LeafReaderContext> uniqueSegments = new HashSet<>();
                for (LeafReaderContextPartition p : sortedPartitions) {
                    uniqueSegments.add(p.ctx);
                }
                return getSlices(new ArrayList<>(uniqueSegments), targetMaxSlice);
            }

            // Assign partition to target slice
            targetSlice.addPartition(partition, segmentOrd, docCount);
            logger.info(
                "  → Assigned to Slice {} (was {} docs, now {} docs)",
                targetSlice.index,
                targetSlice.docCountSum - docCount,
                targetSlice.docCountSum
            );

            // Put all slices back
            for (GroupWithSegmentTracking slice : allSlices) {
                sliceHeap.offer(slice);
            }
        }

        // Convert to array
        List<IndexSearcher.LeafSlice> result = new ArrayList<>();
        List<GroupWithSegmentTracking> finalSlices = new ArrayList<>();
        while (!sliceHeap.isEmpty()) {
            GroupWithSegmentTracking group = sliceHeap.poll();
            if (!group.partitions.isEmpty()) {
                result.add(new IndexSearcher.LeafSlice(group.partitions));
                finalSlices.add(group);
            }
        }

        // Log final distribution and calculate imbalance
        logger.info("");
        logger.info("=== Final Distribution Summary ===");

        long totalDocs = 0;
        long maxDocs = 0;
        long minDocs = Long.MAX_VALUE;

        for (GroupWithSegmentTracking slice : finalSlices) {
            totalDocs += slice.docCountSum;
            maxDocs = Math.max(maxDocs, slice.docCountSum);
            minDocs = Math.min(minDocs, slice.docCountSum);

            logger.info("");
            logger.info("Slice {}: {} docs across {} partitions", slice.index, slice.docCountSum, slice.partitions.size());
            logger.info("  Contains segments: {}", slice.segmentOrdinals);
            logger.info("  Partitions:");
            for (LeafReaderContextPartition p : slice.partitions) {
                int docs = getPartitionDocCount(p);
                String type = (p.maxDocId == Integer.MAX_VALUE) ? "WHOLE" : "PART";
                logger.info("    - Segment {} ({}): {} docs", p.ctx.ord, type, docs);
            }
        }

        // Calculate imbalance
        double avgDocs = finalSlices.isEmpty() ? 0 : (double) totalDocs / finalSlices.size();
        double imbalancePercentage = (avgDocs > 0) ? ((maxDocs - minDocs) / avgDocs) * 100.0 : 0.0;

        logger.info("");
        logger.info("=== Load Balance Statistics ===");
        logger.info("Total docs distributed: {}", totalDocs);
        logger.info("Number of slices: {}", finalSlices.size());
        logger.info("Average docs per slice: {}", String.format("%.0f", avgDocs));
        logger.info("Min docs in a slice: {}", minDocs);
        logger.info("Max docs in a slice: {}", maxDocs);
        logger.info("Imbalance: {}", String.format("%.2f%%", imbalancePercentage));


        logger.info("=== End of Distribution ===");
        logger.info("");

        return result.toArray(new IndexSearcher.LeafSlice[0]);
    }

    /**
     * Helper to get document count from a partition
     */
    private static int getPartitionDocCount(LeafReaderContextPartition partition) {
        if (partition.maxDocId == Integer.MAX_VALUE) { // NO_MORE_DOCS - entire segment
            return partition.ctx.reader().maxDoc();
        }
        return partition.maxDocId - partition.minDocId;
    }

    /**
     * Original Group class for backward compatibility
     */
    static class Group implements Comparable<Group> {
        final int index;
        long sum;

        public Group(int index) {
            this.index = index;
            this.sum = 0;
        }

        @Override
        public int compareTo(Group other) {
            return Long.compare(this.sum, other.sum);
        }
    }

    /**
     * Group that tracks which segments are in the slice to enforce Lucene constraint
     */
    static class GroupWithSegmentTracking implements Comparable<GroupWithSegmentTracking> {
        final int index;
        long docCountSum;
        final Set<Integer> segmentOrdinals;
        final List<LeafReaderContextPartition> partitions;

        public GroupWithSegmentTracking(int index) {
            this.index = index;
            this.docCountSum = 0;
            this.segmentOrdinals = new HashSet<>();
            this.partitions = new ArrayList<>();
        }

        public boolean hasSegment(int segmentOrd) {
            return segmentOrdinals.contains(segmentOrd);
        }

        public void addPartition(LeafReaderContextPartition partition, int segmentOrd, long docCount) {
            this.partitions.add(partition);
            this.segmentOrdinals.add(segmentOrd);
            this.docCountSum += docCount;
        }

        @Override
        public int compareTo(GroupWithSegmentTracking other) {
            return Long.compare(this.docCountSum, other.docCountSum);
        }
    }
}
