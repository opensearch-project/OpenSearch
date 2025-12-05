/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

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
 * to the target slice size get partitioned.
 *
 * @opensearch.internal
 */
final class MaxTargetSliceSupplier {

    /**
     * Original method for whole segments - maintains backward compatibility
     */
    static IndexSearcher.LeafSlice[] getSlices(List<LeafReaderContext> leaves, int targetMaxSlice) {
        if (targetMaxSlice <= 0) {
            throw new IllegalArgumentException("MaxTargetSliceSupplier called with unexpected slice count of " + targetMaxSlice);
        }

        int targetSliceCount = Math.min(targetMaxSlice, leaves.size());

        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
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
        for (int i = 0; i < sortedLeaves.size(); ++i) {
            minGroup = groupQueue.poll();
            groupedLeaves.get(minGroup.index).add(LeafReaderContextPartition.createForEntireSegment(sortedLeaves.get(i)));
            minGroup.sum += sortedLeaves.get(i).reader().maxDoc();
            groupQueue.offer(minGroup);
        }

        return groupedLeaves.stream().map(IndexSearcher.LeafSlice::new).toArray(IndexSearcher.LeafSlice[]::new);
    }

    static IndexSearcher.LeafSlice[] getSlicesWithAutoPartitioning(List<LeafReaderContext> leaves, int targetMaxSlice, int minSegmentSize) {
        if (targetMaxSlice <= 0) {
            throw new IllegalArgumentException("MaxTargetSliceSupplier called with unexpected slice count of " + targetMaxSlice);
        }

        if (leaves.isEmpty()) {
            return new IndexSearcher.LeafSlice[0];
        }

        long totalDocs = 0;
        for (LeafReaderContext leaf : leaves) {
            totalDocs += leaf.reader().maxDoc();
        }

        long maxDocsPerPartition = (long) Math.ceil((double) totalDocs / targetMaxSlice);

        List<LeafReaderContextPartition> partitions = new ArrayList<>();

        for (LeafReaderContext leaf : leaves) {
            int segmentSize = leaf.reader().maxDoc();
            if (segmentSize > maxDocsPerPartition && segmentSize >= minSegmentSize) {
                int numPartitions = (int) Math.ceil((double) segmentSize / maxDocsPerPartition);
                numPartitions = Math.min(numPartitions, targetMaxSlice);
                int docsPerPartition = segmentSize / numPartitions;

                for (int i = 0; i < numPartitions; i++) {
                    int startDoc = i * docsPerPartition;
                    int endDoc = (i == numPartitions - 1) ? segmentSize : (i + 1) * docsPerPartition;
                    partitions.add(LeafReaderContextPartition.createFromAndTo(leaf, startDoc, endDoc));
                }
            } else {
                partitions.add(LeafReaderContextPartition.createForEntireSegment(leaf));
            }
        }

        return distributePartitionsWithLPT(partitions, targetMaxSlice);
    }

    /**
     * Distribute partitions using LPT algorithm while respecting Lucene's constraint
     * that same-segment partitions must be in different slices.
     */
    private static IndexSearcher.LeafSlice[] distributePartitionsWithLPT(List<LeafReaderContextPartition> partitions, int targetMaxSlice) {
        if (partitions.isEmpty()) {
            return new IndexSearcher.LeafSlice[0];
        }

        int sliceCount = Math.min(targetMaxSlice, partitions.size());

        List<LeafReaderContextPartition> sortedPartitions = new ArrayList<>(partitions);
        sortedPartitions.sort(Collections.reverseOrder(Comparator.comparingInt(p -> getPartitionDocCount(p))));

        PriorityQueue<GroupWithSegmentTracking> sliceHeap = new PriorityQueue<>();
        for (int i = 0; i < sliceCount; i++) {
            sliceHeap.offer(new GroupWithSegmentTracking(i));
        }

        for (LeafReaderContextPartition partition : sortedPartitions) {
            int segmentOrd = partition.ctx.ord;
            int docCount = getPartitionDocCount(partition);

            List<GroupWithSegmentTracking> allSlices = new ArrayList<>();
            while (!sliceHeap.isEmpty()) {
                allSlices.add(sliceHeap.poll());
            }

            GroupWithSegmentTracking targetSlice = null;
            for (GroupWithSegmentTracking slice : allSlices) {
                if (!slice.hasSegment(segmentOrd)) {
                    targetSlice = slice;
                    break;
                }
            }

            assert targetSlice != null : "No available slice for segment " + segmentOrd;

            targetSlice.addPartition(partition, segmentOrd, docCount);

            for (GroupWithSegmentTracking slice : allSlices) {
                sliceHeap.offer(slice);
            }
        }

        List<IndexSearcher.LeafSlice> result = new ArrayList<>();
        while (!sliceHeap.isEmpty()) {
            GroupWithSegmentTracking group = sliceHeap.poll();
            if (!group.partitions.isEmpty()) {
                result.add(new IndexSearcher.LeafSlice(group.partitions));
            }
        }

        return result.toArray(new IndexSearcher.LeafSlice[0]);
    }

    private static int getPartitionDocCount(LeafReaderContextPartition partition) {
        if (partition.maxDocId == Integer.MAX_VALUE) {
            return partition.ctx.reader().maxDoc();
        }
        return partition.maxDocId - partition.minDocId;
    }

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
