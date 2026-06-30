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
import java.util.Set;

import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY_FORCE;

/**
 * Supplier to compute leaf slices based on passed in leaves and max target slice count to limit the number of computed slices. It sorts
 * all the leaves based on document count and then assign each leaf in round-robin fashion to the target slice count slices. Based on
 * experiment results as shared in <a href=https://github.com/opensearch-project/OpenSearch/issues/7358>issue-7358</a>
 * we can see this mechanism helps to achieve better tail/median latency over default lucene slice computation.
 *
 * @opensearch.internal
 */
final class MaxTargetSliceSupplier {

    static IndexSearcher.LeafSlice[] getSlices(
        List<LeafReaderContext> leaves,
        int targetMaxSlice,
        boolean useIntraSegmentSearch,
        String partitionStrategy,
        int minSegmentSize
    ) {
        if (targetMaxSlice <= 0) {
            throw new IllegalArgumentException("MaxTargetSliceSupplier called with unexpected slice count of " + targetMaxSlice);
        }
        if (leaves.isEmpty()) {
            return new IndexSearcher.LeafSlice[0];
        }
        if (useIntraSegmentSearch == false) {
            return getSlicesWholeSegments(leaves, targetMaxSlice);
        } else if (CONCURRENT_SEGMENT_SEARCH_PARTITION_STRATEGY_FORCE.equals(partitionStrategy)) {
            return getSlicesWithForcePartitioning(leaves, targetMaxSlice);
        } else {
            return getSlicesWithAutoPartitioning(leaves, targetMaxSlice, minSegmentSize);
        }
    }

    /**
     * Original method for whole segments
     */
    static IndexSearcher.LeafSlice[] getSlicesWholeSegments(List<LeafReaderContext> leaves, int targetMaxSlice) {
        List<LeafReaderContextPartition> partitions = new ArrayList<>(leaves.size());
        for (LeafReaderContext leaf : leaves) {
            partitions.add(LeafReaderContextPartition.createForEntireSegment(leaf));
        }
        return distributePartitions(partitions, targetMaxSlice);
    }

    /**
     * Balanced partitioning - partition segments exceeding fair slice share and min segment size.
     */
    static IndexSearcher.LeafSlice[] getSlicesWithAutoPartitioning(List<LeafReaderContext> leaves, int targetMaxSlice, int minSegmentSize) {
        long totalDocs = 0;
        for (LeafReaderContext leaf : leaves) {
            totalDocs += leaf.reader().maxDoc();
        }
        long maxDocsPerPartition = (totalDocs + targetMaxSlice - 1) / targetMaxSlice;
        List<LeafReaderContextPartition> partitions = new ArrayList<>(Math.min(leaves.size() * 2, targetMaxSlice * 2));
        for (LeafReaderContext leaf : leaves) {
            int segmentSize = leaf.reader().maxDoc();
            if (segmentSize > maxDocsPerPartition && segmentSize >= minSegmentSize) {
                int numPartitions = (int) ((segmentSize + maxDocsPerPartition - 1) / maxDocsPerPartition);
                addPartitions(partitions, leaf, Math.min(numPartitions, targetMaxSlice));
            } else {
                partitions.add(LeafReaderContextPartition.createForEntireSegment(leaf));
            }
        }
        return distributePartitions(partitions, targetMaxSlice);
    }

    /**
     * Force partitioning - partition EVERY segment into available slices.
     * Each segment is split into targetMaxSlice partitions regardless of size.
     */
    static IndexSearcher.LeafSlice[] getSlicesWithForcePartitioning(List<LeafReaderContext> leaves, int targetMaxSlice) {
        List<LeafReaderContextPartition> partitions = new ArrayList<>(leaves.size() * targetMaxSlice);
        for (LeafReaderContext leaf : leaves) {
            int numPartitions = Math.min(targetMaxSlice, leaf.reader().maxDoc());
            addPartitions(partitions, leaf, numPartitions);
        }
        return distributePartitions(partitions, targetMaxSlice);
    }

    /**
     * Creates partitions for a segment and adds them to the list.
     */
    private static void addPartitions(List<LeafReaderContextPartition> partitions, LeafReaderContext leaf, int numPartitions) {
        int segmentSize = leaf.reader().maxDoc();
        if (numPartitions > 1) {
            int docsPerPartition = segmentSize / numPartitions;
            for (int i = 0; i < numPartitions; i++) {
                int startDoc = i * docsPerPartition;
                int endDoc = (i == numPartitions - 1) ? segmentSize : startDoc + docsPerPartition;
                partitions.add(LeafReaderContextPartition.createFromAndTo(leaf, startDoc, endDoc));
            }
        } else {
            partitions.add(LeafReaderContextPartition.createForEntireSegment(leaf));
        }
    }

    /**
     * Distribute partitions using LPT algorithm while respecting Lucene's constraint
     * that same-segment partitions must be in different slices.
     */
    static IndexSearcher.LeafSlice[] distributePartitions(List<LeafReaderContextPartition> partitions, int targetMaxSlice) {
        if (partitions.isEmpty()) {
            return new IndexSearcher.LeafSlice[0];
        }
        int sliceCount = Math.min(targetMaxSlice, partitions.size());
        // Sort partitions by doc count descending
        partitions.sort(Collections.reverseOrder(Comparator.comparingInt(MaxTargetSliceSupplier::getPartitionDocCount)));
        GroupWithSegmentTracking[] slices = new GroupWithSegmentTracking[sliceCount];
        for (int i = 0; i < sliceCount; i++) {
            slices[i] = new GroupWithSegmentTracking(i);
        }
        for (LeafReaderContextPartition partition : partitions) {
            int segmentOrd = partition.ctx.ord;
            int docCount = getPartitionDocCount(partition);
            // Find slice with minimum load that doesn't have this segment
            GroupWithSegmentTracking targetSlice = null;
            long minLoad = Long.MAX_VALUE;
            for (GroupWithSegmentTracking slice : slices) {
                if (slice.hasSegment(segmentOrd) == false && slice.docCountSum < minLoad) {
                    minLoad = slice.docCountSum;
                    targetSlice = slice;
                }
            }
            targetSlice.addPartition(partition, docCount);
        }
        // Collect non-empty slices
        List<IndexSearcher.LeafSlice> result = new ArrayList<>(sliceCount);
        for (GroupWithSegmentTracking slice : slices) {
            if (slice.partitions.isEmpty() == false) {
                result.add(new IndexSearcher.LeafSlice(slice.partitions));
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

        public void addPartition(LeafReaderContextPartition partition, long docCount) {
            this.partitions.add(partition);
            this.segmentOrdinals.add(partition.ctx.ord);
            this.docCountSum += docCount;
        }

        @Override
        public int compareTo(GroupWithSegmentTracking other) {
            return Long.compare(this.docCountSum, other.docCountSum);
        }
    }
}
