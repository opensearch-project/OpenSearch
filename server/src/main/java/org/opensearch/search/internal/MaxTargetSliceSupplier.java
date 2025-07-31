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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Map;

/**
 * Supplier to compute leaf slices based on passed in leaves and max target slice count to limit the number of computed slices. It sorts
 * all the leaves based on document count and then assign each leaf in round-robin fashion to the target slice count slices. Based on
 * experiment results as shared in <a href=https://github.com/opensearch-project/OpenSearch/issues/7358>issue-7358</a>
 * we can see this mechanism helps to achieve better tail/median latency over default lucene slice computation.
 *
 * @opensearch.internal
 */
final class MaxTargetSliceSupplier {

    static IndexSearcher.LeafSlice[] getSlices(List<LeafReaderContext> leaves, SliceInputConfig sliceInputConfig) {

        int targetMaxSlice = sliceInputConfig.targetMaxSliceCount;

        if (targetMaxSlice <= 0) {
            throw new IllegalArgumentException("MaxTargetSliceSupplier called with unexpected slice count of " + targetMaxSlice);
        }

        // slice count should not exceed the segment count
        int targetSliceCount = Math.min(targetMaxSlice, leaves.size());

        boolean isIntraSegmentEnabled = sliceInputConfig.intraSegmentEnabled;
        int segmentSizeToSplit = sliceInputConfig.segmentSizeToSplit; // Smallest partition of a segment
        int minSegmentSizeToSplit = segmentSizeToSplit * 2; // At least 2 partitions would make sense

        List<IndexSearcher.LeafReaderContextPartition> partitions = new ArrayList<>(leaves.size());

        Map<Integer, Integer> leafToLastUnassignedDocId = new HashMap<>(leaves.size());

        for (LeafReaderContext leafReaderContext : leaves) {
            // Don't split a segment if it's not enabled OR it doesn't meet the size criteria.
            if (isIntraSegmentEnabled == true && leafReaderContext.reader().maxDoc() >= minSegmentSizeToSplit) {
                partitions.addAll(partitionSegment(leafReaderContext, segmentSizeToSplit, targetSliceCount));
            } else {
                partitions.add(IndexSearcher.LeafReaderContextPartition.createFromAndTo(leafReaderContext, 0, leafReaderContext.reader().maxDoc()));
            }
            leafToLastUnassignedDocId.put(leafReaderContext.ord, 0);
        }

        // Sort all the partitions based on their doc counts in descending order.
        partitions.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.maxDocId - l.minDocId)));

        PriorityQueue<LeafSliceBuilder> queue = new PriorityQueue<>(targetSliceCount);
        for (int i = 0; i < targetSliceCount; i++) {
            queue.add(new LeafSliceBuilder());
        }

        for (IndexSearcher.LeafReaderContextPartition partition : partitions) {
            LeafSliceBuilder leafSliceBuilder = queue.poll();
            leafSliceBuilder.addLeafPartition(partition);
            queue.offer(leafSliceBuilder);
        }

        // Perform de-duplication
        IndexSearcher.LeafSlice[] leafSlices = new IndexSearcher.LeafSlice[targetSliceCount];
        int index = 0;

        for (LeafSliceBuilder leafSliceBuilder : queue) {
            leafSlices[index++] = leafSliceBuilder.build(leafToLastUnassignedDocId);
        }

        return leafSlices;
    }

    static class SliceInputConfig {
        final int targetMaxSliceCount;
        final boolean intraSegmentEnabled;
        final int segmentSizeToSplit;

        SliceInputConfig(SearchContext searchContext) {
            targetMaxSliceCount = searchContext.getTargetMaxSliceCount();
            intraSegmentEnabled = searchContext.shouldUseIntraSegmentConcurrentSearch();
            segmentSizeToSplit = searchContext.getSegmentPartitionSize();
        }

        SliceInputConfig(int targetMaxSliceCount, boolean intraSegmentEnabled, int segmentSizeToSplit) {
            this.targetMaxSliceCount = targetMaxSliceCount;
            this.intraSegmentEnabled = intraSegmentEnabled;
            this.segmentSizeToSplit = segmentSizeToSplit;
        }

    }

    private static class LeafSliceBuilder implements Comparable<LeafSliceBuilder> {

        private int totalSize = 0;
        private final Map<Integer, IndexSearcher.LeafReaderContextPartition> segmentOrdToMergedPartition = new HashMap<>();

        void addLeafPartition(IndexSearcher.LeafReaderContextPartition leafReaderContextPartition) {
            IndexSearcher.LeafReaderContextPartition effectivePartition = leafReaderContextPartition;
            int effectivePartitionDocCount = effectivePartition.maxDocId - effectivePartition.minDocId;
            // Merging 2 LeafReaderContextPartition that fall within same slice.
            // IndexSearcher in Lucene will throw an exception if not merged as it doesn't help parallelism.
            if (segmentOrdToMergedPartition.containsKey(leafReaderContextPartition.ctx.ord)) {
                IndexSearcher.LeafReaderContextPartition storedPartition = segmentOrdToMergedPartition.get(leafReaderContextPartition.ctx.ord);
                effectivePartitionDocCount += storedPartition.maxDocId - storedPartition.minDocId;
                effectivePartition = IndexSearcher.LeafReaderContextPartition.createFromAndTo(leafReaderContextPartition.ctx, 0, effectivePartitionDocCount);
            }
            segmentOrdToMergedPartition.put(effectivePartition.ctx.ord, effectivePartition);
            totalSize += effectivePartitionDocCount;
        }

        /**
         * Called when all the leaf partitions are added.
         * @param leafToLastUnassignedDocId : Map used to track and generate the real From and To docIds for each segment as
         * @return : Leaf slice containing all partitions with
         */
        IndexSearcher.LeafSlice build(Map<Integer, Integer> leafToLastUnassignedDocId) {
            List<IndexSearcher.LeafReaderContextPartition> partitions = new ArrayList<>(segmentOrdToMergedPartition.size());
            for (IndexSearcher.LeafReaderContextPartition leafReaderContextPartition : segmentOrdToMergedPartition.values()) {
                int fromDocId = leafToLastUnassignedDocId.get(leafReaderContextPartition.ctx.ord);
                int toDocId = fromDocId + leafReaderContextPartition.maxDocId - leafReaderContextPartition.minDocId;
                partitions.add(IndexSearcher.LeafReaderContextPartition.createFromAndTo(leafReaderContextPartition.ctx, fromDocId, toDocId));
                leafToLastUnassignedDocId.put(leafReaderContextPartition.ctx.ord, toDocId);
            }
            return new IndexSearcher.LeafSlice(partitions);
        }

        @Override
        public int compareTo(LeafSliceBuilder o) {
            return Integer.compare(totalSize, o.totalSize);
        }
    }

    /**
     * Consider a segment with 31_000 documents and user has configured 10_000 ( denoted by partitonSize parameter )
     * as minimum size for the partition of a segment. We first determine number of partitions, 31_000 / 10_000 = 3. <br>
     * Then, we determine the remainingDocs = 31_000 % 10_000 = 1000 that need to be divided. <br>
     * Then, it's divided equally amongst all partitions as 10_000 + ( 1000 / 3 ) = 10_333  <br>
     * Still one partition would get one extra doc which is also considered. So, net result is: <br>
     * [ 31_000 ] = [ 10_334. 10_333, 10_333 ]
     */
    private static List<IndexSearcher.LeafReaderContextPartition> partitionSegment(
        LeafReaderContext leaf,
        int partitionSize,
        int targetSliceCount
    ) {

        int segmentMaxDoc = leaf.reader().maxDoc();
        int numPartitions = segmentMaxDoc / partitionSize;

        // Max number of splits/partitions for a segment should not exceed the available slices.
        if (numPartitions > targetSliceCount) {
            numPartitions = targetSliceCount;
            partitionSize = segmentMaxDoc / numPartitions;
        }

        int remainingDocs = segmentMaxDoc % partitionSize;
        int minPartitionSize = partitionSize + (remainingDocs / numPartitions);
        int partitionsWithOneExtraDoc = remainingDocs % numPartitions;

        List<IndexSearcher.LeafReaderContextPartition> partitions = new ArrayList<>(numPartitions);

        int currentStartDocId = 0, currentEndDocId;

        for (int i = 0; i < numPartitions; ++i) {
            currentEndDocId = currentStartDocId + minPartitionSize;
            currentEndDocId += (i < partitionsWithOneExtraDoc) ? 1 : 0;
            partitions.add(IndexSearcher.LeafReaderContextPartition.createFromAndTo(leaf, currentStartDocId, currentEndDocId));
            currentStartDocId = currentEndDocId;
        }

        return partitions;
    }

}
