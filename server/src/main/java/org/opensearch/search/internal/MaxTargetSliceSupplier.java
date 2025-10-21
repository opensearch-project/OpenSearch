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
import java.util.List;
import java.util.PriorityQueue;

/**
 * Supplier to compute leaf slices based on passed in leaves and max target slice count to limit the number of computed slices. It sorts
 * all the leaves based on document count and then assign each leaf in round-robin fashion to the target slice count slices. Based on
 * experiment results as shared in <a href=https://github.com/opensearch-project/OpenSearch/issues/7358>issue-7358</a>
 * we can see this mechanism helps to achieve better tail/median latency over default lucene slice computation.
 *
 * @opensearch.internal
 */
final class MaxTargetSliceSupplier {

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

        final List<List<IndexSearcher.LeafReaderContextPartition>> groupedLeaves = new ArrayList<>(targetSliceCount);
        for (int i = 0; i < targetSliceCount; ++i) {
            groupedLeaves.add(new ArrayList<>());
        }

        PriorityQueue<Group> groupQueue = new PriorityQueue<>();
        for (int i = 0; i < targetSliceCount; i++) {
            groupQueue.offer(new Group(i));
        }
        Group minGroup;
        for (int i = 0; i < sortedLeaves.size(); ++i) {
            // Step 1: Get the least loaded slice
            minGroup = groupQueue.poll(); // This triggers compareTo internally
            // Step 2: Add segment to the slice ← THIS IS WHERE SEGMENT IS ADDED
            groupedLeaves.get(minGroup.index).add(IndexSearcher.LeafReaderContextPartition.createForEntireSegment(sortedLeaves.get(i)));
            // Step 3: Update the slice's load/size
            // This is the key line! It adds the segment's document count to the slice's running total.
            minGroup.sum += sortedLeaves.get(i).reader().maxDoc();
            // Step 4: Put the updated slice back in the priority queue
            groupQueue.offer(minGroup); // This triggers compareTo internally

        }

        return groupedLeaves.stream().map(IndexSearcher.LeafSlice::new).toArray(IndexSearcher.LeafSlice[]::new);
    }

    static class Group implements Comparable<Group> {
        final int index;
        int sum;

        public Group(int index) {
            this.index = index;
            this.sum = 0;
        }

        @Override
        public int compareTo(Group other) {
            return Integer.compare(this.sum, other.sum);
        }
    }
}
