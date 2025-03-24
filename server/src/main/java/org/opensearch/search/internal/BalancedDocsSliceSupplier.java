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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @opensearch.internal
 *
 * Compute leaf slices based on leaf document count. The BalancedDocsSliceSupplier performs greedy assignment of leaves.
 * The slice with the lowest total document count is prioritized as new leaves are read.
 * This is a different mechanism to @ref MaxTargetSliceSupplier, but
 * experiments TODO add link to PR that showed it was better for the vector workload in terms of recall.
 *
 */
final class BalancedDocsSliceSupplier {

    private static final Logger logger = LogManager.getLogger(BalancedDocsSliceSupplier.class);

    /**
     * Creates balanced slices based on leaf reader sizes
     *
     * @param leaves List of LeafReaderContext to be sliced
     * @param targetMaxSlice Maximum number of desired slices
     * @return Array of LeafSlice containing balanced distribution of readers;
     */
    public static IndexSearcher.LeafSlice[] getSlices(List<LeafReaderContext> leaves, int targetMaxSlice) {
        if (targetMaxSlice <= 0) {
            throw new IllegalArgumentException("Target max slice must be > 0 but got: " + targetMaxSlice);
        }

        // Ensure the number of slices does not exceed the number of leaves.
        int targetSliceCount = Math.min(targetMaxSlice, leaves.size());

        // Make a copy and sort the leaves in descending order by maxDoc.
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);
        sortedLeaves.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

        // Helper class to hold a group of leaves and their cumulative document count.
        class SliceGroup {
            final List<IndexSearcher.LeafReaderContextPartition> groupLeaves = new ArrayList<>();
            long totalDocs = 0;
        }

        // Create a priority queue (min-heap) keyed on the totalDocs of each group.
        PriorityQueue<SliceGroup> queue = new PriorityQueue<>(Comparator.comparingLong(g -> g.totalDocs));
        for (int i = 0; i < targetSliceCount; i++) {
            queue.add(new SliceGroup());
        }

        // Process each leaf in descending order and assign it to the slice with the lowest totalDocs.
        for (LeafReaderContext leaf : sortedLeaves) {
            SliceGroup currentGroup = queue.poll();  // get the slice with the smallest totalDocs
            assert currentGroup != null;
            currentGroup.groupLeaves.add(IndexSearcher.LeafReaderContextPartition.createForEntireSegment(leaf));
            currentGroup.totalDocs += leaf.reader().maxDoc();
            queue.add(currentGroup); // reinsert after updating the totalDocs
        }

        // Convert the SliceGroups to an array of LeafSlice objects.

        IndexSearcher.LeafSlice[] slices = new IndexSearcher.LeafSlice[queue.size()];
        int idx = 0;
        for (SliceGroup group : queue) {
            slices[idx++] = new IndexSearcher.LeafSlice(group.groupLeaves);
        }
        return slices;
    }
}
