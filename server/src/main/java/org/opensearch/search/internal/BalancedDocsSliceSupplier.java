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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * @opensearch.internal
 * @opensearch.experimental
 * This class is experimental and may be changed or removed completely
 * in a future release without warning
 */
final class BalancedDocsSliceSupplier {

    private static final Logger logger = LogManager.getLogger(BalancedDocsSliceSupplier.class);

    /**
     * Creates balanced slices based on leaf reader sizes
     *
     * @param leaves List of LeafReaderContext to be sliced
     * @param targetMaxSlice Maximum number of desired slices
     * @return Array of LeafSlice containing balanced distribution of readers
     */
     static IndexSearcher.LeafSlice[] getSlicesv0(List<LeafReaderContext> leaves, int targetMaxSlice) {
        if (leaves == null || leaves.isEmpty()) {
            return new IndexSearcher.LeafSlice[0];
        }

        if (targetMaxSlice <= 1 || leaves.size() == 1) {
            return new IndexSearcher.LeafSlice[]{new IndexSearcher.LeafSlice(leaves)};
        }

        // Calculate sizes for each leaf
        Map<Integer, Long> leafSizes = new HashMap<>();
        long totalDocs = 0;
        for (int i = 0; i < leaves.size(); i++) {
            long size = leaves.get(i).reader().maxDoc();
            leafSizes.put(i, size);
            totalDocs += size;
        }

        // Calculate actual number of slices (min of target and leaf count)
        int actualSlices = Math.min(targetMaxSlice, leaves.size());
        long targetDocsPerSlice = totalDocs / actualSlices;

        // Create slices
        List<List<LeafReaderContext>> groupedLeaves = new ArrayList<>();
        List<LeafReaderContext> currentSlice = new ArrayList<>();
        long currentSliceSize = 0;

        // Sort leaves by size for better distribution
        List<Map.Entry<Integer, Long>> sortedLeaves = new ArrayList<>(leafSizes.entrySet());
        sortedLeaves.sort(Map.Entry.<Integer, Long>comparingByValue().reversed());

        for (Map.Entry<Integer, Long> entry : sortedLeaves) {
            int leafIndex = entry.getKey();
            long leafSize = entry.getValue();

            // If adding this leaf would exceed target size and we haven't reached last slice,
            // start a new slice
            if (currentSliceSize > 0 &&
                currentSliceSize + leafSize > targetDocsPerSlice &&
                groupedLeaves.size() < actualSlices - 1) {
                groupedLeaves.add(currentSlice);
                currentSlice = new ArrayList<>();
                currentSliceSize = 0;
            }

            currentSlice.add(leaves.get(leafIndex));
            currentSliceSize += leafSize;
        }

        // Add the last slice if it's not empty
        if (!currentSlice.isEmpty()) {
            groupedLeaves.add(currentSlice);
        }

        return groupedLeaves.stream().map(IndexSearcher.LeafSlice::new).toArray(IndexSearcher.LeafSlice[]::new);
    }

    static IndexSearcher.LeafSlice[] getSlicesV1(List<LeafReaderContext> leaves, int targetMaxSlice) {
        if (leaves == null || leaves.isEmpty()) {
            return new IndexSearcher.LeafSlice[0];
        }

        if (targetMaxSlice <= 1 || leaves.size() == 1) {
            return new IndexSearcher.LeafSlice[]{new IndexSearcher.LeafSlice(leaves)};
        }

        // Calculate sizes
        long[] sizes = new long[leaves.size()];
        long totalDocs = 0;
        for (int i = 0; i < leaves.size(); i++) {
            sizes[i] = leaves.get(i).reader().maxDoc();
            totalDocs += sizes[i];
        }

        int actualSlices = Math.min(targetMaxSlice, leaves.size());
        long targetDocsPerSlice = totalDocs / actualSlices;

        // Initialize slices
        List<List<LeafReaderContext>> groupedLeaves = new ArrayList<>();
        for (int i = 0; i < actualSlices; i++) {
            groupedLeaves.add(new ArrayList<>());
        }

        // Create pairs of (index, size) and sort by size
        Integer[] indices = new Integer[leaves.size()];
        for (int i = 0; i < leaves.size(); i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, (a, b) -> Long.compare(sizes[b], sizes[a]));

        // Distribute leaves using a greedy approach
        for (int i = 0; i < indices.length; i++) {
            int leafIndex = indices[i];

            // Find the slice with the smallest current total that can fit this leaf
            int bestSlice = 0;
            long minTotal = getSliceTotal(groupedLeaves.get(0), leaves);

            for (int j = 1; j < groupedLeaves.size(); j++) {
                long sliceTotal = getSliceTotal(groupedLeaves.get(j), leaves);
                if (sliceTotal < minTotal) {
                    minTotal = sliceTotal;
                    bestSlice = j;
                }
            }

            groupedLeaves.get(bestSlice).add(leaves.get(leafIndex));
        }

        // Remove empty slices and convert to array
        groupedLeaves.removeIf(List::isEmpty);
        return groupedLeaves.stream()
            .map(IndexSearcher.LeafSlice::new)
            .toArray(IndexSearcher.LeafSlice[]::new);
    }

    private static long getSliceTotal(List<LeafReaderContext> slice, List<LeafReaderContext> allLeaves) {
        return slice.stream()
            .mapToLong(leaf -> leaf.reader().maxDoc())
            .sum();
    }

    public static IndexSearcher.LeafSlice[] getSlicesV2(List<LeafReaderContext> leaves, int targetMaxSlice) {
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
            final List<LeafReaderContext> groupLeaves = new ArrayList<>();
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
            currentGroup.groupLeaves.add(leaf);
            currentGroup.totalDocs += leaf.reader().maxDoc();
            queue.add(currentGroup); // reinsert after updating the totalDocs
        }

        // Convert the SliceGroups to an array of LeafSlice objects.
        List<IndexSearcher.LeafSlice> slices = new ArrayList<>();
        for (SliceGroup group : queue) {
            slices.add(new IndexSearcher.LeafSlice(group.groupLeaves));
        }

        return slices.toArray(new IndexSearcher.LeafSlice[0]);
    }
}
