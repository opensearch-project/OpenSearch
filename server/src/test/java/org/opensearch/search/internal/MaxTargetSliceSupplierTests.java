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
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.internal.IndexReaderUtils.getLeaves;
import static org.opensearch.search.internal.IndexReaderUtils.verifyDocCountAcrossSlices;
import static org.opensearch.search.internal.IndexReaderUtils.verifyPartitionCountInSlices;
import static org.opensearch.search.internal.IndexReaderUtils.verifyPartitionDocCountAcrossSlices;
import static org.opensearch.search.internal.IndexReaderUtils.verifyUniqueSegmentPartitionsPerSlices;

public class MaxTargetSliceSupplierTests extends OpenSearchTestCase {

    public void testSliceCountGreaterThanLeafCount() throws Exception {
        int expectedSliceCount = 2;
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(
            getLeaves(expectedSliceCount),
            new MaxTargetSliceSupplier.SliceInputConfig(5, false, 0)
        );
        // verify slice count is same as leaf count
        assertEquals(expectedSliceCount, slices.length);
        for (int i = 0; i < expectedSliceCount; ++i) {
            assertEquals(1, slices[i].partitions.length);
        }
    }

    public void testNegativeSliceCount() {
        assertThrows(
            IllegalArgumentException.class,
            () -> MaxTargetSliceSupplier.getSlices(
                new ArrayList<>(),
                new MaxTargetSliceSupplier.SliceInputConfig(randomIntBetween(-3, 0), false, 0)
            )
        );
    }

    public void testSingleSliceWithMultipleLeaves() throws Exception {
        int leafCount = randomIntBetween(1, 10);
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(
            getLeaves(leafCount),
            new MaxTargetSliceSupplier.SliceInputConfig(1, false, 0)
        );
        assertEquals(1, slices.length);
        assertEquals(leafCount, slices[0].partitions.length);
    }

    public void testSliceCountLessThanLeafCount() throws Exception {
        int leafCount = 12;
        List<LeafReaderContext> leaves = getLeaves(leafCount);

        // Case 1: test with equal number of leaves per slice
        int expectedSliceCount = 3;
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(
            leaves,
            new MaxTargetSliceSupplier.SliceInputConfig(expectedSliceCount, false, 0)
        );
        int expectedLeavesPerSlice = leafCount / expectedSliceCount;

        assertEquals(expectedSliceCount, slices.length);
        for (int i = 0; i < expectedSliceCount; ++i) {
            assertEquals(expectedLeavesPerSlice, slices[i].partitions.length);
        }

        // Case 2: test with first 2 slice more leaves than others
        // [ 3, 3, 3, 2, 2 ] Slices with count of leaves inside them.
        // Ordering shouldn't matter as overall query time will be same.
        expectedSliceCount = 5;
        slices = MaxTargetSliceSupplier.getSlices(leaves, new MaxTargetSliceSupplier.SliceInputConfig(expectedSliceCount, false, 0));

        assertEquals(expectedSliceCount, slices.length);
        verifyPartitionCountInSlices(slices, Map.of(3, 2, 2, 3));
    }

    public void testEmptyLeaves() {
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(
            new ArrayList<>(),
            new MaxTargetSliceSupplier.SliceInputConfig(2, false, 0)
        );
        assertEquals(0, slices.length);
    }

    public void testOptimizedGroup() throws Exception {
        List<LeafReaderContext> leaves = new ArrayList<>(getLeaves(1, 3));
        leaves.addAll(getLeaves(2, 1));

        assertEquals(3, leaves.size());
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(
            leaves,
            new MaxTargetSliceSupplier.SliceInputConfig(2, false, 0)
        );
        verifyPartitionCountInSlices(slices, Map.of(2, 1, 1, 1));
        verifyDocCountAcrossSlices(slices, Map.of(2, 1, 3, 1));
    }

    public void testPartitioningForOneLeaf() throws Exception {
        List<LeafReaderContext> leaf = IndexReaderUtils.getLeaves(1, 121);
        int maxSliceCount = 10;
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(
            leaf,
            new MaxTargetSliceSupplier.SliceInputConfig(maxSliceCount, true, 10)
        );
        verifyUniqueSegmentPartitionsPerSlices(slices);
        // 1 partition each in 10 slices
        verifyPartitionCountInSlices(slices, Map.of(1, 10));
        // 9 partitions with 12 docs and 1 partition with 13 docs
        verifyPartitionDocCountAcrossSlices(slices, Map.of(12, 9, 13, 1));

        maxSliceCount = 7;
        slices = MaxTargetSliceSupplier.getSlices(leaf, new MaxTargetSliceSupplier.SliceInputConfig(maxSliceCount, true, 10));
        verifyUniqueSegmentPartitionsPerSlices(slices);
        // 1 partition each in 7 slices
        verifyPartitionCountInSlices(slices, Map.of(1, 7));
        // 2 partitions with 18 docs and 5 partition with 17 docs
        verifyPartitionDocCountAcrossSlices(slices, Map.of(18, 2, 17, 5));
    }

    public void testPartitioningForMultipleLeaves() throws Exception {
        List<LeafReaderContext> leaves = new ArrayList<>(IndexReaderUtils.getLeaves(1, 20));
        // This segment won't be split any further
        leaves.addAll(IndexReaderUtils.getLeaves(1, 19));
        int maxSliceCount = 2;
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(
            leaves,
            new MaxTargetSliceSupplier.SliceInputConfig(maxSliceCount, true, 10)
        );
        verifyUniqueSegmentPartitionsPerSlices(slices);
        // 1 partition in each slice
        verifyPartitionCountInSlices(slices, Map.of(1, 2));
        // 1 partitions with 19 docs and 1 partitions with 20 docs
        verifyPartitionDocCountAcrossSlices(slices, Map.of(19, 1, 20, 1));
    }

}
