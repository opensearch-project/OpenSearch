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

import static org.opensearch.search.internal.IndexReaderUtils.getLeaves;

public class MaxTargetSliceSupplierTests extends OpenSearchTestCase {

    public void testSliceCountGreaterThanLeafCount() throws Exception {
        int expectedSliceCount = 2;
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(getLeaves(expectedSliceCount), 5);
        // verify slice count is same as leaf count
        assertEquals(expectedSliceCount, slices.length);
        for (int i = 0; i < expectedSliceCount; ++i) {
            assertEquals(1, slices[i].leaves.length);
        }
    }

    public void testNegativeSliceCount() {
        assertThrows(IllegalArgumentException.class, () -> MaxTargetSliceSupplier.getSlices(new ArrayList<>(), randomIntBetween(-3, 0)));
    }

    public void testSingleSliceWithMultipleLeaves() throws Exception {
        int leafCount = randomIntBetween(1, 10);
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(getLeaves(leafCount), 1);
        assertEquals(1, slices.length);
        assertEquals(leafCount, slices[0].leaves.length);
    }

    public void testSliceCountLessThanLeafCount() throws Exception {
        int leafCount = 12;
        List<LeafReaderContext> leaves = getLeaves(leafCount);

        // Case 1: test with equal number of leaves per slice
        int expectedSliceCount = 3;
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(leaves, expectedSliceCount);
        int expectedLeavesPerSlice = leafCount / expectedSliceCount;

        assertEquals(expectedSliceCount, slices.length);
        for (int i = 0; i < expectedSliceCount; ++i) {
            assertEquals(expectedLeavesPerSlice, slices[i].leaves.length);
        }

        // Case 2: test with first 2 slice more leaves than others
        expectedSliceCount = 5;
        slices = MaxTargetSliceSupplier.getSlices(leaves, expectedSliceCount);
        int expectedLeavesInFirst2Slice = 3;
        int expectedLeavesInOtherSlice = 2;

        assertEquals(expectedSliceCount, slices.length);
        for (int i = 0; i < expectedSliceCount; ++i) {
            if (i < 2) {
                assertEquals(expectedLeavesInFirst2Slice, slices[i].leaves.length);
            } else {
                assertEquals(expectedLeavesInOtherSlice, slices[i].leaves.length);
            }
        }
    }

    public void testEmptyLeaves() {
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(new ArrayList<>(), 2);
        assertEquals(0, slices.length);
    }
}
