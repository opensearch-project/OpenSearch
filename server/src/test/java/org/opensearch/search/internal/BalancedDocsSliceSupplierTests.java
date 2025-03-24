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
import static org.apache.lucene.search.IndexSearcher.LeafReaderContextPartition;
import static org.apache.lucene.search.IndexSearcher.LeafSlice;

public class BalancedDocsSliceSupplierTests extends OpenSearchTestCase {
    public void testSliceCountGreaterThanLeafCount() throws Exception {
        int expectedSliceCount = 2;
        LeafSlice[] slices = BalancedDocsSliceSupplier.getSlices(getLeaves(expectedSliceCount), 5);
        assertEquals(expectedSliceCount, slices.length);
        for (int i = 0; i < expectedSliceCount; ++i) {
            assertEquals(1, slices[i].partitions.length);
        }
    }

    public void testNegativeSliceCount() {
        assertThrows(IllegalArgumentException.class, () -> BalancedDocsSliceSupplier.getSlices(new ArrayList<>(), randomIntBetween(-3, 0)));
    }

    public void testSingleSliceWithMultipleLeaves() throws Exception {
        int leafCount = randomIntBetween(1, 10);
        LeafSlice[] slices = BalancedDocsSliceSupplier.getSlices(getLeaves(leafCount), 1);
        assertEquals(1, slices.length);
        assertEquals(leafCount, slices[0].partitions.length);
    }

    public void testBalancedDistribution() throws Exception {
        // Create leaves with varying document counts
        List<LeafReaderContext> leaves = IndexReaderUtils.getLeavesWithDocCounts(new int[] { 1000, 800, 600, 400, 200 });

        int targetSlices = 2;
        LeafSlice[] slices = BalancedDocsSliceSupplier.getSlices(leaves, targetSlices);

        assertEquals(targetSlices, slices.length);

        // Calculate total docs in each slice
        long[] docsInSlice = new long[targetSlices];
        for (int i = 0; i < slices.length; i++) {
            for (LeafReaderContextPartition partition : slices[i].partitions) {
                docsInSlice[i] += partition.ctx.reader().maxDoc();
            }
        }

        // Verify that the difference between slices is minimal
        long diff = Math.abs(docsInSlice[0] - docsInSlice[1]);
        assertTrue("Difference between slice sizes should be minimal", diff <= 400);
    }

    public void testEmptyLeaves() {
        LeafSlice[] slices = BalancedDocsSliceSupplier.getSlices(new ArrayList<>(), 2);
        assertEquals(0, slices.length);
    }

    public void testLargestSlicesFirst() throws Exception {
        List<LeafReaderContext> leaves = IndexReaderUtils.getLeavesWithDocCounts(new int[] { 1000, 500, 250 });

        LeafSlice[] slices = BalancedDocsSliceSupplier.getSlices(leaves, 2);

        // Verify that the slice with more documents comes first
        long firstSliceDocs = getTotalDocs(slices[0]);
        long secondSliceDocs = getTotalDocs(slices[1]);
        assertTrue("First slice should have more or equal documents than second slice", firstSliceDocs >= secondSliceDocs);
    }

    public void testEvenDistributionWithEqualSizedLeaves() throws Exception {
        int[] docsPerLeaf = new int[] { 100, 100, 100, 100, 100, 100 };
        int leafCount = docsPerLeaf.length;
        List<LeafReaderContext> leaves = IndexReaderUtils.getLeavesWithDocCounts(docsPerLeaf);

        int targetSlices = 2;
        LeafSlice[] slices = BalancedDocsSliceSupplier.getSlices(leaves, targetSlices);

        assertEquals(targetSlices, slices.length);
        assertEquals(leafCount / targetSlices, slices[0].partitions.length);
        assertEquals(leafCount / targetSlices, slices[1].partitions.length);
    }

    public void testJaggedDistribution() throws Exception {
        // Very uneven distribution
        int[] docCounts = new int[] { 1658, 202, 160, 9, 64, 2, 175, 185, 4, 1 };

        List<LeafReaderContext> leaves = IndexReaderUtils.getLeavesWithDocCounts(docCounts);
        int targetSlices = 8;
        IndexSearcher.LeafSlice[] slices = BalancedDocsSliceSupplier.getSlices(leaves, targetSlices);

        assertEquals(targetSlices, slices.length);

        // Calculate docs per slice and total docs
        long[] docsInSlice = new long[targetSlices];
        long totalDocs = 0;
        for (int i = 0; i < slices.length; i++) {
            for (IndexSearcher.LeafReaderContextPartition partition : slices[i].partitions) {
                docsInSlice[i] += partition.ctx.reader().maxDoc();
                totalDocs += partition.ctx.reader().maxDoc();
            }
        }

        assertEquals("Total document count should match", 2460, totalDocs);
        // ensure 2,4,1 all assigned to last slice
        assertEquals(docsInSlice[targetSlices - 1], 7);
    }

    private long getTotalDocs(LeafSlice slice) {
        long total = 0;
        for (LeafReaderContextPartition partition : slice.partitions) {
            total += partition.ctx.reader().maxDoc();
        }
        return total;
    }
}
