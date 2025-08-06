/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
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
            assertEquals(1, slices[i].partitions.length);
        }
    }

    public void testNegativeSliceCount() {
        assertThrows(IllegalArgumentException.class, () -> MaxTargetSliceSupplier.getSlices(new ArrayList<>(), randomIntBetween(-3, 0)));
    }

    public void testSingleSliceWithMultipleLeaves() throws Exception {
        int leafCount = randomIntBetween(1, 10);
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(getLeaves(leafCount), 1);
        assertEquals(1, slices.length);
        assertEquals(leafCount, slices[0].partitions.length);
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
            assertEquals(expectedLeavesPerSlice, slices[i].partitions.length);
        }

        // Case 2: test with first 2 slice more leaves than others
        expectedSliceCount = 5;
        slices = MaxTargetSliceSupplier.getSlices(leaves, expectedSliceCount);
        int expectedLeavesInFirst2Slice = 3;
        int expectedLeavesInOtherSlice = 2;

        assertEquals(expectedSliceCount, slices.length);
        for (int i = 0; i < expectedSliceCount; ++i) {
            if (i < 2) {
                assertEquals(expectedLeavesInFirst2Slice, slices[i].partitions.length);
            } else {
                assertEquals(expectedLeavesInOtherSlice, slices[i].partitions.length);
            }
        }
    }

    public void testEmptyLeaves() {
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(new ArrayList<>(), 2);
        assertEquals(0, slices.length);
    }

    public void testOptimizedGroup() throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            final String fieldValue = "value";
            for (int i = 0; i < 3; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", fieldValue, Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            for (int i = 0; i < 1; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", fieldValue, Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            for (int i = 0; i < 1; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", fieldValue, Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();

            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                assertEquals(3, leaves.size());
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(leaves, 2);
                assertEquals(1, slices[0].partitions.length);
                assertEquals(3, slices[0].getMaxDocs());

                assertEquals(2, slices[1].partitions.length);
                assertEquals(2, slices[1].getMaxDocs());
            }
        }
    }
}
