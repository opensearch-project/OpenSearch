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
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWholeSegments(getLeaves(expectedSliceCount), 5);
        assertEquals(expectedSliceCount, slices.length);
        for (int i = 0; i < expectedSliceCount; ++i) {
            assertEquals(1, slices[i].partitions.length);
        }
    }

    public void testNegativeSliceCount() {
        assertThrows(
            IllegalArgumentException.class,
            () -> MaxTargetSliceSupplier.getSlices(new ArrayList<>(), randomIntBetween(-3, 0), true, "force", 100)
        );
    }

    public void testEmptyLeavesForcePartitioning() {
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWithForcePartitioning(new ArrayList<>(), 4);
        assertEquals(0, slices.length);
    }

    public void testEmptyLeavesAutoPartitioning() {
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWithAutoPartitioning(new ArrayList<>(), 4, 100);
        assertEquals(0, slices.length);
    }

    public void testDistributePartitionsEmpty() {
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.distributePartitions(new ArrayList<>(), 4);
        assertEquals(0, slices.length);
    }

    public void testSingleSliceWithMultipleLeaves() throws Exception {
        int leafCount = randomIntBetween(1, 10);
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWholeSegments(getLeaves(leafCount), 1);
        assertEquals(1, slices.length);
        assertEquals(leafCount, slices[0].partitions.length);
    }

    public void testSliceCountLessThanLeafCount() throws Exception {
        int leafCount = 12;
        List<LeafReaderContext> leaves = getLeaves(leafCount);
        // Case 1: test with equal number of leaves per slice
        int expectedSliceCount = 3;
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWholeSegments(leaves, expectedSliceCount);
        int expectedLeavesPerSlice = leafCount / expectedSliceCount;
        assertEquals(expectedSliceCount, slices.length);
        for (int i = 0; i < expectedSliceCount; ++i) {
            assertEquals(expectedLeavesPerSlice, slices[i].partitions.length);
        }
        // Case 2: test with first 2 slice more leaves than others
        expectedSliceCount = 5;
        slices = MaxTargetSliceSupplier.getSlicesWholeSegments(leaves, expectedSliceCount);
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
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWholeSegments(new ArrayList<>(), 2);
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
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWholeSegments(leaves, 2);
                assertEquals(2, slices.length);
                assertEquals(1, slices[0].partitions.length);
                assertEquals(3, slices[0].getMaxDocs());
                assertEquals(2, slices[1].partitions.length);
                assertEquals(2, slices[1].getMaxDocs());
            }
        }
    }

    public void testForcePartitioningSingleSegment() throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int i = 0; i < 100; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                assertEquals(1, leaves.size());
                // 1 segment of 100 docs → 4 partitions of 25 docs each
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWithForcePartitioning(leaves, 4);
                assertEquals(4, slices.length);
                // Each slice has 1 partition with 25 docs
                assertEquals(1, slices[0].partitions.length);
                assertEquals(25, slices[0].getMaxDocs());
                assertEquals(1, slices[1].partitions.length);
                assertEquals(25, slices[1].getMaxDocs());
                assertEquals(1, slices[2].partitions.length);
                assertEquals(25, slices[2].getMaxDocs());
                assertEquals(1, slices[3].partitions.length);
                assertEquals(25, slices[3].getMaxDocs());
            }
        }
    }

    public void testForcePartitioningMultipleSegments() throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            // Create 2 segments with 100 docs each
            for (int i = 0; i < 100; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            for (int i = 0; i < 100; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                assertEquals(2, leaves.size());
                // Force partitions each segment into 4 partitions = 8 total, distributed to 4 slices
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWithForcePartitioning(leaves, 4);
                assertEquals(4, slices.length);
                assertEquals(2, slices[0].partitions.length);
                assertEquals(50, slices[0].getMaxDocs());
                assertEquals(2, slices[1].partitions.length);
                assertEquals(50, slices[1].getMaxDocs());
                assertEquals(2, slices[2].partitions.length);
                assertEquals(50, slices[2].getMaxDocs());
                assertEquals(2, slices[3].partitions.length);
                assertEquals(50, slices[3].getMaxDocs());
            }
        }
    }

    public void testBalancedPartitioningLargeSegment() throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int i = 0; i < 1000; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                assertEquals(1, leaves.size());
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWithAutoPartitioning(leaves, 4, 100);
                assertEquals(4, slices.length);
                assertEquals(1, slices[0].partitions.length);
                assertEquals(250, slices[0].getMaxDocs());
                assertEquals(1, slices[1].partitions.length);
                assertEquals(250, slices[1].getMaxDocs());
                assertEquals(1, slices[2].partitions.length);
                assertEquals(250, slices[2].getMaxDocs());
                assertEquals(1, slices[3].partitions.length);
                assertEquals(250, slices[3].getMaxDocs());
            }
        }
    }

    public void testBalancedPartitioningSmallSegmentSkipped() throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int i = 0; i < 50; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                assertEquals(1, leaves.size());
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWithAutoPartitioning(leaves, 4, 100);
                assertEquals(1, slices.length);
                assertEquals(1, slices[0].partitions.length);
            }
        }
    }

    public void testGetSlicesWithNoneStrategy() throws Exception {
        List<LeafReaderContext> leaves = getLeaves(4);
        IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(leaves, 2, false, "segment", 100);
        assertEquals(2, slices.length);
    }

    public void testGetSlicesWithForceStrategy() throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int i = 0; i < 100; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(leaves, 4, true, "force", 100);
                assertEquals(4, slices.length);
                assertEquals(1, slices[0].partitions.length);
                assertEquals(25, slices[0].getMaxDocs());
                assertEquals(1, slices[1].partitions.length);
                assertEquals(25, slices[1].getMaxDocs());
                assertEquals(1, slices[2].partitions.length);
                assertEquals(25, slices[2].getMaxDocs());
                assertEquals(1, slices[3].partitions.length);
                assertEquals(25, slices[3].getMaxDocs());
            }
        }
    }

    public void testGetSlicesWithBalancedStrategy() throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            // Create 1 large segment (1000 docs) that exceeds minSegmentSize
            for (int i = 0; i < 1000; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(leaves, 4, true, "balanced", 100);
                assertEquals(4, slices.length);
                assertEquals(1, slices[0].partitions.length);
                assertEquals(250, slices[0].getMaxDocs());
                assertEquals(1, slices[1].partitions.length);
                assertEquals(250, slices[1].getMaxDocs());
                assertEquals(1, slices[2].partitions.length);
                assertEquals(250, slices[2].getMaxDocs());
                assertEquals(1, slices[3].partitions.length);
                assertEquals(250, slices[3].getMaxDocs());
            }
        }
    }

    public void testGetSlicesWithBalancedStrategyMultipleSegments() throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            // Create 1 large segment (400 docs) and 2 small segments (50 docs each)
            for (int i = 0; i < 400; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            for (int i = 0; i < 50; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            for (int i = 0; i < 50; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                assertEquals(3, leaves.size());
                // minSegmentSize=100 → only large segment (400 docs) gets partitioned into 2
                // Small segments (50 docs each) stay whole
                // Total partitions = 2 (from large) + 2 (small whole) = 4, distributed to 2 slices
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlices(leaves, 2, true, "balanced", 100);
                assertEquals(2, slices.length);
                assertEquals(2, slices[0].partitions.length);
                assertEquals(250, slices[0].getMaxDocs());
                assertEquals(2, slices[1].partitions.length);
                assertEquals(250, slices[1].getMaxDocs());
            }
        }
    }

    public void testSameSegmentPartitionsInDifferentSlices() throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int i = 0; i < 100; ++i) {
                Document document = new Document();
                document.add(new StringField("field1", "value", Field.Store.NO));
                iw.addDocument(document);
            }
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                IndexSearcher.LeafSlice[] slices = MaxTargetSliceSupplier.getSlicesWithForcePartitioning(leaves, 4);
                for (IndexSearcher.LeafSlice slice : slices) {
                    assertEquals(1, slice.partitions.length);
                }
            }
        }
    }
}
