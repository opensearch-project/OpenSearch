/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.apache.lucene.document.LongPoint.pack;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApproximateTermQueryTests extends OpenSearchTestCase {

    public void testApproximateTermEqualsActualTerm() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                for (int i = 0; i < 100; i++) {
                    int numPoints = RandomNumbers.randomIntBetween(random(), 1, 10);
                    Document doc = new Document();
                    // Add multiple point values
                    for (int j = 0; j < numPoints; j++) {
                        for (int v = 0; v < dims; v++) {
                            scratch[v] = RandomNumbers.randomLongBetween(random(), 0, 100);
                        }
                        doc.add(new LongPoint("point", scratch));
                    }
                    // Add only one DocValues field per document - use the last value
                    doc.add(new NumericDocValuesField("point", scratch[0]));
                    iw.addDocument(doc);
                }
                iw.flush();
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long value = RandomNumbers.randomLongBetween(random(), 0, 100);
                        byte[] packedValue = pack(new long[] { value }).bytes;

                        Query approximateQuery = new ApproximateTermQuery(
                            "point",
                            packedValue,
                            dims,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        Query query = LongPoint.newExactQuery("point", value);
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 10);
                        TopDocs topDocs1 = searcher.search(query, 10);
                        assertEquals(topDocs.totalHits, topDocs1.totalHits);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void testApproximateTermWithDefaultSize() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = 42; // All documents have the same value
                    }
                    doc.add(new LongPoint("point", scratch));
                    doc.add(new NumericDocValuesField("point", scratch[0]));
                    iw.addDocument(doc);
                    if (i % 15 == 0) iw.flush();
                }
                iw.flush();
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long value = 42;
                        byte[] packedValue = pack(new long[] { value }).bytes;

                        Query approximateQuery = new ApproximateTermQuery(
                            "point",
                            packedValue,
                            dims,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 10);
                        assertEquals(topDocs.totalHits, new TotalHits(1000, TotalHits.Relation.EQUAL_TO));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void testApproximateTermWithSizeDefault() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = 42; // All documents have the same value
                    }
                    doc.add(new LongPoint("point", scratch));
                    doc.add(new NumericDocValuesField("point", scratch[0]));
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                final int size = 10;
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long value = 42;
                        byte[] packedValue = pack(new long[] { value }).bytes;

                        Query approximateQuery = new ApproximateTermQuery(
                            "point",
                            packedValue,
                            dims,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        ((ApproximateTermQuery) approximateQuery).setSize(size);
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, size);
                        assertEquals(size, topDocs.scoreDocs.length);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void testApproximateTermShortCircuit() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = 42; // All documents have the same value
                    }
                    doc.add(new LongPoint("point", scratch));
                    doc.add(new NumericDocValuesField("point", scratch[0]));
                    iw.addDocument(doc);
                    if (i % 10 == 0) iw.flush();
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long value = 42;
                        byte[] packedValue = pack(new long[] { value }).bytes;

                        final int size = 10;
                        Query approximateQuery = new ApproximateTermQuery(
                            "point",
                            packedValue,
                            dims,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        ((ApproximateTermQuery) approximateQuery).setSize(size);
                        Query query = LongPoint.newExactQuery("point", value);

                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, size);
                        TopDocs topDocs1 = searcher.search(query, size);
                        assertEquals(size, topDocs.scoreDocs.length);
                        assertEquals(size, topDocs1.scoreDocs.length);
                        assertEquals(topDocs1.totalHits.value(), 1000);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void testApproximateTermShortCircuitAscSort() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                // Create documents with different values to test sorting
                for (int i = 0; i < 1000; i++) {
                    Document doc = new Document();
                    long value = i % 100; // Create 10 documents for each value 0-99
                    doc.add(new LongPoint("point", value));
                    doc.add(new NumericDocValuesField("point", value));
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long value = 42; // Search for documents with value 42
                        byte[] packedValue = pack(new long[] { value }).bytes;

                        final int size = 10;
                        Query approximateQuery = new ApproximateTermQuery(
                            "point",
                            packedValue,
                            dims,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        ((ApproximateTermQuery) approximateQuery).setSize(size);
                        ((ApproximateTermQuery) approximateQuery).setSortOrder(SortOrder.ASC);
                        Query query = LongPoint.newExactQuery("point", value);

                        IndexSearcher searcher = new IndexSearcher(reader);
                        Sort sort = new Sort(new SortField("point", SortField.Type.LONG));
                        TopDocs topDocs = searcher.search(approximateQuery, size, sort);
                        TopDocs topDocs1 = searcher.search(query, size, sort);

                        // Verify the documents are the same
                        for (int i = 0; i < Math.min(topDocs.scoreDocs.length, topDocs1.scoreDocs.length); i++) {
                            assertEquals(topDocs.scoreDocs[i].doc, topDocs1.scoreDocs[i].doc);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void testApproximateTermShortCircuitDescSort() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                // Create documents with different values to test sorting
                for (int i = 0; i < 1000; i++) {
                    Document doc = new Document();
                    long value = i % 100; // Create 10 documents for each value 0-99
                    doc.add(new LongPoint("point", value));
                    doc.add(new NumericDocValuesField("point", value));
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long value = 42; // Search for documents with value 42
                        byte[] packedValue = pack(new long[] { value }).bytes;

                        final int size = 10;
                        Query approximateQuery = new ApproximateTermQuery(
                            "point",
                            packedValue,
                            dims,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        ((ApproximateTermQuery) approximateQuery).setSize(size);
                        ((ApproximateTermQuery) approximateQuery).setSortOrder(SortOrder.ASC);
                        Query query = LongPoint.newExactQuery("point", value);

                        IndexSearcher searcher = new IndexSearcher(reader);
                        Sort sort = new Sort(new SortField("point", SortField.Type.LONG, true)); // true for DESC
                        TopDocs topDocs = searcher.search(approximateQuery, size, sort);
                        TopDocs topDocs1 = searcher.search(query, size, sort);

                        // Verify the documents are the same
                        for (int i = 0; i < Math.min(topDocs.scoreDocs.length, topDocs1.scoreDocs.length); i++) {
                            assertEquals(topDocs.scoreDocs[i].doc, topDocs1.scoreDocs[i].doc);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    public void testSize() {
        long value = 42;
        byte[] packedValue = pack(new long[] { value }).bytes;

        ApproximateTermQuery query = new ApproximateTermQuery("point", packedValue, 1, ApproximatePointRangeQuery.LONG_FORMAT);
        assertEquals(query.getSize(), 10_000);

        query.setSize(100);
        assertEquals(query.getSize(), 100);
    }

    public void testSortOrder() {
        long value = 42;
        byte[] packedValue = pack(new long[] { value }).bytes;

        ApproximateTermQuery query = new ApproximateTermQuery("point", packedValue, 1, ApproximatePointRangeQuery.LONG_FORMAT);
        assertNull(query.getSortOrder());

        query.setSortOrder(SortOrder.ASC);
        assertEquals(query.getSortOrder(), SortOrder.ASC);
    }

    public void testCanApproximate() {
        long value = 42;
        byte[] packedValue = pack(new long[] { value }).bytes;

        ApproximateTermQuery query = new ApproximateTermQuery("point", packedValue, 1, ApproximatePointRangeQuery.LONG_FORMAT);

        assertFalse(query.canApproximate(null));

        ApproximateTermQuery queryCanApproximate = new ApproximateTermQuery(
            "point",
            packedValue,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        ) {
            public boolean canApproximate(SearchContext context) {
                return true;
            }
        };
        SearchContext searchContext = mock(SearchContext.class);
        assertTrue(queryCanApproximate.canApproximate(searchContext));
    }

    public void testCannotApproximateWithTrackTotalHits() {
        long value = 42;
        byte[] packedValue = pack(new long[] { value }).bytes;

        ApproximateTermQuery query = new ApproximateTermQuery("point", packedValue, 1, ApproximatePointRangeQuery.LONG_FORMAT);
        SearchContext mockContext = mock(SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        assertFalse(query.canApproximate(mockContext));
        when(mockContext.trackTotalHitsUpTo()).thenReturn(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.from()).thenReturn(0);
        when(mockContext.size()).thenReturn(10);
        when(mockContext.request()).thenReturn(null);
        assertTrue(query.canApproximate(mockContext));
    }

    // Test with integer values
    public void testApproximateTermWithIntegerValues() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                // Create documents with integer values
                for (int i = 0; i < 1000; i++) {
                    Document doc = new Document();
                    int value = i % 100; // Create 10 documents for each value 0-99
                    doc.add(new IntPoint("int_point", value));
                    doc.add(new NumericDocValuesField("int_point", value));
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    int value = 42;
                    byte[] packedValue = IntPoint.pack(new int[] { value }).bytes;

                    final int size = 10;
                    ApproximateTermQuery approximateQuery = new ApproximateTermQuery(
                        "int_point",
                        packedValue,
                        1,
                        ApproximatePointRangeQuery.INT_FORMAT
                    );
                    approximateQuery.setSize(size);
                    Query query = IntPoint.newExactQuery("int_point", value);

                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(approximateQuery, size);
                    TopDocs topDocs1 = searcher.search(query, size);

                    assertEquals("Total hits should match", topDocs1.totalHits.value(), topDocs.totalHits.value());
                    assertEquals("Should return exactly size documents", size, topDocs.scoreDocs.length);
                }
            }
        }
    }

    // Test with mixed values (some documents match, some don't)
    public void testApproximateTermWithMixedValues() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                // Create documents with mixed values
                int expectedMatchCount = 0;
                for (int i = 0; i < 1000; i++) {
                    Document doc = new Document();
                    // Only 10% of documents have value 42
                    long value = (i % 10 == 0) ? 42 : i;
                    if (value == 42) {
                        expectedMatchCount++;
                    }
                    doc.add(new LongPoint("point", value));
                    doc.add(new NumericDocValuesField("point", value));
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long value = 42;
                    byte[] packedValue = pack(new long[] { value }).bytes;

                    final int size = 50;
                    ApproximateTermQuery approximateQuery = new ApproximateTermQuery(
                        "point",
                        packedValue,
                        1,
                        ApproximatePointRangeQuery.LONG_FORMAT
                    );
                    approximateQuery.setSize(size);
                    Query query = LongPoint.newExactQuery("point", value);

                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(approximateQuery, size);
                    TopDocs topDocs1 = searcher.search(query, size);

                    assertEquals("Total hits should match", topDocs1.totalHits.value(), topDocs.totalHits.value());
                    assertEquals("Should return exactly the number of matching documents", expectedMatchCount, topDocs.totalHits.value());
                }
            }
        }
    }

    // Test with large dataset to ensure approximation works
    public void testApproximateTermWithLargeDataset() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                // Create a large dataset with 10,000 documents
                int expectedMatchCount = 0;
                for (int i = 0; i < 10000; i++) {
                    Document doc = new Document();
                    // Every 100th document has value 999
                    long value = (i % 100 == 0) ? 999 : i;
                    if (value == 999) {
                        expectedMatchCount++;
                    }
                    doc.add(new LongPoint("point", value));
                    doc.add(new NumericDocValuesField("point", value));
                    iw.addDocument(doc);
                    if (i % 1000 == 0) iw.flush();
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long value = 999;
                    byte[] packedValue = pack(new long[] { value }).bytes;

                    final int size = 20;
                    ApproximateTermQuery approximateQuery = new ApproximateTermQuery(
                        "point",
                        packedValue,
                        1,
                        ApproximatePointRangeQuery.LONG_FORMAT
                    );
                    approximateQuery.setSize(size);
                    Query query = LongPoint.newExactQuery("point", value);

                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(approximateQuery, size);
                    TopDocs exactDocs = searcher.search(query, 1000); // Get all exact matches

                    // We should have expectedMatchCount documents with value 999
                    assertEquals(
                        "Should find the correct number of documents with value 999",
                        expectedMatchCount,
                        exactDocs.totalHits.value()
                    );

                    // The approximate query should return at most 'size' documents
                    assertTrue("Approximate query should return at most " + size + " docs", topDocs.scoreDocs.length <= size);

                    // If there are fewer matching docs than size, we should get them all
                    if (exactDocs.totalHits.value() <= size) {
                        assertEquals(
                            "When exact results fit in size, approximate should match exactly",
                            exactDocs.totalHits.value(),
                            topDocs.totalHits.value()
                        );
                    }
                }
            }
        }
    }
}
