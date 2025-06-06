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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static java.util.Arrays.asList;
import static org.apache.lucene.document.LongPoint.pack;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApproximatePointRangeQueryTests extends OpenSearchTestCase {

    protected static final String DATE_FIELD_NAME = "mapped_date";

    public void testApproximateRangeEqualsActualRange() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                for (int i = 0; i < 100; i++) {
                    int numPoints = RandomNumbers.randomIntBetween(random(), 1, 10);
                    Document doc = new Document();
                    for (int j = 0; j < numPoints; j++) {
                        for (int v = 0; v < dims; v++) {
                            scratch[v] = RandomNumbers.randomLongBetween(random(), 0, 100);
                        }
                        doc.add(new LongPoint("point", scratch));
                    }
                    iw.addDocument(doc);
                }
                iw.flush();
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = RandomNumbers.randomLongBetween(random(), -100, 200);
                        long upper = lower + RandomNumbers.randomLongBetween(random(), 0, 100);
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        Query query = LongPoint.newRangeQuery("point", lower, upper);
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

    public void testApproximateRangeWithDefaultSize() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = i;
                    }
                    doc.add(new LongPoint("point", scratch));
                    iw.addDocument(doc);
                    if (i % 15 == 0) iw.flush();
                }
                iw.flush();
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 0;
                        long upper = 1000;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
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

    public void testApproximateRangeWithSizeDefault() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = i;
                    }
                    doc.add(new LongPoint("point", scratch));
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                final int size = 10;
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 0;
                        long upper = 45;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            size,
                            null,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
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

    public void testApproximateRangeWithSizeOverDefault() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                int numPoints = 15000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = i;
                    }
                    doc.add(new LongPoint("point", scratch));
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 0;
                        long upper = 12000;
                        long maxHits = 12001;
                        final int size = 11000;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            size,
                            null,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, size);

                        if (topDocs.totalHits.relation() == Relation.EQUAL_TO) {
                            assertEquals(topDocs.totalHits.value(), size);
                        } else {
                            assertTrue(11000 <= topDocs.totalHits.value());
                            assertTrue(maxHits >= topDocs.totalHits.value());
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        }
    }

    public void testApproximateRangeShortCircuit() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = i;
                    }
                    doc.add(new LongPoint("point", scratch));
                    iw.addDocument(doc);
                    if (i % 10 == 0) iw.flush();
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 0;
                        long upper = 100;
                        final int size = 10;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            size,
                            null,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        Query query = LongPoint.newRangeQuery("point", lower, upper);

                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, size);
                        TopDocs topDocs1 = searcher.search(query, size);
                        assertEquals(size, topDocs.scoreDocs.length);
                        assertEquals(size, topDocs1.scoreDocs.length);
                        assertEquals(topDocs1.totalHits.value(), 101);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        }
    }

    public void testApproximateRangeShortCircuitAscSort() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                long[] scratch = new long[dims];
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = i;
                    }
                    iw.addDocument(asList(new LongPoint("point", scratch[0]), new NumericDocValuesField("point", scratch[0])));
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 0;
                        long upper = 20;
                        final int size = 10;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            size,
                            SortOrder.ASC,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        Query query = LongPoint.newRangeQuery("point", lower, upper);

                        IndexSearcher searcher = new IndexSearcher(reader);
                        Sort sort = new Sort(new SortField("point", SortField.Type.LONG));
                        TopDocs topDocs = searcher.search(approximateQuery, size, sort);
                        TopDocs topDocs1 = searcher.search(query, size, sort);

                        assertEquals(topDocs.scoreDocs[0].doc, topDocs1.scoreDocs[0].doc);
                        assertEquals(topDocs.scoreDocs[1].doc, topDocs1.scoreDocs[1].doc);
                        assertEquals(topDocs.scoreDocs[2].doc, topDocs1.scoreDocs[2].doc);
                        assertEquals(topDocs.scoreDocs[3].doc, topDocs1.scoreDocs[3].doc);
                        assertEquals(topDocs.scoreDocs[4].doc, topDocs1.scoreDocs[4].doc);
                        assertEquals(topDocs.scoreDocs[5].doc, topDocs1.scoreDocs[5].doc);

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        }
    }

    public void testSize() {
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            "point",
            pack(0).bytes,
            pack(20).bytes,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
        assertEquals(query.getSize(), 10_000);

        query.setSize(100);
        assertEquals(query.getSize(), 100);

    }

    public void testSortOrder() {
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            "point",
            pack(0).bytes,
            pack(20).bytes,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
        assertNull(query.getSortOrder());

        query.setSortOrder(SortOrder.ASC);
        assertEquals(query.getSortOrder(), SortOrder.ASC);
    }

    public void testCanApproximate() {
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            "point",
            pack(0).bytes,
            pack(20).bytes,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        );

        assertFalse(query.canApproximate(null));

        ApproximatePointRangeQuery queryCanApproximate = new ApproximatePointRangeQuery(
            "point",
            pack(0).bytes,
            pack(20).bytes,
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
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            "point",
            pack(0).bytes,
            pack(20).bytes,
            1,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
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

    public void testApproximateRangeShortCircuitDescSort() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                long[] scratch = new long[dims];
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    for (int v = 0; v < dims; v++) {
                        scratch[v] = i;
                    }
                    iw.addDocument(asList(new LongPoint("point", scratch[0]), new NumericDocValuesField("point", scratch[0])));
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 980;
                        long upper = 999;
                        final int size = 10;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            size,
                            SortOrder.DESC,
                            ApproximatePointRangeQuery.LONG_FORMAT
                        );
                        Query query = LongPoint.newRangeQuery("point", lower, upper);

                        IndexSearcher searcher = new IndexSearcher(reader);
                        Sort sort = new Sort(new SortField("point", SortField.Type.LONG, true)); // true for DESC
                        TopDocs topDocs = searcher.search(approximateQuery, size, sort);
                        TopDocs topDocs1 = searcher.search(query, size, sort);

                        // Verify we got the highest values first (DESC order)
                        for (int i = 0; i < size; i++) {
                            assertEquals("Mismatch at doc index " + i, topDocs.scoreDocs[i].doc, topDocs1.scoreDocs[i].doc);
                        }

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    // Test to cover the left child traversal in intersectRight with CELL_INSIDE_QUERY
    public void testIntersectRightLeftChildTraversal() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                long[] scratch = new long[dims];
                // Create a dataset that will create a multi-level BKD tree
                // We need enough documents to create internal nodes (not just leaves)
                int numPoints = 2000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    scratch[0] = i;
                    doc.add(new LongPoint("point", scratch[0]));
                    iw.addDocument(asList(new LongPoint("point", scratch[0]), new NumericDocValuesField("point", scratch[0])));
                    if (i % 100 == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                iw.forceMerge(1);

                try (IndexReader reader = iw.getReader()) {
                    // Query that will match many documents and require tree traversal
                    long lower = 1000;
                    long upper = 1999;
                    final int size = 50;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        "point",
                        pack(lower).bytes,
                        pack(upper).bytes,
                        dims,
                        50, // Small size to ensure we hit the left child traversal condition
                        SortOrder.DESC,
                        ApproximatePointRangeQuery.LONG_FORMAT
                    );
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort = new Sort(new SortField("point", SortField.Type.LONG, true)); // DESC
                    TopDocs topDocs = searcher.search(query, size, sort);
                    assertEquals("Should return exactly size value documents", size, topDocs.scoreDocs.length);
                }
            }
        }
    }

    // Test to cover pointTree.visitDocIDs(visitor) in CELL_INSIDE_QUERY leaf case for intersectRight
    public void testIntersectRightCellInsideQueryLeaf() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                long[] scratch = new long[dims];
                // Create a smaller dataset that will result in leaf nodes that are completely inside the query range
                for (int i = 900; i <= 999; i++) {
                    scratch[0] = i;
                    iw.addDocument(asList(new LongPoint("point", scratch[0]), new NumericDocValuesField("point", scratch[0])));
                }
                iw.flush();
                iw.forceMerge(1);

                try (IndexReader reader = iw.getReader()) {
                    // Query that completely contains all documents (CELL_INSIDE_QUERY)
                    long lower = 800;
                    long upper = 1100;
                    final int size = 200;
                    final int returnSize = 100;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        "point",
                        pack(lower).bytes,
                        pack(upper).bytes,
                        dims,
                        200,
                        SortOrder.DESC,
                        ApproximatePointRangeQuery.LONG_FORMAT
                    );

                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort = new Sort(new SortField("point", SortField.Type.LONG, true)); // DESC
                    TopDocs topDocs = searcher.search(query, size, sort);

                    assertEquals("Should find all documents", returnSize, topDocs.totalHits.value());
                    // Should return all the indexed point values from 900 to 999 which tests CELL_INSIDE_QUERY
                    assertEquals("Should return exactly return size value documents", returnSize, topDocs.scoreDocs.length);
                }
            }
        }
    }

    // Test to cover CELL_OUTSIDE_QUERY break case for intersectRight
    public void testIntersectRightCellOutsideQuery() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                long[] scratch = new long[dims];
                // Create documents in two separate ranges to ensure some cells are outside query
                // Range 1: 0-99
                for (int i = 0; i < 100; i++) {
                    scratch[0] = i;
                    iw.addDocument(asList(new LongPoint("point", scratch[0]), new NumericDocValuesField("point", scratch[0])));
                }
                // Range 2: 500-599 (gap ensures some tree nodes will be completely outside query)
                for (int i = 500; i < 600; i++) {
                    scratch[0] = i;
                    iw.addDocument(asList(new LongPoint("point", scratch[0]), new NumericDocValuesField("point", scratch[0])));
                }
                iw.flush();
                iw.forceMerge(1);

                try (IndexReader reader = iw.getReader()) {
                    // Query only the middle range - this should create CELL_OUTSIDE_QUERY for some nodes
                    long lower = 200;
                    long upper = 400;
                    final int size = 50;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        "point",
                        pack(lower).bytes,
                        pack(upper).bytes,
                        dims,
                        size,
                        SortOrder.DESC,
                        ApproximatePointRangeQuery.LONG_FORMAT
                    );

                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort = new Sort(new SortField("point", SortField.Type.LONG, true)); // DESC
                    TopDocs topDocs = searcher.search(query, size, sort);

                    // Should find no documents since our query range (200-400) has no documents
                    assertEquals("Should find no documents in the gap range", 0, topDocs.totalHits.value());
                }
            }
        }
    }

    // Test to cover intersectRight with CELL_CROSSES_QUERY case and ensure comprehensive coverage for intersectRight
    public void testIntersectRightCellCrossesQuery() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                long[] scratch = new long[dims];
                // Create documents that will result in cells that cross the query boundary
                for (int i = 0; i < 1000; i++) {
                    scratch[0] = i;
                    iw.addDocument(asList(new LongPoint("point", scratch[0]), new NumericDocValuesField("point", scratch[0])));
                }
                iw.flush();
                iw.forceMerge(1);

                try (IndexReader reader = iw.getReader()) {
                    // Query that will partially overlap with tree nodes (CELL_CROSSES_QUERY)
                    // This range will intersect with some tree nodes but not completely contain them
                    long lower = 250;
                    long upper = 750;
                    final int size = 100;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        "point",
                        pack(lower).bytes,
                        pack(upper).bytes,
                        dims,
                        100,
                        SortOrder.DESC,
                        ApproximatePointRangeQuery.LONG_FORMAT
                    );

                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort = new Sort(new SortField("point", SortField.Type.LONG, true)); // DESC
                    TopDocs topDocs = searcher.search(query, size, sort);

                    assertEquals("Should return exactly size value documents", size, topDocs.scoreDocs.length);
                    // For Desc sort the ApproximatePointRangeQuery will slightly over collect to retain the highest matched docs
                    assertTrue("Should collect at least requested number of documents", topDocs.totalHits.value() >= 100);
                }
            }
        }
    }

    // Test to specifically cover the single child case in intersectRight
    public void testIntersectRightSingleChildNode() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                long[] scratch = new long[dims];

                for (int i = 0; i < 100; i++) {
                    scratch[0] = 1000L;
                    iw.addDocument(asList(new LongPoint("point", scratch[0]), new NumericDocValuesField("point", scratch[0])));
                }
                scratch[0] = 987654321L;
                iw.addDocument(asList(new LongPoint("point", scratch[0]), new NumericDocValuesField("point", scratch[0])));

                iw.flush();
                iw.forceMerge(1);

                try (IndexReader reader = iw.getReader()) {
                    long lower = 500L;
                    long upper = 999999999L;
                    final int size = 50;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        "point",
                        pack(lower).bytes,
                        pack(upper).bytes,
                        dims,
                        size,
                        SortOrder.DESC,
                        ApproximatePointRangeQuery.LONG_FORMAT
                    );

                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort = new Sort(new SortField("point", SortField.Type.LONG, true));
                    TopDocs topDocs = searcher.search(query, size, sort);

                    assertEquals("Should return exactly size value documents", size, topDocs.scoreDocs.length);
                }
            }
        }
    }

    // Following test replicates the http_logs dataset
    public void testHttpLogTimestampDistribution() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                // Sparse range: 100-199 (100 docs, one per value)
                for (int i = 100; i < 200; i++) {
                    Document doc = new Document();
                    doc.add(new LongPoint("timestamp", i));
                    doc.add(new NumericDocValuesField("timestamp", i));
                    iw.addDocument(doc);
                }
                // Dense range: 1000-1999 (5000 docs, 5 per value)
                for (int i = 0; i < 5000; i++) {
                    long value = 1000 + (i / 5); // Creates 5 docs per value from 1000-1999
                    Document doc = new Document();
                    doc.add(new LongPoint("timestamp", value));
                    doc.add(new NumericDocValuesField("timestamp", value));
                    iw.addDocument(doc);
                }
                // 0-99 (100 docs)
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    doc.add(new LongPoint("timestamp", i));
                    doc.add(new NumericDocValuesField("timestamp", i));
                    iw.addDocument(doc);
                }
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    // Test sparse region
                    testApproximateVsExactQuery(searcher, "timestamp", 100, 199, 50, dims);
                    // Test dense region
                    testApproximateVsExactQuery(searcher, "timestamp", 1000, 1500, 100, dims);
                    // Test across regions
                    testApproximateVsExactQuery(searcher, "timestamp", 0, 2000, 200, dims);
                }
            }
        }
    }

    // Following test replicates the nyx_taxis dataset
    public void testNycTaxiDataDistribution() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                // Create NYC taxi fare distribution with different ranges
                // Structure: {count, baseAmount, increment}
                int[][] fareRanges = { { 100, 250, 2 }, { 5000, 1000, 1 }, { 1000, 3000, 3 }, { 200, 10000, 100 } };
                for (int[] range : fareRanges) {
                    int count = range[0];
                    int base = range[1];
                    int increment = range[2];
                    for (int i = 0; i < count; i++) {
                        long fare = base + (increment == 1 ? (i % 2000) : (i * increment));
                        iw.addDocument(asList(new LongPoint("fare_amount", fare), new NumericDocValuesField("fare_amount", fare)));
                    }
                }
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    // Test 1: Query for fare range 10-30
                    long typicalFareMin = 1000;
                    long typicalFareMax = 3000;
                    testApproximateVsExactQuery(searcher, "fare_amount", typicalFareMin, typicalFareMax, 100, dims);
                    // Test 2: Query for high fare range
                    long highFareMin = 10000;
                    long highFareMax = 50000;
                    testApproximateVsExactQuery(searcher, "fare_amount", highFareMin, highFareMax, 50, dims);
                    // Test 3: Query for very low fares
                    long lowFareMin = 250;
                    long lowFareMax = 500;
                    testApproximateVsExactQuery(searcher, "fare_amount", lowFareMin, lowFareMax, 50, dims);
                }
            }
        }
    }

    private void testApproximateVsExactQuery(IndexSearcher searcher, String field, long lower, long upper, int size, int dims)
        throws IOException {
        // Test with approximate query
        ApproximatePointRangeQuery approxQuery = new ApproximatePointRangeQuery(
            field,
            pack(lower).bytes,
            pack(upper).bytes,
            dims,
            size,
            null,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
        // Test with exact query
        Query exactQuery = LongPoint.newRangeQuery(field, lower, upper);
        TopDocs approxDocs = searcher.search(approxQuery, size);
        TopDocs exactDocs = searcher.search(exactQuery, size);
        // Verify approximate query returns correct number of results
        assertTrue("Approximate query should return at most " + size + " docs", approxDocs.scoreDocs.length <= size);
        // If exact query returns fewer docs than size, approximate should match
        if (exactDocs.totalHits.value() <= size) {
            assertEquals(
                "When exact results fit in size, approximate should match exactly",
                exactDocs.totalHits.value(),
                approxDocs.totalHits.value()
            );
        }
        // Test with sorting (ASC and DESC)
        Sort ascSort = new Sort(new SortField(field, SortField.Type.LONG));
        Sort descSort = new Sort(new SortField(field, SortField.Type.LONG, true));
        // Test ASC sort
        ApproximatePointRangeQuery approxQueryAsc = new ApproximatePointRangeQuery(
            field,
            pack(lower).bytes,
            pack(upper).bytes,
            dims,
            size,
            SortOrder.ASC,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
        TopDocs approxDocsAsc = searcher.search(approxQueryAsc, size, ascSort);
        TopDocs exactDocsAsc = searcher.search(exactQuery, size, ascSort);
        // Verify results match
        for (int i = 0; i < size; i++) {
            assertEquals("ASC sorted results should match at position " + i, exactDocsAsc.scoreDocs[i].doc, approxDocsAsc.scoreDocs[i].doc);
        }
        assertEquals("Should return exactly size value documents", size, approxDocsAsc.scoreDocs.length);
        assertEquals(
            "Should return exactly size value documents as regular query",
            exactDocsAsc.scoreDocs.length,
            approxDocsAsc.scoreDocs.length
        );
        // Test DESC sort
        ApproximatePointRangeQuery approxQueryDesc = new ApproximatePointRangeQuery(
            field,
            pack(lower).bytes,
            pack(upper).bytes,
            dims,
            size,
            SortOrder.DESC,
            ApproximatePointRangeQuery.LONG_FORMAT
        );
        TopDocs approxDocsDesc = searcher.search(approxQueryDesc, size, descSort);
        TopDocs exactDocsDesc = searcher.search(exactQuery, size, descSort);
        // Verify the results match
        for (int i = 0; i < size; i++) {
            assertEquals(
                "DESC sorted results should match at position " + i,
                exactDocsDesc.scoreDocs[i].doc,
                approxDocsDesc.scoreDocs[i].doc
            );
        }
        assertEquals("Should return exactly size value documents", size, approxDocsAsc.scoreDocs.length);
        assertEquals(
            "Should return exactly size value documents as regular query",
            exactDocsAsc.scoreDocs.length,
            approxDocsAsc.scoreDocs.length
        );
    }
}
