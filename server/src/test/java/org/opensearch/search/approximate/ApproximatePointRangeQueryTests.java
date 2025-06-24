/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.sandbox.document.BigIntegerPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
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
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApproximatePointRangeQueryTests extends OpenSearchTestCase {

    private final NumericType numericType;

    public ApproximatePointRangeQueryTests(NumericType numericType) {
        this.numericType = numericType;
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[][] {
                { NumericType.INT },
                { NumericType.LONG },
                { NumericType.HALF_FLOAT },
                { NumericType.FLOAT },
                { NumericType.DOUBLE },
                { NumericType.UNSIGNED_LONG } }
        );
    }

    enum NumericType {
        INT("int_field", ApproximatePointRangeQuery.INT_FORMAT) {
            @Override
            byte[] encode(Number value) {
                return IntPoint.pack(new int[] { value.intValue() }).bytes;
            }

            @Override
            void addField(Document doc, String fieldName, Number value) {
                doc.add(new IntPoint(fieldName, value.intValue()));
            }

            @Override
            void addDocValuesField(Document doc, String fieldName, Number value) {
                doc.add(new NumericDocValuesField(fieldName, value.intValue()));
            }

            @Override
            Query rangeQuery(String fieldName, Number lower, Number upper) {
                return IntPoint.newRangeQuery(fieldName, lower.intValue(), upper.intValue());
            }

            @Override
            SortField.Type getSortFieldType() {
                return SortField.Type.INT;
            }
        },
        LONG("long_field", ApproximatePointRangeQuery.LONG_FORMAT) {
            @Override
            byte[] encode(Number value) {
                return LongPoint.pack(new long[] { value.longValue() }).bytes;
            }

            // Add to BKD
            @Override
            void addField(Document doc, String fieldName, Number value) {
                doc.add(new LongPoint(fieldName, value.longValue()));
            }

            // Add DocValues
            @Override
            void addDocValuesField(Document doc, String fieldName, Number value) {
                doc.add(new NumericDocValuesField(fieldName, value.longValue()));
            }

            @Override
            Query rangeQuery(String fieldName, Number lower, Number upper) {
                return LongPoint.newRangeQuery(fieldName, lower.longValue(), upper.longValue());
            }

            @Override
            SortField.Type getSortFieldType() {
                return SortField.Type.LONG;
            }
        },
        HALF_FLOAT("half_float_field", ApproximatePointRangeQuery.HALF_FLOAT_FORMAT) {
            @Override
            byte[] encode(Number value) {
                byte[] bytes = new byte[HalfFloatPoint.BYTES];
                HalfFloatPoint.encodeDimension(value.floatValue(), bytes, 0);
                return bytes;
            }

            @Override
            void addField(Document doc, String fieldName, Number value) {
                doc.add(new HalfFloatPoint(fieldName, value.floatValue()));
            }

            @Override
            void addDocValuesField(Document doc, String fieldName, Number value) {
                doc.add(new NumericDocValuesField(fieldName + "_sort", value.longValue()));
            }

            @Override
            Query rangeQuery(String fieldName, Number lower, Number upper) {
                return HalfFloatPoint.newRangeQuery(fieldName, lower.floatValue(), upper.floatValue());
            }

            @Override
            SortField.Type getSortFieldType() {
                return SortField.Type.LONG;
            }
        },
        FLOAT("float_field", ApproximatePointRangeQuery.FLOAT_FORMAT) {
            @Override
            byte[] encode(Number value) {
                return FloatPoint.pack(new float[] { value.floatValue() }).bytes;
            }

            @Override
            void addField(Document doc, String fieldName, Number value) {
                doc.add(new FloatPoint(fieldName, value.floatValue()));
            }

            @Override
            void addDocValuesField(Document doc, String fieldName, Number value) {
                doc.add(new NumericDocValuesField(fieldName, Float.floatToIntBits(value.floatValue())));
            }

            @Override
            Query rangeQuery(String fieldName, Number lower, Number upper) {
                return FloatPoint.newRangeQuery(fieldName, lower.floatValue(), upper.floatValue());
            }

            @Override
            SortField.Type getSortFieldType() {
                return SortField.Type.FLOAT;
            }
        },
        DOUBLE("double_field", ApproximatePointRangeQuery.DOUBLE_FORMAT) {
            @Override
            byte[] encode(Number value) {
                return DoublePoint.pack(new double[] { value.doubleValue() }).bytes;
            }

            @Override
            void addField(Document doc, String fieldName, Number value) {
                doc.add(new DoublePoint(fieldName, value.doubleValue()));
            }

            @Override
            void addDocValuesField(Document doc, String fieldName, Number value) {
                doc.add(new NumericDocValuesField(fieldName, Double.doubleToLongBits(value.doubleValue())));
            }

            @Override
            Query rangeQuery(String fieldName, Number lower, Number upper) {
                return DoublePoint.newRangeQuery(fieldName, lower.doubleValue(), upper.doubleValue());
            }

            @Override
            SortField.Type getSortFieldType() {
                return SortField.Type.DOUBLE;
            }
        },
        UNSIGNED_LONG("unsigned_long_field", ApproximatePointRangeQuery.UNSIGNED_LONG_FORMAT) {
            @Override
            byte[] encode(Number value) {
                byte[] bytes = new byte[BigIntegerPoint.BYTES];
                BigIntegerPoint.encodeDimension(BigInteger.valueOf(value.longValue()), bytes, 0);
                return bytes;
            }

            @Override
            void addField(Document doc, String fieldName, Number value) {
                doc.add(new BigIntegerPoint(fieldName, BigInteger.valueOf(value.longValue())));
            }

            @Override
            void addDocValuesField(Document doc, String fieldName, Number value) {
                doc.add(new NumericDocValuesField(fieldName + "_sort", value.longValue()));
            }

            @Override
            Query rangeQuery(String fieldName, Number lower, Number upper) {
                return BigIntegerPoint.newRangeQuery(
                    fieldName,
                    BigInteger.valueOf(lower.longValue()),
                    BigInteger.valueOf(upper.longValue())
                );
            }

            @Override
            SortField.Type getSortFieldType() {
                return SortField.Type.LONG;
            }
        };

        final String fieldName;
        final Function<byte[], String> format;

        NumericType(String fieldName, Function<byte[], String> format) {
            this.fieldName = fieldName;
            this.format = format;
        }

        abstract byte[] encode(Number value);

        abstract void addField(Document doc, String fieldName, Number value);

        abstract void addDocValuesField(Document doc, String fieldName, Number value);

        abstract Query rangeQuery(String fieldName, Number lower, Number upper);

        abstract SortField.Type getSortFieldType();
    }

    public void testApproximateRangeEqualsActualRange() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                for (int i = 0; i < 100; i++) {
                    int numPoints = RandomNumbers.randomIntBetween(random(), 1, 10);
                    Document doc = new Document();
                    for (int j = 0; j < numPoints; j++) {
                        long randomValue = RandomNumbers.randomLongBetween(random(), 0, 100);
                        numericType.addField(doc, numericType.fieldName, randomValue);
                    }
                    iw.addDocument(doc);
                }
                iw.flush();
                try (IndexReader reader = iw.getReader()) {
                    long lower = RandomNumbers.randomLongBetween(random(), -100, 200);
                    long upper = lower + RandomNumbers.randomLongBetween(random(), 0, 100);
                    if (numericType == NumericType.UNSIGNED_LONG && lower < 0) {
                        lower = 0;
                        upper = RandomNumbers.randomLongBetween(random(), 0, 100);
                    }
                    Query approximateQuery = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        numericType.format
                    );
                    Query exactQuery = numericType.rangeQuery(numericType.fieldName, lower, upper);
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(approximateQuery, 10);
                    TopDocs topDocs1 = searcher.search(exactQuery, 10);
                    assertEquals(topDocs.totalHits, topDocs1.totalHits);
                }
            }
        }
    }

    public void testApproximateRangeWithDefaultSize() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    if (i % 15 == 0) iw.flush();
                }
                iw.flush();
                try (IndexReader reader = iw.getReader()) {
                    long lower = 0;
                    long upper = 1000;
                    Query approximateQuery = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        numericType.format
                    );
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(approximateQuery, 10);
                    assertEquals(topDocs.totalHits, new TotalHits(1000, TotalHits.Relation.EQUAL_TO));
                }
            }
        }
    }

    public void testApproximateRangeShortCircuitAscSort() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long lower = 0;
                    long upper = 20;
                    final int size = 10;
                    Query approximateQuery = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        size,
                        SortOrder.ASC,
                        numericType.format
                    );
                    Query exactQuery = numericType.rangeQuery(numericType.fieldName, lower, upper);
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort;
                    if (numericType == NumericType.HALF_FLOAT || numericType == NumericType.UNSIGNED_LONG) {
                        sort = new Sort(new SortField(numericType.fieldName + "_sort", numericType.getSortFieldType()));
                    } else {
                        sort = new Sort(new SortField(numericType.fieldName, numericType.getSortFieldType()));
                    }
                    TopDocs topDocs = searcher.search(approximateQuery, size, sort);
                    TopDocs topDocs1 = searcher.search(exactQuery, size, sort);
                    for (int i = 0; i < Math.min(6, topDocs.scoreDocs.length); i++) {
                        assertEquals(topDocs.scoreDocs[i].doc, topDocs1.scoreDocs[i].doc);
                    }
                }
            }
        }
    }

    public void testApproximateRangeShortCircuitDescSort() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long lower = 980;
                    long upper = 999;
                    final int size = 10;
                    Query approximateQuery = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        size,
                        SortOrder.DESC,
                        numericType.format
                    );
                    Query exactQuery = numericType.rangeQuery(numericType.fieldName, lower, upper);
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort;
                    if (numericType == NumericType.HALF_FLOAT || numericType == NumericType.UNSIGNED_LONG) {
                        sort = new Sort(new SortField(numericType.fieldName + "_sort", numericType.getSortFieldType(), true));
                    } else {
                        sort = new Sort(new SortField(numericType.fieldName, numericType.getSortFieldType(), true));
                    }
                    TopDocs topDocs = searcher.search(approximateQuery, size, sort);
                    TopDocs topDocs1 = searcher.search(exactQuery, size, sort);
                    // Verify we got the highest values first (DESC order)
                    for (int i = 0; i < size; i++) {
                        assertEquals("Mismatch at doc index " + i, topDocs.scoreDocs[i].doc, topDocs1.scoreDocs[i].doc);
                    }
                }
            }
        }
    }

    public void testApproximateRangeWithSizeDefault() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                final int size = 10;
                try (IndexReader reader = iw.getReader()) {
                    long lower = 0;
                    long upper = 45;
                    Query approximateQuery = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        size,
                        null,
                        numericType.format
                    );
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(approximateQuery, size);
                    assertEquals(size, topDocs.scoreDocs.length);
                }
            }
        }
    }

    public void testApproximateRangeWithSizeOverDefault() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = 15000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long lower = 0;
                    long upper = 12000;
                    long maxHits = 12001;
                    final int size = 11000;
                    Query approximateQuery = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        size,
                        null,
                        numericType.format
                    );
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(approximateQuery, size);
                    if (topDocs.totalHits.relation() == Relation.EQUAL_TO) {
                        assertEquals(topDocs.totalHits.value(), size);
                    } else {
                        assertTrue(11000 <= topDocs.totalHits.value());
                        assertTrue(maxHits >= topDocs.totalHits.value());
                    }
                }
            }
        }
    }

    public void testApproximateRangeShortCircuit() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = 1000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    if (i % 10 == 0) iw.flush();
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long lower = 0;
                    long upper = 100;
                    final int size = 10;
                    Query approximateQuery = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        size,
                        null,
                        numericType.format
                    );
                    Query exactQuery = numericType.rangeQuery(numericType.fieldName, lower, upper);
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(approximateQuery, size);
                    TopDocs topDocs1 = searcher.search(exactQuery, size);
                    assertEquals(size, topDocs.scoreDocs.length);
                    assertEquals(size, topDocs1.scoreDocs.length);
                    assertEquals(topDocs1.totalHits.value(), 101);
                }
            }
        }
    }

    public void testSize() {
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(0),
            numericType.encode(20),
            1,
            numericType.format
        );
        assertEquals(query.getSize(), 10_000);
        query.setSize(100);
        assertEquals(query.getSize(), 100);
    }

    public void testSortOrder() {
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(0),
            numericType.encode(20),
            1,
            numericType.format
        );
        assertNull(query.getSortOrder());
        query.setSortOrder(SortOrder.ASC);
        assertEquals(query.getSortOrder(), SortOrder.ASC);
    }

    public void testCanApproximate() {
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(0),
            numericType.encode(20),
            1,
            numericType.format
        );
        assertFalse(query.canApproximate(null));
        ApproximatePointRangeQuery queryCanApproximate = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(0),
            numericType.encode(20),
            1,
            numericType.format
        ) {
            public boolean canApproximate(SearchContext context) {
                return true;
            }
        };
        SearchContext searchContext = org.mockito.Mockito.mock(SearchContext.class);
        assertTrue(queryCanApproximate.canApproximate(searchContext));
    }

    public void testCannotApproximateWithTrackTotalHits() {
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(0),
            numericType.encode(20),
            1,
            numericType.format
        );
        SearchContext mockContext = mock(org.opensearch.search.internal.SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        assertFalse(query.canApproximate(mockContext));
        when(mockContext.trackTotalHitsUpTo()).thenReturn(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        when(mockContext.aggregations()).thenReturn(null);
        when(mockContext.from()).thenReturn(0);
        when(mockContext.size()).thenReturn(10);
        when(mockContext.request()).thenReturn(null);
        assertTrue(query.canApproximate(mockContext));
    }

    // Test to cover the left child traversal in intersectRight with CELL_INSIDE_QUERY
    public void testIntersectRightLeftChildTraversal() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                // Create a dataset that will create a multi-level BKD tree
                int numPoints = 2000;
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    if (i % 100 == 0) {
                        iw.flush();
                    }
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long lower = 1000;
                    long upper = 1999;
                    final int size = 50;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        50,
                        SortOrder.DESC,
                        numericType.format
                    );
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort;
                    if (numericType == NumericType.HALF_FLOAT || numericType == NumericType.UNSIGNED_LONG) {
                        sort = new Sort(new SortField(numericType.fieldName + "_sort", numericType.getSortFieldType(), true));
                    } else {
                        sort = new Sort(new SortField(numericType.fieldName, numericType.getSortFieldType(), true));
                    }
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
                // Create a smaller dataset that will result in leaf nodes that are completely inside the query range
                for (int i = 900; i <= 999; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long lower = 800;
                    long upper = 1100;
                    final int size = 200;
                    final int returnSize = 100;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        200,
                        SortOrder.DESC,
                        numericType.format
                    );
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort;
                    if (numericType == NumericType.HALF_FLOAT || numericType == NumericType.UNSIGNED_LONG) {
                        sort = new Sort(new SortField(numericType.fieldName + "_sort", numericType.getSortFieldType(), true));
                    } else {
                        sort = new Sort(new SortField(numericType.fieldName, numericType.getSortFieldType(), true));
                    }
                    TopDocs topDocs = searcher.search(query, size, sort);
                    assertEquals("Should find all documents", returnSize, topDocs.totalHits.value());
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
                // Create documents in two separate ranges to ensure some cells are outside query
                // Range 1: 0-99
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                // Range 2: 500-599 (gap ensures some tree nodes will be completely outside query)
                for (int i = 500; i < 600; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long lower = 200;
                    long upper = 400;
                    final int size = 50;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        size,
                        SortOrder.DESC,
                        numericType.format
                    );
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort;
                    if (numericType == NumericType.HALF_FLOAT || numericType == NumericType.UNSIGNED_LONG) {
                        sort = new Sort(new SortField(numericType.fieldName + "_sort", numericType.getSortFieldType(), true));
                    } else {
                        sort = new Sort(new SortField(numericType.fieldName, numericType.getSortFieldType(), true));
                    }
                    TopDocs topDocs = searcher.search(query, size, sort);
                    // Should find no documents since our query range (200-400) has no documents
                    assertEquals("Should find no documents in the gap range", 0, topDocs.totalHits.value());
                }
            }
        }
    }

    // Test to cover intersectRight with CELL_CROSSES_QUERY case
    public void testIntersectRightCellCrossesQuery() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                // Create documents that will result in cells that cross the query boundary
                for (int i = 0; i < 1000; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    // Query that will partially overlap with tree nodes (CELL_CROSSES_QUERY)
                    long lower = 250;
                    long upper = 750;
                    final int size = 100;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        100,
                        SortOrder.DESC,
                        numericType.format
                    );
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort;
                    if (numericType == NumericType.HALF_FLOAT || numericType == NumericType.UNSIGNED_LONG) {
                        sort = new Sort(new SortField(numericType.fieldName + "_sort", numericType.getSortFieldType(), true));
                    } else {
                        sort = new Sort(new SortField(numericType.fieldName, numericType.getSortFieldType(), true));
                    }
                    TopDocs topDocs = searcher.search(query, size, sort);
                    assertEquals("Should return exactly size value documents", size, topDocs.scoreDocs.length);
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
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, 1000);
                    numericType.addDocValuesField(doc, numericType.fieldName, 1000);
                    iw.addDocument(doc);
                }
                Document doc = new Document();
                numericType.addField(doc, numericType.fieldName, 987654321L);
                numericType.addDocValuesField(doc, numericType.fieldName, 987654321L);
                iw.addDocument(doc);
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    long lower = 500L;
                    long upper = 999999999L;
                    final int size = 50;
                    ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        size,
                        SortOrder.DESC,
                        numericType.format
                    );
                    IndexSearcher searcher = new IndexSearcher(reader);
                    Sort sort;
                    if (numericType == NumericType.HALF_FLOAT || numericType == NumericType.UNSIGNED_LONG) {
                        sort = new Sort(new SortField(numericType.fieldName + "_sort", numericType.getSortFieldType(), true));
                    } else {
                        sort = new Sort(new SortField(numericType.fieldName, numericType.getSortFieldType(), true));
                    }
                    TopDocs topDocs = searcher.search(query, size, sort);
                    assertEquals("Should return exactly size value documents", size, topDocs.scoreDocs.length);
                }
            }
        }
    }

    public void testHttpLogTimestampDistribution() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                String fieldName = numericType.fieldName;
                // Sparse range: 100-199 (100 docs, one per value)
                for (int i = 100; i < 200; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, fieldName, i);
                    numericType.addDocValuesField(doc, fieldName, i);
                    iw.addDocument(doc);
                }
                // Dense range: 1000-1999 (5000 docs, 5 per value)
                for (int i = 0; i < 5000; i++) {
                    long value = 1000 + (i / 5); // Creates 5 docs per value from 1000-1999
                    Document doc = new Document();
                    numericType.addField(doc, fieldName, value);
                    numericType.addDocValuesField(doc, fieldName, value);
                    iw.addDocument(doc);
                }
                // 0-99 (100 docs)
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, fieldName, i);
                    numericType.addDocValuesField(doc, fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    // Test sparse region
                    testApproximateVsExactQuery(searcher, fieldName, 100, 199, 50, dims);
                    // Test dense region
                    testApproximateVsExactQuery(searcher, fieldName, 1000, 1500, 100, dims);
                    // Test across regions
                    testApproximateVsExactQuery(searcher, fieldName, 0, 2000, 200, dims);
                }
            }
        }
    }

    // Following test replicates the nyx_taxis dataset
    public void testNycTaxiDataDistribution() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                String fieldName = numericType.fieldName;
                // Create NYC taxi fare distribution with different ranges
                // Low fares: 250-500 (sparse)
                for (long fare = 250; fare <= 500; fare++) {
                    Document doc = new Document();
                    numericType.addField(doc, fieldName, fare);
                    numericType.addDocValuesField(doc, fieldName, fare);
                    iw.addDocument(doc);
                }
                // Typical fares: 1000-3000 (dense, 5 docs per value)
                for (long fare = 1000; fare <= 3000; fare++) {
                    for (int dup = 0; dup < 5; dup++) {
                        Document doc = new Document();
                        numericType.addField(doc, fieldName, fare);
                        numericType.addDocValuesField(doc, fieldName, fare);
                        iw.addDocument(doc);
                    }
                }
                // High fares: 10000-20000 (sparse, 1 doc every 100)
                for (long fare = 10000; fare <= 20000; fare += 100) {
                    Document doc = new Document();
                    numericType.addField(doc, fieldName, fare);
                    numericType.addDocValuesField(doc, fieldName, fare);
                    iw.addDocument(doc);
                }
                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    // Test 1: Query for typical fare range
                    testApproximateVsExactQuery(searcher, fieldName, 1000, 3000, 100, dims);
                    // Test 2: Query for high fare range
                    testApproximateVsExactQuery(searcher, fieldName, 10000, 20000, 50, dims);
                    // Test 3: Query for low fares
                    testApproximateVsExactQuery(searcher, fieldName, 250, 500, 50, dims);
                }
            }
        }
    }

    private void testApproximateVsExactQuery(IndexSearcher searcher, String field, long lower, long upper, int size, int dims)
        throws IOException {
        // Test with approximate query
        ApproximatePointRangeQuery approxQuery = new ApproximatePointRangeQuery(
            field,
            numericType.encode(lower),
            numericType.encode(upper),
            dims,
            size,
            null,
            numericType.format
        );
        // Test with exact query
        Query exactQuery = numericType.rangeQuery(field, lower, upper);
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
        Sort ascSort;
        Sort descSort;
        if (numericType == NumericType.HALF_FLOAT || numericType == NumericType.UNSIGNED_LONG) {
            ascSort = new Sort(new SortField(field + "_sort", numericType.getSortFieldType()));
            descSort = new Sort(new SortField(field + "_sort", numericType.getSortFieldType(), true));
        } else {
            ascSort = new Sort(new SortField(field, numericType.getSortFieldType()));
            descSort = new Sort(new SortField(field, numericType.getSortFieldType(), true));
        }
        // Test ASC sort
        ApproximatePointRangeQuery approxQueryAsc = new ApproximatePointRangeQuery(
            field,
            numericType.encode(lower),
            numericType.encode(upper),
            dims,
            size,
            SortOrder.ASC,
            numericType.format
        );
        TopDocs approxDocsAsc = searcher.search(approxQueryAsc, size, ascSort);
        TopDocs exactDocsAsc = searcher.search(exactQuery, size, ascSort);
        // Verify results match
        int compareSize = Math.min(size, Math.min(approxDocsAsc.scoreDocs.length, exactDocsAsc.scoreDocs.length));
        for (int i = 0; i < compareSize; i++) {
            assertEquals("ASC sorted results should match at position " + i, exactDocsAsc.scoreDocs[i].doc, approxDocsAsc.scoreDocs[i].doc);
        }
        assertEquals("Should return same number of documents", exactDocsAsc.scoreDocs.length, approxDocsAsc.scoreDocs.length);
        assertEquals("Should return exactly size value documents", size, approxDocsAsc.scoreDocs.length);
        assertEquals(
            "Should return exactly size value documents as regular query",
            exactDocsAsc.scoreDocs.length,
            approxDocsAsc.scoreDocs.length
        );
        // Test DESC sort
        ApproximatePointRangeQuery approxQueryDesc = new ApproximatePointRangeQuery(
            field,
            numericType.encode(lower),
            numericType.encode(upper),
            dims,
            size,
            SortOrder.DESC,
            numericType.format
        );
        TopDocs approxDocsDesc = searcher.search(approxQueryDesc, size, descSort);
        TopDocs exactDocsDesc = searcher.search(exactQuery, size, descSort);
        // Verify the results match
        compareSize = Math.min(size, Math.min(approxDocsDesc.scoreDocs.length, exactDocsDesc.scoreDocs.length));
        for (int i = 0; i < compareSize; i++) {
            assertEquals(
                "DESC sorted results should match at position " + i,
                exactDocsDesc.scoreDocs[i].doc,
                approxDocsDesc.scoreDocs[i].doc
            );
        }
        assertEquals("Should return same number of documents", exactDocsDesc.scoreDocs.length, approxDocsDesc.scoreDocs.length);
        assertEquals("Should return exactly size value documents", size, approxDocsAsc.scoreDocs.length);
        assertEquals(
            "Should return exactly size value documents as regular query",
            exactDocsAsc.scoreDocs.length,
            approxDocsAsc.scoreDocs.length
        );
    }
}
