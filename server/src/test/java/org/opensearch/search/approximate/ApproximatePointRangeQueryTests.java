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
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.index.mapper.DateFieldMapper.DateFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
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

            @Override
            NumberFieldMapper.NumberType getNumberType() {
                return NumberFieldMapper.NumberType.INTEGER;
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

            @Override
            NumberFieldMapper.NumberType getNumberType() {
                return NumberFieldMapper.NumberType.LONG;
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
            String getSortFieldName() {
                return fieldName + "_sort";
            }

            @Override
            Query rangeQuery(String fieldName, Number lower, Number upper) {
                return HalfFloatPoint.newRangeQuery(fieldName, lower.floatValue(), upper.floatValue());
            }

            @Override
            SortField.Type getSortFieldType() {
                return SortField.Type.LONG;
            }

            @Override
            NumberFieldMapper.NumberType getNumberType() {
                return NumberFieldMapper.NumberType.HALF_FLOAT;
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

            @Override
            NumberFieldMapper.NumberType getNumberType() {
                return NumberFieldMapper.NumberType.FLOAT;
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

            @Override
            NumberFieldMapper.NumberType getNumberType() {
                return NumberFieldMapper.NumberType.DOUBLE;
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

            @Override
            String getSortFieldName() {
                return fieldName + "_sort";
            }

            @Override
            NumberFieldMapper.NumberType getNumberType() {
                return NumberFieldMapper.NumberType.UNSIGNED_LONG;
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

        abstract NumberFieldMapper.NumberType getNumberType();

        String getSortFieldName() {
            return fieldName;
        }

        long getMinTestValue() {
            return this == UNSIGNED_LONG ? 0 : -100;
        }
    }

    public void testApproximateRangeEqualsActualRange() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numDocs = RandomNumbers.randomIntBetween(random(), 1500, 3000);
                for (int i = 0; i < numDocs; i++) {
                    int numValues = RandomNumbers.randomIntBetween(random(), 1, 10);
                    Document doc = new Document();
                    for (int j = 0; j < numValues; j++) {
                        long randomValue = RandomNumbers.randomLongBetween(random(), 0, 200);
                        numericType.addField(doc, numericType.fieldName, randomValue);
                    }
                    iw.addDocument(doc);
                    if (random().nextInt(20) == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    long lower = RandomNumbers.randomLongBetween(random(), numericType.getMinTestValue(), 200);
                    long upper = RandomNumbers.randomLongBetween(random(), lower, lower + 150);
                    int searchSize = RandomNumbers.randomIntBetween(random(), 10, 50);
                    Query approximateQuery = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        numericType.format
                    );
                    Query exactQuery = numericType.rangeQuery(numericType.fieldName, lower, upper);
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(approximateQuery, searchSize);
                    TopDocs topDocs1 = searcher.search(exactQuery, searchSize);
                    assertEquals(
                        "Approximate and exact queries should return same number of docs",
                        topDocs1.scoreDocs.length,
                        topDocs.scoreDocs.length
                    );
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
                int numPoints = RandomNumbers.randomIntBetween(random(), 1000, 3000);
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    // Randomly flush
                    if (random().nextInt(20) == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    long lower = RandomNumbers.randomLongBetween(random(), 0, 50);
                    long upper = RandomNumbers.randomLongBetween(random(), lower + 10, Math.min(lower + 100, numPoints - 1));
                    final int size = RandomNumbers.randomIntBetween(random(), 5, 20);
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
                    Sort sort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType()));
                    TopDocs topDocs = searcher.search(approximateQuery, size, sort);
                    TopDocs topDocs1 = searcher.search(exactQuery, size, sort);
                    int compareSize = Math.min(size, Math.min(topDocs.scoreDocs.length, topDocs1.scoreDocs.length));
                    for (int i = 0; i < compareSize; i++) {
                        assertEquals("Mismatch at doc index " + i, topDocs.scoreDocs[i].doc, topDocs1.scoreDocs[i].doc);
                    }
                }
            }
        }
    }

    public void testApproximateRangeShortCircuitDescSort() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = RandomNumbers.randomIntBetween(random(), 1000, 3000);
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    if (random().nextInt(20) == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    long lower = RandomNumbers.randomLongBetween(random(), numPoints - 100, numPoints - 20);
                    long upper = RandomNumbers.randomLongBetween(random(), lower + 10, numPoints - 1);
                    final int size = RandomNumbers.randomIntBetween(random(), 5, 20);
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
                    Sort sort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType(), true));
                    TopDocs topDocs = searcher.search(approximateQuery, size, sort);
                    TopDocs topDocs1 = searcher.search(exactQuery, size, sort);
                    int compareSize = Math.min(size, Math.min(topDocs.scoreDocs.length, topDocs1.scoreDocs.length));
                    for (int i = 0; i < compareSize; i++) {
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
                int numPoints = RandomNumbers.randomIntBetween(random(), 1000, 3000);
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    if (random().nextInt(20) == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                final int size = RandomNumbers.randomIntBetween(random(), 5, 20);
                try (IndexReader reader = iw.getReader()) {
                    long lower = RandomNumbers.randomLongBetween(random(), 0, numPoints / 2);
                    long upper = RandomNumbers.randomLongBetween(random(), lower + size * 2, numPoints - 1);
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
                int numPoints = RandomNumbers.randomIntBetween(random(), 15000, 20000);
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    if (random().nextInt(100) == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    final int size = RandomNumbers.randomIntBetween(random(), 11000, 13000);
                    long lower = RandomNumbers.randomLongBetween(random(), 0, 1000);
                    long upper = RandomNumbers.randomLongBetween(random(), lower + size, Math.min(lower + size + 2000, numPoints - 1));
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
                        assertTrue(size <= topDocs.totalHits.value());
                    }
                }
            }
        }
    }

    public void testApproximateRangeShortCircuit() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = RandomNumbers.randomIntBetween(random(), 3000, 10000);
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    if (random().nextInt(20) == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    final int size = RandomNumbers.randomIntBetween(random(), 5, 20);
                    long lower = RandomNumbers.randomLongBetween(random(), 0, numPoints / 2);
                    long upper = RandomNumbers.randomLongBetween(random(), lower + size * 2, Math.min(lower + 200, numPoints - 1));
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
                    TopDocs exactAllDocs = searcher.search(exactQuery, numPoints);
                    long actualHitsInRange = exactAllDocs.totalHits.value();
                    TopDocs topDocs = searcher.search(approximateQuery, size);
                    TopDocs topDocs1 = searcher.search(exactQuery, size);
                    assertEquals(size, topDocs.scoreDocs.length);
                    assertEquals(size, topDocs1.scoreDocs.length);
                    assertEquals(actualHitsInRange, topDocs1.totalHits.value());
                    assertTrue("Approximate query should find at least 'size' documents", topDocs.totalHits.value() >= size);
                }
            }
        }
    }

    public void testSize() {
        long lower = RandomNumbers.randomLongBetween(random(), 0, 100);
        long upper = RandomNumbers.randomLongBetween(random(), lower + 10, lower + 1000);
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(lower),
            numericType.encode(upper),
            1,
            numericType.format
        );
        assertEquals(query.getSize(), 10_000);
        int newSize = RandomNumbers.randomIntBetween(random(), 50, 500);
        query.setSize(newSize);
        assertEquals(query.getSize(), newSize);
    }

    public void testSortOrder() {
        long lower = RandomNumbers.randomLongBetween(random(), 0, 100);
        long upper = RandomNumbers.randomLongBetween(random(), lower + 10, lower + 1000);
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(lower),
            numericType.encode(upper),
            1,
            numericType.format
        );
        assertNull(query.getSortOrder());
        SortOrder sortOrder = random().nextBoolean() ? SortOrder.ASC : SortOrder.DESC;
        query.setSortOrder(sortOrder);
        assertEquals(query.getSortOrder(), sortOrder);
    }

    public void testCanApproximate() {
        long lower = RandomNumbers.randomLongBetween(random(), 0, 100);
        long upper = RandomNumbers.randomLongBetween(random(), lower + 10, lower + 1000);
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(lower),
            numericType.encode(upper),
            1,
            numericType.format
        );
        assertFalse(query.canApproximate(null));
        ApproximatePointRangeQuery queryCanApproximate = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(lower),
            numericType.encode(upper),
            1,
            numericType.format
        ) {
            public boolean canApproximate(SearchContext context) {
                return true;
            }
        };
        SearchContext searchContext = mock(SearchContext.class);
        assertTrue(queryCanApproximate.canApproximate(searchContext));
    }

    public void testCannotApproximateWithTrackTotalHits() {
        long lower = RandomNumbers.randomLongBetween(random(), 0, 100);
        long upper = RandomNumbers.randomLongBetween(random(), lower + 10, lower + 1000);
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(lower),
            numericType.encode(upper),
            1,
            numericType.format
        );
        SearchContext mockContext = mock(org.opensearch.search.internal.SearchContext.class);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        assertFalse(query.canApproximate(mockContext));
        int trackTotalHitsUpTo = RandomNumbers.randomIntBetween(random(), 1000, 20000);
        when(mockContext.trackTotalHitsUpTo()).thenReturn(trackTotalHitsUpTo);
        when(mockContext.aggregations()).thenReturn(null);
        int from = RandomNumbers.randomIntBetween(random(), 0, 100);
        int size = RandomNumbers.randomIntBetween(random(), 10, 100);
        when(mockContext.from()).thenReturn(from);
        when(mockContext.size()).thenReturn(size);
        when(mockContext.request()).thenReturn(null);
        assertTrue(query.canApproximate(mockContext));
        int expectedSize = Math.max(from + size, trackTotalHitsUpTo) + 1;
        assertEquals(expectedSize, query.getSize());
    }

    // Test to cover the left child traversal in intersectRight with CELL_INSIDE_QUERY
    public void testIntersectRightLeftChildTraversal() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = RandomNumbers.randomIntBetween(random(), 2000, 5000);
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    if (i % RandomNumbers.randomIntBetween(random(), 50, 200) == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    // To test upper half of the data to test the DESC
                    long lower = RandomNumbers.randomLongBetween(random(), numPoints / 2, numPoints - 200);
                    long upper = RandomNumbers.randomLongBetween(random(), lower + 100, numPoints - 1);
                    final int size = RandomNumbers.randomIntBetween(random(), 30, 100);
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
                    Sort sort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType(), true));
                    TopDocs topDocs = searcher.search(query, size, sort);
                    assertEquals(
                        "Should return exactly min(size, hits) documents",
                        (int) Math.min(size, (upper - lower + 1)),
                        topDocs.scoreDocs.length
                    );
                }
            }
        }
    }

    // Test to cover pointTree.visitDocIDs(visitor) in CELL_INSIDE_QUERY leaf case for intersectRight
    public void testIntersectRightCellInsideQueryLeaf() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int dataStart = RandomNumbers.randomIntBetween(random(), 5000, 15000);
                int dataEnd = dataStart + RandomNumbers.randomIntBetween(random(), 1000, 2000);
                int numDocs = dataEnd - dataStart + 1;
                for (int i = dataStart; i <= dataEnd; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    // Create a query range that fully encompasses the data
                    // This ensures CELL_INSIDE_QUERY condition
                    long lower = dataStart - RandomNumbers.randomIntBetween(random(), 50, 200);
                    long upper = dataEnd + RandomNumbers.randomIntBetween(random(), 50, 200);
                    final int size = RandomNumbers.randomIntBetween(random(), numDocs + 50, numDocs + 200);
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
                    Sort sort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType(), true));
                    TopDocs topDocs = searcher.search(query, size, sort);
                    assertEquals("Should find all documents", numDocs, topDocs.totalHits.value());
                    assertEquals("Should return exactly all documents", numDocs, topDocs.scoreDocs.length);
                }
            }
        }
    }

    // Test to cover CELL_OUTSIDE_QUERY break case for intersectRight
    public void testIntersectRightCellOutsideQuery() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int range1Start = RandomNumbers.randomIntBetween(random(), 0, 1000);
                int range1End = range1Start + RandomNumbers.randomIntBetween(random(), 1000, 15000);
                for (int i = range1Start; i <= range1End; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                int gapSize = RandomNumbers.randomIntBetween(random(), 200, 400);
                int range2Start = range1End + gapSize;
                int range2End = range2Start + RandomNumbers.randomIntBetween(random(), 50, 150);
                for (int i = range2Start; i <= range2End; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    long lower = range1End + RandomNumbers.randomIntBetween(random(), 10, gapSize / 2);
                    long upper = range2Start - RandomNumbers.randomIntBetween(random(), 10, gapSize / 2);
                    final int size = RandomNumbers.randomIntBetween(random(), 20, 100);

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
                    Sort sort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType(), true));
                    TopDocs topDocs = searcher.search(query, size, sort);
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
                int numPoints = RandomNumbers.randomIntBetween(random(), 1000, 3000);
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    if (random().nextInt(100) == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    // Query that will partially overlap with tree nodes (CELL_CROSSES_QUERY)
                    long lower = RandomNumbers.randomLongBetween(random(), numPoints / 4, numPoints / 2);
                    long upper = RandomNumbers.randomLongBetween(random(), numPoints / 2, 3 * numPoints / 4);
                    final int size = RandomNumbers.randomIntBetween(random(), 50, 200);
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
                    Sort sort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType(), true));
                    TopDocs topDocs = searcher.search(query, size, sort);
                    long expectedHits = upper - lower + 1;
                    int expectedReturnSize = (int) Math.min(size, expectedHits);
                    assertEquals("Should return min(size, hits) documents", expectedReturnSize, topDocs.scoreDocs.length);
                    assertTrue("Should collect at least min(size, hits) documents", topDocs.totalHits.value() >= expectedReturnSize);
                }
            }
        }
    }

    // Test to specifically cover the single child case in intersectRight
    public void testIntersectRightSingleChildNode() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numSameValueDocs = RandomNumbers.randomIntBetween(random(), 1000, 3000);
                long sameValue = RandomNumbers.randomLongBetween(random(), 500, 2000);
                for (int i = 0; i < numSameValueDocs; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, sameValue);
                    numericType.addDocValuesField(doc, numericType.fieldName, sameValue);
                    iw.addDocument(doc);
                }
                long highValue = RandomNumbers.randomLongBetween(random(), 900000000L, 999999999L);
                Document doc = new Document();
                numericType.addField(doc, numericType.fieldName, highValue);
                numericType.addDocValuesField(doc, numericType.fieldName, highValue);
                iw.addDocument(doc);
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    long lower = RandomNumbers.randomLongBetween(random(), 0, sameValue - 100);
                    long upper = RandomNumbers.randomLongBetween(random(), highValue, highValue + 1000000L);
                    final int size = RandomNumbers.randomIntBetween(random(), 20, 100);
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
                    Sort sort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType(), true));
                    TopDocs topDocs = searcher.search(query, size, sort);
                    int totalDocs = numSameValueDocs + 1;
                    int expectedReturnSize = Math.min(size, totalDocs);
                    assertEquals("Should return min(size, totalDocs) documents", expectedReturnSize, topDocs.scoreDocs.length);
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

    public void testDateRangeIncludingNowQueryApproximation() throws IOException {
        if (numericType != NumericType.LONG) {
            return;
        }
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;

                DateFieldType dateFieldType = new DateFieldType("@timestamp");
                long minute = 60 * 1000L;

                long currentTime = System.currentTimeMillis();
                long startTimestamp = currentTime - (48 * 60 * minute); // Start 48 hours ago

                // Create 10000 documents with 1 minute intervals
                for (int i = 0; i < 10000; i++) {
                    long timestamp = startTimestamp + (i * minute);

                    long convertedTimestamp = dateFieldType.resolution().convert(Instant.ofEpochMilli(timestamp));

                    Document doc = new Document();
                    doc.add(new LongPoint(dateFieldType.name(), convertedTimestamp));
                    doc.add(new NumericDocValuesField(dateFieldType.name(), convertedTimestamp));
                    iw.addDocument(doc);
                }

                iw.flush();
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);

                    testApproximateVsExactQueryWithDateField(searcher, dateFieldType, "now-1h", "now", 50, dims);
                    testApproximateVsExactQueryWithDateField(searcher, dateFieldType, "now-1d", "now", 50, dims);
                    testApproximateVsExactQueryWithDateField(searcher, dateFieldType, "now-30m", "now+30m", 50, dims);
                }
            }
        }
    }

    public void testApproximateVsExactQueryWithDateField(
        IndexSearcher searcher,
        DateFieldType dateFieldType,
        String lowerBound,
        String upperBound,
        int size,
        int dims
    ) throws IOException {
        DateFormatter formatter = dateFieldType.dateTimeFormatter();
        DateMathParser parser = formatter.toDateMathParser();
        long nowInMillis = System.currentTimeMillis();

        // Parse the date expressions using the DateFieldType's resolution
        long lowerMillis = dateFieldType.resolution().convert(parser.parse(lowerBound, () -> nowInMillis));

        long upperMillis = dateFieldType.resolution().convert(parser.parse(upperBound, () -> nowInMillis));

        testApproximateVsExactQuery(searcher, dateFieldType.name(), lowerMillis, upperMillis, size, dims);
    }

    private void testApproximateVsExactQuery(IndexSearcher searcher, String field, Number lower, Number upper, int size, int dims)
        throws IOException {
        // Test with approximate query
        byte[] lowerBytes = numericType.encode(lower);
        byte[] upperBytes = numericType.encode(upper);
        Function<byte[], String> format = numericType.format;

        Query exactQuery = numericType.rangeQuery(field, lower, upper);

        if (field.equals("@timestamp")) {

            // Use NumericType.LONG for date fields
            lowerBytes = LongPoint.pack((long) lower).bytes;
            upperBytes = LongPoint.pack((long) upper).bytes;
            format = ApproximatePointRangeQuery.LONG_FORMAT;
            exactQuery = LongPoint.newRangeQuery(field, (long) lower, (long) upper);
        }

        ApproximatePointRangeQuery approxQuery = new ApproximatePointRangeQuery(field, upperBytes, lowerBytes, dims, size, null, format);
        // Test with exact query
        TopDocs approxDocs = searcher.search(approxQuery, size);
        TopDocs exactDocs = searcher.search(exactQuery, size);

        verifyRangeQueries(searcher, exactQuery, approxDocs, exactDocs, field, lower, upper, size, dims);
    }

    private void verifyRangeQueries(
        IndexSearcher searcher,
        Query exactQuery,
        TopDocs approxDocs,
        TopDocs exactDocs,
        String field,
        Number lower,
        Number upper,
        int size,
        int dims
    ) throws IOException {

        byte[] lowerBytes = numericType.encode(lower);
        byte[] upperBytes = numericType.encode(upper);
        Function<byte[], String> format = numericType.format;
        ;

        // Test with sorting (ASC and DESC)
        Sort ascSort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType()));
        Sort descSort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType(), true));

        if (field.equals("@timestamp")) {
            // Use NumericType.LONG for date fields
            ascSort = new Sort(new SortField(field, SortField.Type.LONG));
            descSort = new Sort(new SortField(field, SortField.Type.LONG, true));
        }

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

        // Test ASC sort
        ApproximatePointRangeQuery approxQueryAsc = new ApproximatePointRangeQuery(
            field,
            lowerBytes,
            upperBytes,
            dims,
            size,
            SortOrder.ASC,
            format
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
            lowerBytes,
            upperBytes,
            dims,
            size,
            SortOrder.DESC,
            format
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

    public void testApproximateWithSort() {
        long lower = RandomNumbers.randomLongBetween(random(), 0, 100);
        long upper = RandomNumbers.randomLongBetween(random(), lower + 10, lower + 1000);
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery(
            numericType.fieldName,
            numericType.encode(lower),
            numericType.encode(upper),
            1,
            numericType.format
        );
        // Test 1: Multiple sorts should prevent approximation
        {
            SearchContext mockContext = mock(SearchContext.class);
            ShardSearchRequest mockRequest = mock(ShardSearchRequest.class);
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.sort(new FieldSortBuilder(numericType.fieldName).order(SortOrder.ASC));
            source.sort(new FieldSortBuilder("another_field").order(SortOrder.ASC));
            source.terminateAfter(SearchContext.DEFAULT_TERMINATE_AFTER);
            when(mockContext.aggregations()).thenReturn(null);
            when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
            when(mockContext.from()).thenReturn(0);
            when(mockContext.size()).thenReturn(10);
            when(mockContext.request()).thenReturn(mockRequest);
            when(mockRequest.source()).thenReturn(source);
            assertFalse("Should not approximate with multiple sorts", query.canApproximate(mockContext));
        }
        // Test 2: Single sort on the same field should allow approximation
        {
            SearchContext mockContext = mock(SearchContext.class);
            ShardSearchRequest mockRequest = mock(ShardSearchRequest.class);
            SearchSourceBuilder source = new SearchSourceBuilder();
            source.sort(new FieldSortBuilder(numericType.fieldName).order(SortOrder.ASC));
            source.terminateAfter(SearchContext.DEFAULT_TERMINATE_AFTER);
            when(mockContext.aggregations()).thenReturn(null);
            when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
            when(mockContext.from()).thenReturn(0);
            when(mockContext.size()).thenReturn(10);
            when(mockContext.request()).thenReturn(mockRequest);
            when(mockRequest.source()).thenReturn(source);
            assertTrue("Should approximate with single sort on same field", query.canApproximate(mockContext));
        }
    }

    public void testApproximateRangeWithSearchAfterAsc() throws IOException {
        testApproximateRangeWithSearchAfter(SortOrder.ASC);
    }

    public void testApproximateRangeWithSearchAfterDesc() throws IOException {
        testApproximateRangeWithSearchAfter(SortOrder.DESC);
    }

    private void testApproximateRangeWithSearchAfter(SortOrder sortOrder) throws IOException {
        if (numericType == NumericType.HALF_FLOAT) {
            // Skip - HALF_FLOAT uses different fields for storage vs sorting which causes issues with search_after boundary checking during
            // tests
            return;
        }
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer())) {
                int dims = 1;
                int numPoints = RandomNumbers.randomIntBetween(random(), 2000, 5000);
                for (int i = 0; i < numPoints; i++) {
                    Document doc = new Document();
                    numericType.addField(doc, numericType.fieldName, i);
                    numericType.addDocValuesField(doc, numericType.fieldName, i);
                    iw.addDocument(doc);
                    if (random().nextInt(20) == 0) {
                        iw.flush();
                    }
                }
                iw.flush();
                if (random().nextBoolean()) {
                    iw.forceMerge(1);
                }
                try (IndexReader reader = iw.getReader()) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    long lower = RandomNumbers.randomLongBetween(random(), 0, numPoints / 4);
                    long upper = RandomNumbers.randomLongBetween(random(), 3 * numPoints / 4, numPoints - 1);
                    int size = RandomNumbers.randomIntBetween(random(), 10, 50);
                    long searchAfterValue = RandomNumbers.randomLongBetween(random(), lower, upper - size);
                    // First, get a document at searchAfterValue to use as the searchAfter point
                    Query exactValueQuery = numericType.rangeQuery(numericType.fieldName, searchAfterValue, searchAfterValue);
                    boolean reverseSort = sortOrder == SortOrder.DESC;
                    Sort sort = new Sort(new SortField(numericType.getSortFieldName(), numericType.getSortFieldType(), reverseSort));
                    TopDocs searchAfterDocs = searcher.search(exactValueQuery, 1, sort);
                    FieldDoc searchAfterDoc = (FieldDoc) searchAfterDocs.scoreDocs[0];
                    // Create mock context for approximate query
                    SearchContext mockContext = mock(SearchContext.class);
                    ShardSearchRequest mockRequest = mock(ShardSearchRequest.class);
                    SearchSourceBuilder source = new SearchSourceBuilder();
                    source.sort(new FieldSortBuilder(numericType.fieldName).order(sortOrder));
                    source.searchAfter(searchAfterDoc.fields);
                    when(mockContext.aggregations()).thenReturn(null);
                    when(mockContext.trackTotalHitsUpTo()).thenReturn(10000);
                    when(mockContext.from()).thenReturn(0);
                    when(mockContext.size()).thenReturn(size);
                    when(mockContext.request()).thenReturn(mockRequest);
                    when(mockRequest.source()).thenReturn(source);
                    NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                        numericType.fieldName,
                        numericType.getNumberType()
                    );
                    QueryShardContext queryShardContext = mock(QueryShardContext.class);
                    when(queryShardContext.fieldMapper(numericType.fieldName)).thenReturn(fieldType);
                    when(mockContext.getQueryShardContext()).thenReturn(queryShardContext);
                    // Test approximate query with searchAfter
                    ApproximatePointRangeQuery approxQuery = new ApproximatePointRangeQuery(
                        numericType.fieldName,
                        numericType.encode(lower),
                        numericType.encode(upper),
                        dims,
                        size,
                        sortOrder,
                        numericType.format
                    );
                    assertTrue("Should be able to approximate", approxQuery.canApproximate(mockContext));
                    TopDocs approxDocs = searcher.search(approxQuery, size, sort);
                    // Compare with exact query using Lucene's searchAfter
                    Query exactQuery = numericType.rangeQuery(numericType.fieldName, lower, upper);
                    TopDocs exactDocs = searcher.searchAfter(searchAfterDoc, exactQuery, size, sort);
                    // Verify results match
                    assertEquals(
                        "Approximate and exact queries should return same number of docs",
                        exactDocs.scoreDocs.length,
                        approxDocs.scoreDocs.length
                    );
                    for (int i = 0; i < Math.min(approxDocs.scoreDocs.length, exactDocs.scoreDocs.length); i++) {
                        FieldDoc approxFieldDoc = (FieldDoc) approxDocs.scoreDocs[i];
                        FieldDoc exactFieldDoc = (FieldDoc) exactDocs.scoreDocs[i];
                        assertEquals("Doc at position " + i + " should match", exactFieldDoc.doc, approxFieldDoc.doc);
                        assertEquals(
                            "Sort value at position " + i + " should match",
                            (exactFieldDoc.fields[0]),
                            (approxFieldDoc.fields[0])
                        );
                    }
                    // Verify all returned docs are correctly ordered relative to searchAfterValue
                    for (ScoreDoc scoreDoc : approxDocs.scoreDocs) {
                        FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                        Number value = (Number) fieldDoc.fields[0];
                        long searchAfterLong = ((Number) searchAfterDoc.fields[0]).longValue();

                        if (sortOrder == SortOrder.ASC) {
                            assertTrue(
                                "Doc value " + value + " should be > searchAfterValue " + searchAfterLong,
                                value.longValue() > searchAfterLong
                            );
                        } else {
                            assertTrue(
                                "Doc value " + value + " should be < searchAfterValue " + searchAfterLong,
                                value.longValue() < searchAfterLong
                            );
                        }
                    }
                }
            }
        }
    }
}
