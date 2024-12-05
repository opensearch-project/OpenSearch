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
                        Query approximateQuery = new ApproximatePointRangeQuery("point", pack(lower).bytes, pack(upper).bytes, dims) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
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
                        Query approximateQuery = new ApproximatePointRangeQuery("point", pack(lower).bytes, pack(upper).bytes, dims) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
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

    public void testApproximateRangeWithSizeUnderDefault() throws IOException {
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
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 0;
                        long upper = 45;
                        Query approximateQuery = new ApproximatePointRangeQuery("point", pack(lower).bytes, pack(upper).bytes, dims, 10) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 10);
                        assertEquals(topDocs.totalHits, new TotalHits(10, TotalHits.Relation.EQUAL_TO));
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
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            11_000
                        ) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 11000);

                        if (topDocs.totalHits.relation == Relation.EQUAL_TO) {
                            assertEquals(topDocs.totalHits.value, 11000);
                        } else {
                            assertTrue(11000 <= topDocs.totalHits.value);
                            assertTrue(maxHits >= topDocs.totalHits.value);
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
                        Query approximateQuery = new ApproximatePointRangeQuery("point", pack(lower).bytes, pack(upper).bytes, dims, 10) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        Query query = LongPoint.newRangeQuery("point", lower, upper);

                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 10);
                        TopDocs topDocs1 = searcher.search(query, 10);

                        // since we short-circuit from the approx range at the end of size these will not be equal
                        assertNotEquals(topDocs.totalHits, topDocs1.totalHits);
                        assertEquals(topDocs.totalHits, new TotalHits(10, TotalHits.Relation.EQUAL_TO));
                        assertEquals(topDocs1.totalHits, new TotalHits(101, TotalHits.Relation.EQUAL_TO));
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
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            10,
                            SortOrder.ASC
                        ) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        Query query = LongPoint.newRangeQuery("point", lower, upper);

                        IndexSearcher searcher = new IndexSearcher(reader);
                        Sort sort = new Sort(new SortField("point", SortField.Type.LONG));
                        TopDocs topDocs = searcher.search(approximateQuery, 10, sort);
                        TopDocs topDocs1 = searcher.search(query, 10, sort);

                        // since we short-circuit from the approx range at the end of size these will not be equal
                        assertNotEquals(topDocs.totalHits, topDocs1.totalHits);
                        assertEquals(topDocs.totalHits, new TotalHits(10, TotalHits.Relation.EQUAL_TO));
                        assertEquals(topDocs1.totalHits, new TotalHits(21, TotalHits.Relation.EQUAL_TO));
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
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery("point", pack(0).bytes, pack(20).bytes, 1) {
            protected String toString(int dimension, byte[] value) {
                return Long.toString(LongPoint.decodeDimension(value, 0));
            }
        };
        assertEquals(query.getSize(), 10_000);

        query.setSize(100);
        assertEquals(query.getSize(), 100);

    }

    public void testSortOrder() {
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery("point", pack(0).bytes, pack(20).bytes, 1) {
            protected String toString(int dimension, byte[] value) {
                return Long.toString(LongPoint.decodeDimension(value, 0));
            }
        };
        assertNull(query.getSortOrder());

        query.setSortOrder(SortOrder.ASC);
        assertEquals(query.getSortOrder(), SortOrder.ASC);
    }

    public void testCanApproximate() {
        ApproximatePointRangeQuery query = new ApproximatePointRangeQuery("point", pack(0).bytes, pack(20).bytes, 1) {
            protected String toString(int dimension, byte[] value) {
                return Long.toString(LongPoint.decodeDimension(value, 0));
            }
        };

        assertFalse(query.canApproximate(null));

        ApproximatePointRangeQuery queryCanApproximate = new ApproximatePointRangeQuery("point", pack(0).bytes, pack(20).bytes, 1) {
            protected String toString(int dimension, byte[] value) {
                return Long.toString(LongPoint.decodeDimension(value, 0));
            }

            public boolean canApproximate(SearchContext context) {
                return true;
            }
        };
        SearchContext searchContext = mock(SearchContext.class);
        assertTrue(queryCanApproximate.canApproximate(searchContext));
    }
}
