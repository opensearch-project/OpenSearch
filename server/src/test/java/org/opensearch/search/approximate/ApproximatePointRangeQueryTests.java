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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.apache.lucene.document.LongPoint.pack;

public class ApproximatePointRangeQueryTests extends OpenSearchTestCase {

    public void testApproximateRangeEqualsActualRange() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, new WhitespaceAnalyzer());
        int dims = 1;

        long[] scratch = new long[dims];
        int numPoints = 100;
        Document doc = new Document();
        for (int j = 0; j < numPoints; j++) {
            for (int v = 0; v < dims; v++) {
                scratch[v] = RandomNumbers.randomLongBetween(random(), 0, 100);
            }
            doc.add(new LongPoint("point", scratch));
            iw.addDocument(doc);
        }
        iw.flush();
        try (IndexReader reader = iw.getReader()) {
            try {
                long lower = 20;
                long upper = 100;
                Query approximateQuery = new ApproximatePointRangeQuery("point", pack(lower).bytes, pack(upper).bytes, dims) {
                    protected String toString(int dimension, byte[] value) {
                        return Long.toString(LongPoint.decodeDimension(value, 0));
                    }
                };
                Query query = new PointRangeQuery("point", pack(lower).bytes, pack(upper).bytes, dims) {
                    protected String toString(int dimension, byte[] value) {
                        return Long.toString(LongPoint.decodeDimension(value, 0));
                    }
                };
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs topDocs = searcher.search(approximateQuery, 100);
                TopDocs topDocs1 = searcher.search(query, 100);
                assertEquals(topDocs.totalHits.value, topDocs1.totalHits.value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } finally {
            iw.close();
            directory.close();
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

    public void testApproximateRangeWithSizeOverDefaultAscSort() throws IOException {
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
                // need a single segment to test this optimization, if size > number of docs in segment we fall back to pointrangequery
                // because it is faster
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 1000;
                        long upper = 14000;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            11_000,
                            SortOrder.ASC
                        ) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 20000);
                        assertEquals(topDocs.totalHits, new TotalHits(11_000, TotalHits.Relation.EQUAL_TO));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        }
    }

    public void testApproximateRangeWithSizeOverDefaultDescSort() throws IOException {
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
                // need a single segment to test this optimization, if size > number of docs in segment we fall back to pointrangequery
                // because it is faster
                iw.forceMerge(1);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 1000;
                        long upper = 14000;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            11_000,
                            SortOrder.DESC
                        ) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 20000);
                        assertEquals(topDocs.totalHits, new TotalHits(11_000, TotalHits.Relation.EQUAL_TO));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        }
    }

    public void testApproximateRangeWithDeletedDocs() throws IOException {
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
                // need a single segment to test this optimization, if size > number of docs in segment we fall back to pointrangequery
                // because it is faster
                iw.forceMerge(1);

                // delete some documents where size < number of docs
                long l = 500;
                long u = 1499;
                Query query = new PointRangeQuery("point", pack(l).bytes, pack(u).bytes, dims) {
                    @Override
                    protected String toString(int dimension, byte[] value) {
                        return Long.toString(LongPoint.decodeDimension(value, 0));
                    }
                };
                iw.deleteDocuments(query);

                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 1000;
                        long upper = 14000;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            11_000,
                            SortOrder.ASC
                        ) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 20000);
                        assertEquals(topDocs.totalHits, new TotalHits(10_500, TotalHits.Relation.EQUAL_TO));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        }
    }

    public void testApproximateRangeWithDeletedDocsDesc() throws IOException {
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
                // need a single segment to test this optimization, if size > number of docs in segment we fall back to pointrangequery
                // because it is faster
                iw.forceMerge(1);

                // delete some documents where size < number of docs
                long l = 500;
                long u = 1499;
                Query query = new PointRangeQuery("point", pack(l).bytes, pack(u).bytes, dims) {
                    @Override
                    protected String toString(int dimension, byte[] value) {
                        return Long.toString(LongPoint.decodeDimension(value, 0));
                    }
                };
                iw.deleteDocuments(query);
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 1000;
                        long upper = 14000;
                        Query approximateQuery = new ApproximatePointRangeQuery(
                            "point",
                            pack(lower).bytes,
                            pack(upper).bytes,
                            dims,
                            11_000,
                            SortOrder.DESC
                        ) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 20000);
                        assertEquals(topDocs.totalHits, new TotalHits(11_500, TotalHits.Relation.EQUAL_TO));
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
                }
                iw.flush();
                try (IndexReader reader = iw.getReader()) {
                    try {
                        long lower = 0;
                        long upper = 100;
                        Query approximateQuery = new ApproximatePointRangeQuery("point", pack(lower).bytes, pack(upper).bytes, dims, 10) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        Query query = new PointRangeQuery("point", pack(lower).bytes, pack(upper).bytes, dims) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
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
                    doc.add(new LongPoint("point", scratch));
                    iw.addDocument(doc);
                }
                iw.flush();
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
                        Query query = new PointRangeQuery("point", pack(lower).bytes, pack(upper).bytes, dims) {
                            protected String toString(int dimension, byte[] value) {
                                return Long.toString(LongPoint.decodeDimension(value, 0));
                            }
                        };
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs topDocs = searcher.search(approximateQuery, 10);
                        TopDocs topDocs1 = searcher.search(query, 10);

                        // since we short-circuit from the approx range at the end of size these will not be equal
                        assertNotEquals(topDocs.totalHits, topDocs1.totalHits);
                        assertEquals(topDocs.totalHits, new TotalHits(10, TotalHits.Relation.EQUAL_TO));
                        assertEquals(topDocs1.totalHits, new TotalHits(21, TotalHits.Relation.EQUAL_TO));
                        assertEquals(topDocs.scoreDocs[0].doc, 0);
                        assertEquals(topDocs.scoreDocs[1].doc, 1);
                        assertEquals(topDocs.scoreDocs[2].doc, 2);
                        assertEquals(topDocs.scoreDocs[3].doc, 3);
                        assertEquals(topDocs.scoreDocs[4].doc, 4);
                        assertEquals(topDocs.scoreDocs[5].doc, 5);

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }
            }
        }
    }
}
