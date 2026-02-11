/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.common.Randomness;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class Bitmap64IndexQueryTests extends OpenSearchTestCase {
    private Directory dir;
    private IndexWriter w;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    @Before
    public void initSearcher() throws IOException {
        dir = newDirectory();
        w = new IndexWriter(dir, newIndexWriterConfig());
        reader = DirectoryReader.open(w);
    }

    @After
    public void closeAllTheReaders() throws IOException {
        reader.close();
        w.close();
        dir.close();
    }

    public void testScore() throws IOException {
        Document d = new Document();
        d.add(new LongField("product_id", 1L, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 2L, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 3L, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 4L, Field.Store.NO));
        w.addDocument(d);

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        bitmap.addLong(1L);
        bitmap.addLong(4L);
        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        List<Long> actual = getMatchingValues(weight, searcher.getIndexReader());
        List<Long> expected = List.of(1L, 4L);
        assertEquals(expected, actual);
    }

    static List<Long> getMatchingValues(Weight weight, IndexReader reader) throws IOException {
        List<Long> actual = new LinkedList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedNumericDocValues dv = DocValues.getSortedNumeric(leaf.reader(), "product_id");
            Scorer scorer = weight.scorer(leaf);
            DocIdSetIterator disi = scorer.iterator();
            int docId;
            while ((docId = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                dv.advanceExact(docId);
                for (int count = 0; count < dv.docValueCount(); ++count) {
                    actual.add(dv.nextValue());
                }
            }
        }
        return actual;
    }

    public void testScoreMultiValues() throws IOException {
        Document d = new Document();
        d.add(new LongField("product_id", 1L, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 2L, Field.Store.NO));
        d.add(new LongField("product_id", 3L, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 3L, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 4L, Field.Store.NO));
        w.addDocument(d);

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        bitmap.addLong(3L);
        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        Set<Long> actual = new HashSet<>(getMatchingValues(weight, searcher.getIndexReader()));
        Set<Long> expected = Set.of(2L, 3L);
        assertEquals(expected, actual);
    }

    public void testRandomDocumentsAndQueries() throws IOException {
        Random random = Randomness.get();
        int valueRange = 10_000;

        for (int i = 0; i < valueRange + 1; i++) {
            Document d = new Document();
            d.add(new LongField("product_id", (long) i, Field.Store.NO));
            w.addDocument(d);
        }

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        Set<Long> queryValues = new HashSet<>();
        int numberOfValues = 5;
        for (int i = 0; i < numberOfValues; i++) {
            long value = random.nextInt(valueRange) + 1L;
            queryValues.add(value);
        }
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        for (long v : queryValues) {
            bitmap.addLong(v);
        }

        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        Set<Long> actualSet = new HashSet<>(getMatchingValues(weight, searcher.getIndexReader()));

        List<Long> expected = new ArrayList<>(queryValues);
        Collections.sort(expected);
        List<Long> actual = new ArrayList<>(actualSet);
        Collections.sort(actual);
        assertEquals(expected, actual);
    }

    public void testLargeValues() throws IOException {
        long largeVal1 = Integer.MAX_VALUE + 100L;
        long largeVal2 = Integer.MAX_VALUE + 200L;
        long largeVal3 = Integer.MAX_VALUE + 300L;

        Document d = new Document();
        d.add(new LongField("product_id", largeVal1, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", largeVal2, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", largeVal3, Field.Store.NO));
        w.addDocument(d);

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        bitmap.addLong(largeVal1);
        bitmap.addLong(largeVal3);
        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        List<Long> actual = getMatchingValues(weight, searcher.getIndexReader());
        List<Long> expected = List.of(largeVal1, largeVal3);
        assertEquals(expected, actual);
    }

    public void testNegativeLongs() throws IOException {
        Document d = new Document();
        d.add(new LongField("product_id", -100L, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 0L, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 100L, Field.Store.NO));
        w.addDocument(d);

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        bitmap.addLong(-100L);
        bitmap.addLong(100L);
        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        List<Long> actual = getMatchingValues(weight, searcher.getIndexReader());
        List<Long> expected = List.of(-100L, 100L);
        assertEquals(expected, actual);
    }

    public void testBoundaryValues() throws IOException {
        Document d = new Document();
        d.add(new LongField("product_id", Long.MIN_VALUE, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 0L, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", Long.MAX_VALUE, Field.Store.NO));
        w.addDocument(d);

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        bitmap.addLong(Long.MIN_VALUE);
        bitmap.addLong(Long.MAX_VALUE);
        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        List<Long> actual = getMatchingValues(weight, searcher.getIndexReader());
        List<Long> expected = List.of(Long.MIN_VALUE, Long.MAX_VALUE);
        assertEquals(expected, actual);
    }

    public void testCheckArgsNullBitmap() {
        assertThrows(IllegalArgumentException.class, () -> Bitmap64IndexQuery.checkArgs("field", null));
    }

    public void testCheckArgsNullField() {
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        assertThrows(IllegalArgumentException.class, () -> Bitmap64IndexQuery.checkArgs(null, bitmap));
    }

    public void testCheckArgsWithNullBitmap() {
        assertThrows(IllegalArgumentException.class, () -> { Bitmap64IndexQuery.checkArgs("product_id", null); });
    }

    public void testCheckArgsWithNullFieldAndBitmap() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> { Bitmap64IndexQuery.checkArgs(null, null); }
        );
        assertEquals("field must not be null", exception.getMessage());
    }

    public void testCreateWeight() throws IOException {
        Document d = new Document();
        d.add(new LongField("product_id", 4L, Field.Store.NO));
        w.addDocument(d);

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        bitmap.addLong(1L);
        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
        assertNotNull(weight);
        Scorer scorer = weight.scorer(reader.leaves().get(0));
        assertNotNull(scorer);
        ScorerSupplier supplier = weight.scorerSupplier(reader.leaves().get(0));
        assertNotNull(supplier);
        long cost = supplier.cost();
        assertEquals(20, cost);
    }

    public void testRewrite() throws IOException {
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);
        assertEquals(new MatchNoDocsQuery(), query.rewrite(searcher));
    }

    public void testPointVisitor() throws IOException {
        w.close();
        w = new IndexWriter(dir, new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec()));

        for (int i = 0; i < 512 + 1; i++) {
            Document d = new Document();
            d.add(new LongField("product_id", 1L, Field.Store.NO));
            w.addDocument(d);
        }

        for (int i = 0; i < 256 + 1; i++) {
            Document d = new Document();
            d.add(new LongField("product_id", 2L, Field.Store.NO));
            w.addDocument(d);
        }

        for (int i = 0; i < 256 + 1; i++) {
            Document d = new Document();
            d.add(new LongField("product_id", 3L, Field.Store.NO));
            w.addDocument(d);
        }

        for (int i = 0; i < 512 + 1; i++) {
            Document d = new Document();
            d.add(new LongField("product_id", 4L, Field.Store.NO));
            w.addDocument(d);
        }

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        bitmap.addLong(0L);
        bitmap.addLong(1L);
        bitmap.addLong(2L);
        bitmap.addLong(3L);
        bitmap.addLong(5L);
        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        Set<Long> actual = new HashSet<>(getMatchingValues(weight, searcher.getIndexReader()));
        Set<Long> expected = Set.of(1L, 2L, 3L);
        assertEquals(expected, actual);
    }
}
