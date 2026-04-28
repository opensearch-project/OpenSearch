/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.opensearch.common.Randomness;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
    public void closeAll() throws IOException {
        reader.close();
        w.close();
        dir.close();
    }

    public void testScore() throws IOException {
        addDoc(1L);
        addDoc(2L);
        addDoc(3L);
        addDoc(4L);

        refresh();

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        bitmap.add(1L);
        bitmap.add(4L);

        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        List<Long> actual = getMatchingValues(weight, reader);
        assertEquals(List.of(1L, 4L), actual);
    }

    public void testScoreMultiValues() throws IOException {
        addDoc(1L);
        addDoc(2L, 3L);
        addDoc(3L);
        addDoc(4L);

        refresh();

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        bitmap.add(3L);

        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        Set<Long> actual = new HashSet<>(getMatchingValues(weight, reader));
        assertEquals(Set.of(2L, 3L), actual);
    }

    public void testRandomDocumentsAndQueries() throws IOException {
        Random random = Randomness.get();
        int valueRange = 10_000;

        for (long i = 0; i <= valueRange; i++) {
            addDoc(i);
        }

        refresh();

        Set<Long> queryValues = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            queryValues.add((long) random.nextInt(valueRange));
        }

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        queryValues.forEach(bitmap::add);

        Bitmap64IndexQuery query = new Bitmap64IndexQuery("product_id", bitmap);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        Set<Long> actual = new HashSet<>(getMatchingValues(weight, reader));
        assertEquals(queryValues, actual);
    }

    // ---------------- Helpers ----------------

    private void addDoc(long... values) throws IOException {
        Document d = new Document();
        for (long v : values) {
            d.add(new LongPoint("product_id", v));
            d.add(new SortedNumericDocValuesField("product_id", v));
        }
        w.addDocument(d);
    }

    private void refresh() throws IOException {
        w.commit();
        reader.close();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);
    }

    static List<Long> getMatchingValues(Weight weight, IndexReader reader) throws IOException {
        List<Long> actual = new ArrayList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedNumericDocValues dv = DocValues.getSortedNumeric(leaf.reader(), "product_id");
            Scorer scorer = weight.scorer(leaf);
            if (scorer == null) continue;

            DocIdSetIterator it = scorer.iterator();
            int docId;
            while ((docId = it.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (dv.advanceExact(docId)) {
                    for (int i = 0; i < dv.docValueCount(); i++) {
                        actual.add(dv.nextValue());
                    }
                }
            }
        }
        Collections.sort(actual);
        return actual;
    }
}
