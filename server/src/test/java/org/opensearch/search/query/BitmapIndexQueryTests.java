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
import org.apache.lucene.document.IntField;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.roaringbitmap.RoaringBitmap;

public class BitmapIndexQueryTests extends OpenSearchTestCase {
    private Directory dir;
    private IndexWriter w;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    @Before
    public void initSearcher() throws IOException {
        dir = newDirectory();
        w = new IndexWriter(dir, newIndexWriterConfig());
    }

    @After
    public void closeAllTheReaders() throws IOException {
        reader.close();
        w.close();
        dir.close();
    }

    public void testScore() throws IOException {
        Document d = new Document();
        d.add(new IntField("product_id", 1, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new IntField("product_id", 2, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new IntField("product_id", 3, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new IntField("product_id", 4, Field.Store.NO));
        w.addDocument(d);

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(1);
        bitmap.add(4);
        BitmapIndexQuery query = new BitmapIndexQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        List<Integer> actual = getMatchingValues(weight, searcher.getIndexReader());
        List<Integer> expected = List.of(1, 4);
        assertEquals(expected, actual);
    }

    // use doc values to get the actual value of the matching docs
    // cannot directly check the docId because test can randomize segment numbers
    static List<Integer> getMatchingValues(Weight weight, IndexReader reader) throws IOException {
        List<Integer> actual = new LinkedList<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            SortedNumericDocValues dv = DocValues.getSortedNumeric(leaf.reader(), "product_id");
            Scorer scorer = weight.scorer(leaf);
            DocIdSetIterator disi = scorer.iterator();
            int docId;
            while ((docId = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                dv.advanceExact(docId);
                for (int count = 0; count < dv.docValueCount(); ++count) {
                    actual.add((int) dv.nextValue());
                }
            }
        }
        return actual;
    }

    public void testScoreMutilValues() throws IOException {
        Document d = new Document();
        d.add(new IntField("product_id", 1, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new IntField("product_id", 2, Field.Store.NO));
        d.add(new IntField("product_id", 3, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new IntField("product_id", 3, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new IntField("product_id", 4, Field.Store.NO));
        w.addDocument(d);

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(3);
        BitmapIndexQuery query = new BitmapIndexQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        Set<Integer> actual = new HashSet<>(getMatchingValues(weight, searcher.getIndexReader()));
        Set<Integer> expected = Set.of(2, 3);
        assertEquals(expected, actual);
    }

    public void testRandomDocumentsAndQueries() throws IOException {
        Random random = Randomness.get();
        int valueRange = 10_000; // the range of query values should be within indexed values

        for (int i = 0; i < valueRange + 1; i++) {
            Document d = new Document();
            d.add(new IntField("product_id", i, Field.Store.NO));
            w.addDocument(d);
        }

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        // Generate random values for bitmap query
        Set<Integer> queryValues = new HashSet<>();
        int numberOfValues = 5;
        for (int i = 0; i < numberOfValues; i++) {
            int value = random.nextInt(valueRange) + 1;
            queryValues.add(value);
        }
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(queryValues.stream().mapToInt(Integer::intValue).toArray());

        BitmapIndexQuery query = new BitmapIndexQuery("product_id", bitmap);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        Set<Integer> actualSet = new HashSet<>(getMatchingValues(weight, searcher.getIndexReader()));

        List<Integer> expected = new ArrayList<>(queryValues);
        Collections.sort(expected);
        List<Integer> actual = new ArrayList<>(actualSet);
        Collections.sort(actual);
        assertEquals(expected, actual);
    }

}
