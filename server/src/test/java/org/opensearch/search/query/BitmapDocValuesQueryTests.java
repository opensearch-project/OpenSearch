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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.roaringbitmap.RoaringBitmap;

import static org.opensearch.search.query.BitmapIndexQueryTests.getMatchingValues;

public class BitmapDocValuesQueryTests extends OpenSearchTestCase {
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
        BitmapDocValuesQuery query = new BitmapDocValuesQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        List<Integer> actual = getMatchingValues(weight, searcher.getIndexReader());
        List<Integer> expected = List.of(1, 4);
        assertEquals(expected, actual);
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
        BitmapDocValuesQuery query = new BitmapDocValuesQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        Set<Integer> actual = new HashSet<>(getMatchingValues(weight, searcher.getIndexReader()));
        Set<Integer> expected = Set.of(2, 3);
        assertEquals(expected, actual);
    }
}
