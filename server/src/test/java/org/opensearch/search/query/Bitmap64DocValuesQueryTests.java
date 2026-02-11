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

import org.roaringbitmap.longlong.Roaring64NavigableMap;

import static org.opensearch.search.query.Bitmap64IndexQueryTests.getMatchingValues;

public class Bitmap64DocValuesQueryTests extends OpenSearchTestCase {
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
        Bitmap64DocValuesQuery query = new Bitmap64DocValuesQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        List<Long> actual = getMatchingValues(weight, searcher.getIndexReader());
        List<Long> expected = List.of(1L, 4L);
        assertEquals(expected, actual);
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
        Bitmap64DocValuesQuery query = new Bitmap64DocValuesQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        Set<Long> actual = new HashSet<>(getMatchingValues(weight, searcher.getIndexReader()));
        Set<Long> expected = Set.of(2L, 3L);
        assertEquals(expected, actual);
    }

    public void testScoreLargeValues() throws IOException {
        long largeVal1 = Integer.MAX_VALUE + 100L;
        long largeVal2 = Integer.MAX_VALUE + 200L;

        Document d = new Document();
        d.add(new LongField("product_id", largeVal1, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", largeVal2, Field.Store.NO));
        w.addDocument(d);

        d = new Document();
        d.add(new LongField("product_id", 1L, Field.Store.NO));
        w.addDocument(d);

        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);

        Roaring64NavigableMap bitmap = new Roaring64NavigableMap(true);
        bitmap.addLong(largeVal1);
        bitmap.addLong(largeVal2);
        Bitmap64DocValuesQuery query = new Bitmap64DocValuesQuery("product_id", bitmap);

        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);

        List<Long> actual = getMatchingValues(weight, searcher.getIndexReader());
        List<Long> expected = List.of(largeVal1, largeVal2);
        assertEquals(expected, actual);
    }
}
