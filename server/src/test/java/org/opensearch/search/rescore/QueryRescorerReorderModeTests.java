/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.rescore;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.search.rescore.QueryRescorer.QueryRescoreContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Tests the behavior of the three {@link RescoreReorderMode}s in {@link QueryRescorer} when the
 * rescore query lowers ("tanks") the scores of the hits inside the rescore window.
 *
 * Index layout: 9 documents, docids 0..8 (single segment, insertion order). Documents 0, 1, 2, 3 and 6
 * contain the term the rescore query matches. The hand-built first pass ranks the documents by docid
 * with descending scores 9, 8, ..., 1. With a rescore window of 4, documents 0-3 are inside the window
 * and get their scores multiplied by 0.1 while document 6 (also matching) is outside the window.
 */
public class QueryRescorerReorderModeTests extends OpenSearchTestCase {

    private static final int WINDOW_SIZE = 4;
    private static final float RESCORE_FACTOR = 0.1f;

    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))) {
            for (int i = 0; i < 9; i++) {
                Document doc = new Document();
                boolean matchesRescore = i <= 3 || i == 6;
                doc.add(new StringField("f", matchesRescore ? "quick" : "lazy", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            reader = DirectoryReader.open(writer);
        }
        searcher = new IndexSearcher(reader);
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        directory.close();
        super.tearDown();
    }

    /** Builds a first-pass result of docids 0..count-1 with descending scores count, count-1, ..., 1. */
    private static TopDocs firstPass(int count) {
        ScoreDoc[] scoreDocs = new ScoreDoc[count];
        for (int i = 0; i < count; i++) {
            scoreDocs[i] = new ScoreDoc(i, count - i);
        }
        return new TopDocs(new TotalHits(count, TotalHits.Relation.EQUAL_TO), scoreDocs);
    }

    private QueryRescoreContext context(RescoreReorderMode mode, float rescoreBoost) {
        Query rescoreQuery = new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("f", "quick"))), rescoreBoost);
        QueryRescoreContext context = new QueryRescoreContext(WINDOW_SIZE);
        context.setParsedQuery(new ParsedQuery(rescoreQuery));
        context.setScoreMode(QueryRescoreMode.Multiply);
        context.setReorderMode(mode);
        return context;
    }

    private TopDocs rescore(RescoreReorderMode mode, int hitCount) throws IOException {
        return QueryRescorer.INSTANCE.rescore(firstPass(hitCount), searcher, context(mode, RESCORE_FACTOR));
    }

    private static int[] docIds(TopDocs topDocs) {
        int[] ids = new int[topDocs.scoreDocs.length];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = topDocs.scoreDocs[i].doc;
        }
        return ids;
    }

    private static void assertSortedByScore(TopDocs topDocs) {
        for (int i = 1; i < topDocs.scoreDocs.length; i++) {
            assertTrue(
                "scores must be non-increasing but hit " + i + " has a greater score than hit " + (i - 1),
                topDocs.scoreDocs[i].score <= topDocs.scoreDocs[i - 1].score
            );
        }
    }

    public void testLegacyMixesTankedWindowWithTail() throws IOException {
        TopDocs result = rescore(RescoreReorderMode.LEGACY, 9);
        // Window docs 0-3 are tanked to 0.9, 0.8, 0.7, 0.6 and sink below the non-rescored tail (5, 4, 3, 2, 1).
        // Doc 6 matches the rescore query but is beyond the window and keeps its first-pass score of 3.
        assertArrayEquals(new int[] { 4, 5, 6, 7, 8, 0, 1, 2, 3 }, docIds(result));
        assertSortedByScore(result);
        assertEquals(0.9f, result.scoreDocs[5].score, 0.0001f);
    }

    public void testAllHitsRescoresEveryCollectedHit() throws IOException {
        TopDocs result = rescore(RescoreReorderMode.ALL_HITS, 9);
        // Every collected hit is rescored: docs 0-3 become 0.9, 0.8, 0.7, 0.6 and doc 6 (beyond the window
        // but matching) becomes 0.3, so unlike LEGACY it sinks below the window docs.
        assertArrayEquals(new int[] { 4, 5, 7, 8, 0, 1, 2, 3, 6 }, docIds(result));
        assertSortedByScore(result);
        assertEquals(0.3f, result.scoreDocs[8].score, 0.0001f);
    }

    public void testWindowOnlyKeepsWindowAheadOfTail() throws IOException {
        TopDocs result = rescore(RescoreReorderMode.WINDOW_ONLY, 9);
        // The window docs are reordered among themselves (here the rescore preserves their relative order)
        // and stay ahead of the tail, which keeps its first-pass order.
        assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, docIds(result));
        // Scores must remain non-increasing so that the coordinator level merge preserves this order.
        assertSortedByScore(result);
        // The tail scores are untouched.
        assertEquals(5.0f, result.scoreDocs[4].score, 0.0001f);
        assertEquals(1.0f, result.scoreDocs[8].score, 0.0001f);
        // The window scores were shifted above the tail.
        assertTrue(result.scoreDocs[3].score > result.scoreDocs[4].score);
    }

    public void testWindowOnlyPaginationIsConsistent() throws IOException {
        // The ordering of the shared prefix must not depend on how many hits were collected (i.e. on from + size).
        TopDocs shallow = rescore(RescoreReorderMode.WINDOW_ONLY, 6);
        TopDocs deep = rescore(RescoreReorderMode.WINDOW_ONLY, 9);
        int[] shallowIds = docIds(shallow);
        int[] deepIds = docIds(deep);
        for (int i = 0; i < shallowIds.length; i++) {
            assertEquals("hit " + i + " differs between collection sizes", shallowIds[i], deepIds[i]);
        }
    }

    public void testLegacyPaginationIsInconsistent() throws IOException {
        // Documents the bug this option exists for: with LEGACY, page content depends on the collected size,
        // so the same documents show up on multiple pages (opensearch-project/OpenSearch#19813).
        // "from=4, size=2" collects 6 hits and the page (positions 4-5) contains docs 2 and 3 ...
        TopDocs shallow = rescore(RescoreReorderMode.LEGACY, 6);
        assertArrayEquals(new int[] { 2, 3 }, new int[] { docIds(shallow)[4], docIds(shallow)[5] });
        // ... but "from=6, size=2" collects 8 hits and that page (positions 6-7) contains docs 2 and 3 again.
        TopDocs deep = rescore(RescoreReorderMode.LEGACY, 8);
        assertArrayEquals(new int[] { 2, 3 }, new int[] { docIds(deep)[6], docIds(deep)[7] });
    }

    public void testWindowOnlyDoesNotAdjustScoresWhenRescoreBoosts() throws IOException {
        // When the rescore increases scores, the window is already ahead of the tail and scores are untouched.
        TopDocs topDocs = firstPass(9);
        TopDocs result = QueryRescorer.INSTANCE.rescore(topDocs, searcher, context(RescoreReorderMode.WINDOW_ONLY, 10.0f));
        assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8 }, docIds(result));
        assertSortedByScore(result);
        assertEquals(90.0f, result.scoreDocs[0].score, 0.0001f);
        assertEquals(60.0f, result.scoreDocs[3].score, 0.0001f);
        assertEquals(5.0f, result.scoreDocs[4].score, 0.0001f);
    }

    public void testAllHitsMarksAllDocsRescored() throws IOException {
        QueryRescoreContext context = context(RescoreReorderMode.ALL_HITS, RESCORE_FACTOR);
        QueryRescorer.INSTANCE.rescore(firstPass(9), searcher, context);
        for (int i = 0; i < 9; i++) {
            assertTrue("doc " + i + " should be marked as rescored", context.isRescored(i));
        }
    }
}
