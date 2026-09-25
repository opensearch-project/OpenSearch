/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.rescore;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RescoreProcessorTests extends OpenSearchTestCase {

    private static final SortField[] SORT_BY_SCORE = new SortField[] { SortField.FIELD_SCORE };

    /** Rescorer test double that returns a predefined {@link TopDocs} and records how many times it ran. */
    private static class RecordingRescorer implements Rescorer {
        private final TopDocs output;
        int invocations = 0;

        RecordingRescorer(TopDocs output) {
            this.output = output;
        }

        @Override
        public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) {
            invocations++;
            return output;
        }

        @Override
        public Explanation explain(
            int topLevelDocId,
            IndexSearcher searcher,
            RescoreContext rescoreContext,
            Explanation sourceExplanation
        ) {
            throw new UnsupportedOperationException();
        }
    }

    private static SearchContext mockContext(QuerySearchResult queryResult, Rescorer rescorer) {
        SearchContext context = mock(SearchContext.class);
        when(context.queryResult()).thenReturn(queryResult);
        when(context.rescore()).thenReturn(List.of(new RescoreContext(10, rescorer)));
        // sort() defaults to null, which the in-process assertion requires for a rescore.
        return context;
    }

    private static TopDocs scoreDocs(int totalHits, ScoreDoc... docs) {
        return new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), docs);
    }

    private static CollapseTopFieldDocs collapseDocs(String field, ScoreDoc[] docs, Object[] collapseValues) {
        return new CollapseTopFieldDocs(
            field,
            new TotalHits(docs.length, TotalHits.Relation.EQUAL_TO),
            docs,
            SORT_BY_SCORE,
            collapseValues
        );
    }

    public void testEmptyTopDocsIsNoOpAndDoesNotRescore() {
        QuerySearchResult queryResult = new QuerySearchResult();
        TopDocs empty = scoreDocs(0);
        queryResult.topDocs(new TopDocsAndMaxScore(empty, Float.NaN), new DocValueFormat[0]);

        RecordingRescorer rescorer = new RecordingRescorer(scoreDocs(0));
        new RescoreProcessor().process(mockContext(queryResult, rescorer));

        assertEquals(0, rescorer.invocations);
        assertSame(empty, queryResult.topDocs().topDocs);
    }

    public void testRescoreWithoutCollapseReplacesTopDocs() {
        QuerySearchResult queryResult = new QuerySearchResult();
        TopDocs original = scoreDocs(2, new ScoreDoc(1, 2.0f), new ScoreDoc(2, 1.0f));
        queryResult.topDocs(new TopDocsAndMaxScore(original, 2.0f), new DocValueFormat[0]);

        // Rescorer reorders the docs and bumps the scores; result stays sorted by score descending.
        TopDocs rescored = scoreDocs(2, new ScoreDoc(2, 10.0f), new ScoreDoc(1, 4.0f));
        RecordingRescorer rescorer = new RecordingRescorer(rescored);

        new RescoreProcessor().process(mockContext(queryResult, rescorer));

        assertEquals(1, rescorer.invocations);
        TopDocs result = queryResult.topDocs().topDocs;
        assertFalse(result instanceof CollapseTopFieldDocs);
        assertSame(rescored, result);
        assertEquals(10.0f, queryResult.topDocs().maxScore, 0.0f);
    }

    public void testRescoreAfterCollapseReconstructsCollapseTopFieldDocs() {
        QuerySearchResult queryResult = new QuerySearchResult();
        ScoreDoc[] collapsed = new ScoreDoc[] {
            new FieldDoc(10, 5.0f, new Object[] { 5.0f }),
            new FieldDoc(20, 3.0f, new Object[] { 3.0f }),
            new FieldDoc(30, 1.0f, new Object[] { 1.0f }) };
        Object[] collapseValues = new Object[] { "a", "b", "c" };
        CollapseTopFieldDocs original = collapseDocs("group", collapsed, collapseValues);
        // CollapseTopFieldDocs hits are FieldDocs, so a single sort value format is required.
        queryResult.topDocs(new TopDocsAndMaxScore(original, 5.0f), new DocValueFormat[] { DocValueFormat.RAW });

        // Rescoring drops collapse info and returns plain ScoreDocs in a new order.
        TopDocs rescored = scoreDocs(3, new ScoreDoc(30, 9.0f), new ScoreDoc(10, 8.0f), new ScoreDoc(20, 2.0f));
        RecordingRescorer rescorer = new RecordingRescorer(rescored);

        new RescoreProcessor().process(mockContext(queryResult, rescorer));

        assertEquals(1, rescorer.invocations);
        TopDocs result = queryResult.topDocs().topDocs;
        assertTrue(result instanceof CollapseTopFieldDocs);
        CollapseTopFieldDocs collapseResult = (CollapseTopFieldDocs) result;
        assertEquals("group", collapseResult.field);
        assertArrayEquals(SORT_BY_SCORE, collapseResult.fields);

        // For every hit: order follows the rescored docs, the original collapse value is preserved,
        // and the rescored score is carried both as the score and as the sole sort value.
        int[] expectedDocs = { 30, 10, 20 };
        Object[] expectedCollapseValues = { "c", "a", "b" };
        float[] expectedScores = { 9.0f, 8.0f, 2.0f };
        assertEquals(expectedDocs.length, collapseResult.scoreDocs.length);
        for (int i = 0; i < expectedDocs.length; i++) {
            FieldDoc hit = (FieldDoc) collapseResult.scoreDocs[i];
            assertEquals(expectedDocs[i], hit.doc);
            assertEquals(expectedCollapseValues[i], collapseResult.collapseValues[i]);
            assertEquals(expectedScores[i], hit.score, 0.0f);
            assertEquals(1, hit.fields.length);
            assertEquals(expectedScores[i], (float) hit.fields[0], 0.0f);
        }
        assertEquals(9.0f, queryResult.topDocs().maxScore, 0.0f);
    }

    public void testCaptureFailsWhenCollapseArrayLengthsMismatch() {
        QuerySearchResult queryResult = new QuerySearchResult();
        ScoreDoc[] collapsed = new ScoreDoc[] {
            new FieldDoc(10, 2.0f, new Object[] { 2.0f }),
            new FieldDoc(20, 1.0f, new Object[] { 1.0f }) };
        // Deliberately mismatched: two hits but a single collapse value.
        CollapseTopFieldDocs original = collapseDocs("group", collapsed, new Object[] { "a" });
        queryResult.topDocs(new TopDocsAndMaxScore(original, 2.0f), new DocValueFormat[] { DocValueFormat.RAW });

        RecordingRescorer rescorer = new RecordingRescorer(scoreDocs(0));
        SearchContext context = mockContext(queryResult, rescorer);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new RescoreProcessor().process(context));
        assertEquals("scoreDocs and collapseValues arrays must have the same length", e.getMessage());
        assertEquals(0, rescorer.invocations);
    }

    public void testReconstructFailsWhenRescoreIntroducesUnknownDoc() {
        QuerySearchResult queryResult = new QuerySearchResult();
        ScoreDoc[] collapsed = new ScoreDoc[] {
            new FieldDoc(10, 2.0f, new Object[] { 2.0f }),
            new FieldDoc(20, 1.0f, new Object[] { 1.0f }) };
        CollapseTopFieldDocs original = collapseDocs("group", collapsed, new Object[] { "a", "b" });
        queryResult.topDocs(new TopDocsAndMaxScore(original, 2.0f), new DocValueFormat[] { DocValueFormat.RAW });

        // Doc 99 was never part of the original results.
        TopDocs rescored = scoreDocs(2, new ScoreDoc(99, 5.0f), new ScoreDoc(10, 4.0f));
        RecordingRescorer rescorer = new RecordingRescorer(rescored);
        SearchContext context = mockContext(queryResult, rescorer);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new RescoreProcessor().process(context));
        assertEquals("rescore must not introduce new docs, but doc 99 was not in original results", e.getMessage());
        // Unlike testCaptureFailsWhenCollapseArrayLengthsMismatch (which fails before rescoring),
        // this failure happens during reconstruction, i.e. after rescoring ran.
        assertEquals(1, rescorer.invocations);
    }

    public void testMultipleRescorersAreAppliedInOrder() {
        QuerySearchResult queryResult = new QuerySearchResult();
        TopDocs original = scoreDocs(2, new ScoreDoc(1, 2.0f), new ScoreDoc(2, 1.0f));
        queryResult.topDocs(new TopDocsAndMaxScore(original, 2.0f), new DocValueFormat[0]);

        TopDocs afterFirst = scoreDocs(2, new ScoreDoc(2, 6.0f), new ScoreDoc(1, 5.0f));
        TopDocs afterSecond = scoreDocs(2, new ScoreDoc(1, 9.0f), new ScoreDoc(2, 7.0f));
        RecordingRescorer first = new RecordingRescorer(afterFirst);
        RecordingRescorer second = new RecordingRescorer(afterSecond);

        SearchContext context = mock(SearchContext.class);
        when(context.queryResult()).thenReturn(queryResult);
        when(context.rescore()).thenReturn(List.of(new RescoreContext(10, first), new RescoreContext(10, second)));

        new RescoreProcessor().process(context);

        assertEquals(1, first.invocations);
        assertEquals(1, second.invocations);
        assertSame(afterSecond, queryResult.topDocs().topDocs);
        assertEquals(9.0f, queryResult.topDocs().maxScore, 0.0f);
    }

    public void testNoRescorersLeavesTopDocsUnchanged() {
        QuerySearchResult queryResult = new QuerySearchResult();
        TopDocs original = scoreDocs(1, new ScoreDoc(1, 2.0f));
        queryResult.topDocs(new TopDocsAndMaxScore(original, 2.0f), new DocValueFormat[0]);

        SearchContext context = mock(SearchContext.class);
        when(context.queryResult()).thenReturn(queryResult);
        when(context.rescore()).thenReturn(Collections.emptyList());

        new RescoreProcessor().process(context);

        assertSame(original, queryResult.topDocs().topDocs);
        assertEquals(2.0f, queryResult.topDocs().maxScore, 0.0f);
    }
}
