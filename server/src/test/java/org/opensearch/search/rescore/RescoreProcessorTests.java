/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.rescore;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RescoreProcessorTests extends OpenSearchTestCase {

    public void testTopDocsCutToFromPlusSize() {
        int from = randomIntBetween(0, 10);
        int size = randomIntBetween(1, 10);
        int windowSize = randomIntBetween(from + size + 1, 100);
        TopDocs topDocs = runRescore(from, size, windowSize);

        assertEquals(from + size, topDocs.scoreDocs.length);
        assertEquals(1000, topDocs.totalHits.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocs.totalHits.relation());
        // The rescorer reverses the order, thus the last collected doc is now the top hit.
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            assertEquals(windowSize - 1 - i, topDocs.scoreDocs[i].doc);
        }
    }

    public void testTopDocsNotCutWhenWindowIsSmall() {
        int from = randomIntBetween(0, 10);
        int size = randomIntBetween(1, 10);
        int windowSize = randomIntBetween(1, from + size);
        TopDocs topDocs = runRescore(from, size, windowSize);

        assertEquals(windowSize, topDocs.scoreDocs.length);
    }

    public void testEachRescorerGetsAllCollectedDocs() {
        int from = randomIntBetween(0, 10);
        int size = randomIntBetween(1, 10);
        int windowSize = randomIntBetween(from + size + 1, 100);
        List<Integer> seenSizes = new ArrayList<>();
        TopDocs topDocs = runRescore(from, size, windowSize, new ReverseRescorer(seenSizes), new ReverseRescorer(seenSizes));

        assertEquals(List.of(windowSize, windowSize), seenSizes);
        assertEquals(from + size, topDocs.scoreDocs.length);
        // Two reverses give the collected order again.
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            assertEquals(i, topDocs.scoreDocs[i].doc);
        }
    }

    public void testMaxScoreKeptWhenAllDocsCut() throws IOException {
        int windowSize = randomIntBetween(1, 100);
        TopDocsAndMaxScore result = RescoreProcessor.rescore(
            collectedDocs(windowSize),
            null,
            List.of(new RescoreContext(windowSize, new ReverseRescorer(new ArrayList<>()))),
            0
        );

        assertEquals(0, result.topDocs.scoreDocs.length);
        assertEquals(windowSize, result.maxScore, 0f);
    }

    private TopDocs runRescore(int from, int size, int windowSize) {
        return runRescore(from, size, windowSize, new ReverseRescorer(new ArrayList<>()));
    }

    private static TopDocs collectedDocs(int windowSize) {
        ScoreDoc[] scoreDocs = new ScoreDoc[windowSize];
        for (int i = 0; i < windowSize; i++) {
            scoreDocs[i] = new ScoreDoc(i, windowSize - i);
        }
        return new TopDocs(new TotalHits(1000, TotalHits.Relation.EQUAL_TO), scoreDocs);
    }

    private TopDocs runRescore(int from, int size, int windowSize, Rescorer... rescorers) {
        TopDocs collected = collectedDocs(windowSize);
        QuerySearchResult queryResult = new QuerySearchResult();
        queryResult.topDocs(new TopDocsAndMaxScore(collected, windowSize), new DocValueFormat[0]);

        SearchContext context = mock(SearchContext.class);
        when(context.queryResult()).thenReturn(queryResult);
        List<RescoreContext> rescore = new ArrayList<>();
        for (Rescorer rescorer : rescorers) {
            rescore.add(new RescoreContext(windowSize, rescorer));
        }
        when(context.rescore()).thenReturn(rescore);
        when(context.from()).thenReturn(from);
        when(context.size()).thenReturn(size);

        new RescoreProcessor().process(context);

        TopDocsAndMaxScore result = queryResult.topDocs();
        assertEquals(result.topDocs.scoreDocs[0].score, result.maxScore, 0f);
        return result.topDocs;
    }

    /** Gives the docs the reverse of their order, and records how many docs it got. */
    private static class ReverseRescorer implements Rescorer {
        private final List<Integer> seenSizes;

        ReverseRescorer(List<Integer> seenSizes) {
            this.seenSizes = seenSizes;
        }

        @Override
        public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) {
            seenSizes.add(topDocs.scoreDocs.length);
            ScoreDoc[] scoreDocs = new ScoreDoc[topDocs.scoreDocs.length];
            for (int i = 0; i < scoreDocs.length; i++) {
                ScoreDoc doc = topDocs.scoreDocs[scoreDocs.length - 1 - i];
                scoreDocs[i] = new ScoreDoc(doc.doc, scoreDocs.length - i);
            }
            return new TopDocs(topDocs.totalHits, scoreDocs);
        }

        @Override
        public Explanation explain(
            int topLevelDocId,
            IndexSearcher searcher,
            RescoreContext rescoreContext,
            Explanation sourceExplanation
        ) {
            return sourceExplanation;
        }
    }
}
