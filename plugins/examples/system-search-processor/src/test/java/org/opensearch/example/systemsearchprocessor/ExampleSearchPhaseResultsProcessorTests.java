/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemsearchprocessor;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExampleSearchPhaseResultsProcessorTests extends OpenSearchTestCase {
    private final ExampleSearchPhaseResultsProcessor.Factory factory = new ExampleSearchPhaseResultsProcessor.Factory();
    private final ProcessorGenerationContext context = new ProcessorGenerationContext(mock(SearchRequest.class));

    public void testShouldGenerate_thenAlwaysTrue() {
        assertTrue(factory.shouldGenerate(context));
    }

    public void testProcess() throws Exception {
        // Create a TopDocs with some initial maxScore
        ScoreDoc[] scoreDocs = new ScoreDoc[] { new ScoreDoc(0, 1.5f) };
        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), scoreDocs);

        // Wrap in a QuerySearchResult
        QuerySearchResult querySearchResult = new QuerySearchResult();
        querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 1.5f), null);

        // Wrap in a SearchPhaseResult (mocked to keep it simple)
        SearchPhaseResult searchPhaseResult = mock(SearchPhaseResult.class);
        when(searchPhaseResult.queryResult()).thenReturn(querySearchResult);

        // Wrap in SearchPhaseResults with a single shard result
        @SuppressWarnings("unchecked")
        SearchPhaseResults<SearchPhaseResult> results = mock(SearchPhaseResults.class);
        AtomicArray<SearchPhaseResult> resultAtomicArray = new AtomicArray<>(1);
        resultAtomicArray.set(0, searchPhaseResult);
        when(results.getAtomicArray()).thenReturn(resultAtomicArray);

        // Create a dummy context
        SearchPhaseContext context = mock(SearchPhaseContext.class);

        SearchPhaseResultsProcessor processor = factory.create(null, null, null, true, null, null);
        processor.process(results, context);

        // Verify maxScore is set to 10
        assertEquals(10f, querySearchResult.topDocs().maxScore, 0.0001f);
    }
}
