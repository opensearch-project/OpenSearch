/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemsearchprocessor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.ProcessorConflictEvaluationContext;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExampleSearchResponseProcessorTests extends OpenSearchTestCase {

    public void testProcessResponseUnsupportedOperation() {
        ExampleSearchResponseProcessor processor = new ExampleSearchResponseProcessor("tag", false);
        SearchRequest request = new SearchRequest();
        SearchResponse response = mock(SearchResponse.class);

        UnsupportedOperationException ex = assertThrows(
            UnsupportedOperationException.class,
            () -> processor.processResponse(request, response)
        );
        assertTrue(ex.getMessage().contains(ExampleSearchResponseProcessor.TYPE));
    }

    public void testProcessResponseTruncatesHits() throws Exception {
        ExampleSearchResponseProcessor processor = new ExampleSearchResponseProcessor("tag", false);

        SearchHit hit1 = new SearchHit(1);
        hit1.score(5.0f);
        SearchHit hit2 = new SearchHit(2);
        hit2.score(8.0f);
        SearchHit hit3 = new SearchHit(3);
        hit3.score(3.0f);

        SearchHits hits = new SearchHits(new SearchHit[] { hit1, hit2, hit3 }, null, 8.0f);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 1, List.of());
        SearchResponse response = new SearchResponse(sections, null, 1, 1, 0, 10, null, null, null, null);

        PipelineProcessingContext ctx = new PipelineProcessingContext();
        ctx.setAttribute(ExampleSearchRequestPreProcessor.ORIGINAL_QUERY_SIZE_KEY, 2);

        SearchResponse processed = processor.processResponse(new SearchRequest(), response, ctx);

        SearchHits newHits = processed.getHits();
        assertEquals(2, newHits.getHits().length);
        assertEquals(8.0f, newHits.getMaxScore(), 0.0001f);
    }

    public void testProcessResponseDoesNotTruncateWhenOriginalSizeLarger() throws Exception {
        ExampleSearchResponseProcessor processor = new ExampleSearchResponseProcessor("tag", false);

        SearchHit hit1 = new SearchHit(1);
        hit1.score(1.0f);

        SearchHits hits = new SearchHits(new SearchHit[] { hit1 }, null, 1.0f);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 1, List.of());
        SearchResponse response = new SearchResponse(sections, null, 1, 1, 0, 10, null, null, null, null);

        PipelineProcessingContext ctx = new PipelineProcessingContext();
        ctx.setAttribute(ExampleSearchRequestPreProcessor.ORIGINAL_QUERY_SIZE_KEY, 5);

        SearchResponse processed = processor.processResponse(new SearchRequest(), response, ctx);

        assertSame(response, processed);
    }

    public void testEvaluateConflictsThrowsWhenConflictProcessorExists() {
        ExampleSearchResponseProcessor processor = new ExampleSearchResponseProcessor("tag", false);
        ProcessorConflictEvaluationContext ctx = mock(ProcessorConflictEvaluationContext.class);

        SearchResponseProcessor mockProcessor = mock(SearchResponseProcessor.class);
        when(mockProcessor.getType()).thenReturn("truncate_hits");
        when(ctx.getUserDefinedSearchResponseProcessors()).thenReturn(List.of(mockProcessor));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> processor.evaluateConflicts(ctx));
        assertTrue(ex.getMessage().contains("truncate_hits"));
    }

    public void testEvaluateConflictsDoesNothingWhenNoConflict() {
        ExampleSearchResponseProcessor processor = new ExampleSearchResponseProcessor("tag", false);
        ProcessorConflictEvaluationContext ctx = mock(ProcessorConflictEvaluationContext.class);

        when(ctx.getUserDefinedSearchResponseProcessors()).thenReturn(List.of());

        processor.evaluateConflicts(ctx);
    }

    public void testFactoryShouldGenerateTrueWhenMatchQueryOnTriggerField() {
        ExampleSearchResponseProcessor.Factory factory = new ExampleSearchResponseProcessor.Factory();
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchQueryBuilder("trigger_field", "value"));
        request.source(sourceBuilder);

        ProcessorGenerationContext ctx = new ProcessorGenerationContext(request);
        assertTrue(factory.shouldGenerate(ctx));
    }

    public void testFactoryShouldGenerateFalseForNonMatchQuery() {
        ExampleSearchResponseProcessor.Factory factory = new ExampleSearchResponseProcessor.Factory();
        SearchRequest request = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchQueryBuilder("field", "value"));
        request.source(sourceBuilder);

        ProcessorGenerationContext ctx = new ProcessorGenerationContext(request);
        assertFalse(factory.shouldGenerate(ctx));
    }

    public void testFactoryCreateReturnsProcessor() throws Exception {
        ExampleSearchResponseProcessor.Factory factory = new ExampleSearchResponseProcessor.Factory();
        ExampleSearchResponseProcessor processor = (ExampleSearchResponseProcessor) factory.create(
            Map.of(),
            "tag",
            "desc",
            true,
            Map.of(),
            null
        );

        assertNotNull(processor);
        assertEquals("tag", processor.getTag());
        assertTrue(processor.isIgnoreFailure());
    }
}
