/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemsearchprocessor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

public class ExampleSearchRequestPreProcessorTests extends OpenSearchTestCase {
    public void testProcessRequestThrowsUnsupported() {
        ExampleSearchRequestPreProcessor processor = new ExampleSearchRequestPreProcessor("tag1", false);
        SearchRequest request = new SearchRequest();

        UnsupportedOperationException ex = assertThrows(UnsupportedOperationException.class, () -> processor.processRequest(request));

        assertTrue(ex.getMessage().contains(ExampleSearchRequestPreProcessor.TYPE));
    }

    public void testProcessRequestWithContextIncrementsSizeAndStoresOriginal() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().size(3);
        SearchRequest request = new SearchRequest();
        request.source(source);

        PipelineProcessingContext context = new PipelineProcessingContext();

        ExampleSearchRequestPreProcessor processor = new ExampleSearchRequestPreProcessor("tag1", false);
        SearchRequest processed = processor.processRequest(request, context);

        assertNotNull(processed);
        assertEquals(4, processed.source().size()); // original 3 + 1
        assertEquals(3, context.getAttribute(ExampleSearchRequestPreProcessor.ORIGINAL_QUERY_SIZE_KEY));
    }

    public void testProcessRequestHandlesNullRequestOrSource() throws Exception {
        ExampleSearchRequestPreProcessor processor = new ExampleSearchRequestPreProcessor("tag1", false);
        PipelineProcessingContext context = new PipelineProcessingContext();

        // null request
        SearchRequest processed = processor.processRequest(null, context);
        assertNull(processed);

        // null source
        SearchRequest reqWithNullSource = new SearchRequest();
        processed = processor.processRequest(reqWithNullSource, context);
        assertSame(reqWithNullSource, processed);
    }

    public void testGettersAndExecutionStage() {
        ExampleSearchRequestPreProcessor processor = new ExampleSearchRequestPreProcessor("tag1", true);

        assertEquals(ExampleSearchRequestPreProcessor.TYPE, processor.getType());
        assertEquals(ExampleSearchRequestPreProcessor.DESCRIPTION, processor.getDescription());
        assertEquals("tag1", processor.getTag());
        assertTrue(processor.isIgnoreFailure());
        assertEquals(ExampleSearchRequestPreProcessor.ExecutionStage.PRE_USER_DEFINED, processor.getExecutionStage());
    }

    public void testFactoryShouldGenerate() {
        ExampleSearchRequestPreProcessor.Factory factory = new ExampleSearchRequestPreProcessor.Factory();

        // original size < 5 => should generate
        SearchRequest smallRequest = new SearchRequest();
        smallRequest.source(new SearchSourceBuilder().size(4));
        ProcessorGenerationContext smallContext = new ProcessorGenerationContext(smallRequest);
        assertTrue(factory.shouldGenerate(smallContext));

        // original size = 5 => should not generate
        SearchRequest boundaryRequest = new SearchRequest();
        boundaryRequest.source(new SearchSourceBuilder().size(5));
        ProcessorGenerationContext boundaryContext = new ProcessorGenerationContext(boundaryRequest);
        assertFalse(factory.shouldGenerate(boundaryContext));

        // original size > 5 => should not generate
        SearchRequest largeRequest = new SearchRequest();
        largeRequest.source(new SearchSourceBuilder().size(6));
        ProcessorGenerationContext largeContext = new ProcessorGenerationContext(largeRequest);
        assertFalse(factory.shouldGenerate(largeContext));

        // null request => should not generate
        ProcessorGenerationContext nullContext = new ProcessorGenerationContext(null);
        assertFalse(factory.shouldGenerate(nullContext));

        // null source => should not generate
        SearchRequest nullSourceRequest = new SearchRequest();
        ProcessorGenerationContext nullSourceContext = new ProcessorGenerationContext(nullSourceRequest);
        assertFalse(factory.shouldGenerate(nullSourceContext));
    }

    public void testFactoryCreate() throws Exception {
        ExampleSearchRequestPreProcessor.Factory factory = new ExampleSearchRequestPreProcessor.Factory();

        ExampleSearchRequestPreProcessor processor = (ExampleSearchRequestPreProcessor) factory.create(
            null,
            "tagX",
            "desc",
            true,
            Collections.emptyMap(),
            null
        );

        assertNotNull(processor);
        assertEquals("tagX", processor.getTag());
        assertTrue(processor.isIgnoreFailure());
        assertEquals(ExampleSearchRequestPreProcessor.TYPE, processor.getType());
    }
}
