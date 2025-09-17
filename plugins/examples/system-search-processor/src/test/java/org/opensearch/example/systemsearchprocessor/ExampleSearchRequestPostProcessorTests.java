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
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

public class ExampleSearchRequestPostProcessorTests extends OpenSearchTestCase {
    public void testProcessRequestIncreasesSize() {
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.size(3); // initial size

        SearchRequest request = new SearchRequest();
        request.source(source);

        ExampleSearchRequestPostProcessor processor = new ExampleSearchRequestPostProcessor("tag1", false);
        SearchRequest processed = processor.processRequest(request);

        assertNotNull(processed);
        assertEquals(5, processed.source().size()); // size + 2
    }

    public void testProcessRequestHandlesNullSource() {
        SearchRequest request = new SearchRequest(); // source is null
        ExampleSearchRequestPostProcessor processor = new ExampleSearchRequestPostProcessor("tag1", false);
        SearchRequest processed = processor.processRequest(request);

        assertSame(request, processed);
    }

    public void testGetters() {
        ExampleSearchRequestPostProcessor processor = new ExampleSearchRequestPostProcessor("tag1", true);
        assertEquals(ExampleSearchRequestPostProcessor.TYPE, processor.getType());
        assertEquals(ExampleSearchRequestPostProcessor.DESCRIPTION, processor.getDescription());
        assertEquals("tag1", processor.getTag());
        assertTrue(processor.isIgnoreFailure());
        assertEquals(ExampleSearchRequestPostProcessor.ExecutionStage.POST_USER_DEFINED, processor.getExecutionStage());
    }

    public void testFactoryShouldGenerate() {
        SearchRequest smallRequest = new SearchRequest();
        SearchSourceBuilder smallSource = new SearchSourceBuilder().size(3);
        smallRequest.source(smallSource);

        SearchRequest largeRequest = new SearchRequest();
        SearchSourceBuilder largeSource = new SearchSourceBuilder().size(6);
        largeRequest.source(largeSource);

        ExampleSearchRequestPostProcessor.Factory factory = new ExampleSearchRequestPostProcessor.Factory();

        ProcessorGenerationContext smallContext = new ProcessorGenerationContext(smallRequest);
        ProcessorGenerationContext largeContext = new ProcessorGenerationContext(largeRequest);

        assertTrue(factory.shouldGenerate(smallContext));
        assertFalse(factory.shouldGenerate(largeContext));
        assertFalse(factory.shouldGenerate(new ProcessorGenerationContext(null)));
    }

    public void testFactoryCreate() throws Exception {
        ExampleSearchRequestPostProcessor.Factory factory = new ExampleSearchRequestPostProcessor.Factory();
        ExampleSearchRequestPostProcessor processor = (ExampleSearchRequestPostProcessor) factory.create(
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
    }
}
