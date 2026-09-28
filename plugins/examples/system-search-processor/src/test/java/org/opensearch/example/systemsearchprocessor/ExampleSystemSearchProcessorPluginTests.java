/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemsearchprocessor;

import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;

public class ExampleSystemSearchProcessorPluginTests extends OpenSearchTestCase {
    private final ExampleSystemSearchProcessorPlugin plugin = new ExampleSystemSearchProcessorPlugin();
    private final SearchPipelinePlugin.Parameters parameters = mock(SearchPipelinePlugin.Parameters.class);

    public void testGetSystemGeneratedRequestProcessors() {
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor>> factoryMap = plugin
            .getSystemGeneratedRequestProcessors(parameters);

        assertEquals(2, factoryMap.size());
        assertTrue(factoryMap.get(ExampleSearchRequestPreProcessor.Factory.TYPE) instanceof ExampleSearchRequestPreProcessor.Factory);
        assertTrue(factoryMap.get(ExampleSearchRequestPostProcessor.Factory.TYPE) instanceof ExampleSearchRequestPostProcessor.Factory);
    }

    public void testGetSystemGeneratedSearchPhaseResultsProcessors() {
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor>> factoryMap = plugin
            .getSystemGeneratedSearchPhaseResultsProcessors(parameters);

        assertEquals(1, factoryMap.size());
        assertTrue(factoryMap.get(ExampleSearchPhaseResultsProcessor.Factory.TYPE) instanceof ExampleSearchPhaseResultsProcessor.Factory);
    }

    public void testGetSystemGeneratedResponseProcessors() {
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>> factoryMap = plugin
            .getSystemGeneratedResponseProcessors(parameters);

        assertEquals(1, factoryMap.size());
        assertTrue(factoryMap.get(ExampleSearchResponseProcessor.Factory.TYPE) instanceof ExampleSearchResponseProcessor.Factory);
    }
}
