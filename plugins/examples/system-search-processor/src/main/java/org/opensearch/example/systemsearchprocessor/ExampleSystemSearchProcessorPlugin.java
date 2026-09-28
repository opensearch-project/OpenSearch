/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.systemsearchprocessor;

import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

import java.util.Map;

/**
 * An example plugin to demonstrate how to use system generated search processors.
 */
public class ExampleSystemSearchProcessorPlugin extends Plugin implements SearchPipelinePlugin {
    /**
     * Constructs a new ExampleSystemSearchProcessorPlugin
     */
    public ExampleSystemSearchProcessorPlugin() {}

    @Override
    public Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchRequestProcessor>> getSystemGeneratedRequestProcessors(
        Parameters parameters
    ) {
        return Map.of(
            ExampleSearchRequestPreProcessor.Factory.TYPE,
            new ExampleSearchRequestPreProcessor.Factory(),
            ExampleSearchRequestPostProcessor.Factory.TYPE,
            new ExampleSearchRequestPostProcessor.Factory()
        );
    }

    @Override
    public
        Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchPhaseResultsProcessor>>
        getSystemGeneratedSearchPhaseResultsProcessors(Parameters parameters) {
        return Map.of(ExampleSearchPhaseResultsProcessor.Factory.TYPE, new ExampleSearchPhaseResultsProcessor.Factory());
    }

    @Override
    public Map<String, SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor>> getSystemGeneratedResponseProcessors(
        Parameters parameters
    ) {
        return Map.of(ExampleSearchResponseProcessor.Factory.TYPE, new ExampleSearchResponseProcessor.Factory());
    }
}
