/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.Map;

/**
 * Plugin providing common search request/response processors for use in search pipelines.
 */
public class SearchPipelineCommonModulePlugin extends Plugin implements SearchPipelinePlugin {

    /**
     * No constructor needed, but build complains if we don't have a constructor with JavaDoc.
     */
    public SearchPipelineCommonModulePlugin() {}

    /**
     * Returns a map of processor factories.
     *
     * @param parameters The parameters required for creating the processor factories.
     * @return A map of processor factories, where the keys are the processor types and the values are the corresponding factory instances.
     */
    @Override
    public Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessors(Parameters parameters) {
        return Map.of(
            FilterQueryRequestProcessor.TYPE,
            new FilterQueryRequestProcessor.Factory(parameters.namedXContentRegistry),
            ScriptRequestProcessor.TYPE,
            new ScriptRequestProcessor.Factory(parameters.scriptService)
        );
    }

    @Override
    public Map<String, Processor.Factory<SearchResponseProcessor>> getResponseProcessors(Parameters parameters) {
        return Map.of(RenameFieldResponseProcessor.TYPE, new RenameFieldResponseProcessor.Factory());
    }
}
