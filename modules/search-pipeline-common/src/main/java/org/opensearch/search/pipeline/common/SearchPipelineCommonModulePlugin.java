/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPipelinesPlugin;
import org.opensearch.search.pipeline.Processor;

import java.util.Map;

/**
 * Plugin providing common search request/response processors for use in search pipelines.
 */
public class SearchPipelineCommonModulePlugin extends Plugin implements SearchPipelinesPlugin {

    /**
     * No constructor needed, but build complains if we don't have a constructor with JavaDoc.
     */
    public SearchPipelineCommonModulePlugin() {}

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(FilterQueryRequestProcessor.TYPE, new FilterQueryRequestProcessor.Factory(parameters.namedXContentRegistry));
    }
}
