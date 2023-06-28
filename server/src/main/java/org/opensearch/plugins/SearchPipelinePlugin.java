/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.Collections;
import java.util.Map;

/**
 * An extension point for {@link Plugin} implementation to add custom search pipeline processors.
 *
 * @opensearch.api
 */
public interface SearchPipelinePlugin {
    /**
     * Returns additional search pipeline request processor types added by this plugin.
     *
     * The key of the returned {@link Map} is the unique name for the processor which is specified
     * in pipeline configurations, and the value is a {@link org.opensearch.search.pipeline.Processor.Factory}
     * to create the processor from a given pipeline configuration.
     */
    default Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessors(Processor.Parameters parameters) {
        return Collections.emptyMap();
    }

    /**
     * Returns additional search pipeline response processor types added by this plugin.
     *
     * The key of the returned {@link Map} is the unique name for the processor which is specified
     * in pipeline configurations, and the value is a {@link org.opensearch.search.pipeline.Processor.Factory}
     * to create the processor from a given pipeline configuration.
     */
    default Map<String, Processor.Factory<SearchResponseProcessor>> getResponseProcessors(Processor.Parameters parameters) {
        return Collections.emptyMap();
    }

    /**
     * Returns additional search pipeline search phase results processor types added by this plugin.
     *
     * The key of the returned {@link Map} is the unique name for the processor which is specified
     * in pipeline configurations, and the value is a {@link org.opensearch.search.pipeline.Processor.Factory}
     * to create the processor from a given pipeline configuration.
     */
    default Map<String, Processor.Factory<SearchPhaseResultsProcessor>> getSearchPhaseResultsProcessors(Processor.Parameters parameters) {
        return Collections.emptyMap();
    }
}
