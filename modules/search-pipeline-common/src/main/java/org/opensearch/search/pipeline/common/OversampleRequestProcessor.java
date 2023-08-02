/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.pipeline.StatefulSearchRequestProcessor;

import java.util.Map;

/**
 * Multiplies the "size" parameter on the {@link SearchRequest} by the given scaling factor, storing the original value
 * in the request context as "original_size".
 */
public class OversampleRequestProcessor extends AbstractProcessor implements StatefulSearchRequestProcessor {

    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "oversample";
    private static final String SAMPLE_FACTOR = "sample_factor";
    static final String ORIGINAL_SIZE = "original_size";
    private final double sampleFactor;

    private OversampleRequestProcessor(String tag, String description, boolean ignoreFailure, double sampleFactor) {
        super(tag, description, ignoreFailure);
        this.sampleFactor = sampleFactor;
    }

    @Override
    public SearchRequest processRequest(SearchRequest request, Map<String, Object> requestContext) {
        if (request.source() != null) {
            int originalSize = request.source().size();
            requestContext.put(ORIGINAL_SIZE, originalSize);
            int newSize = (int) Math.ceil(originalSize * sampleFactor);
            request.source().size(newSize);
        }
        return request;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    static class Factory implements Processor.Factory<SearchRequestProcessor> {

        @Override
        public OversampleRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            double sampleFactor = ConfigurationUtils.readDoubleProperty(TYPE, tag, config, SAMPLE_FACTOR);
            if (sampleFactor < 1.0) {
                throw ConfigurationUtils.newConfigurationException(TYPE, tag, SAMPLE_FACTOR, "Value must be >= 1.0");
            }
            return new OversampleRequestProcessor(tag, description, ignoreFailure, sampleFactor);
        }
    }
}
