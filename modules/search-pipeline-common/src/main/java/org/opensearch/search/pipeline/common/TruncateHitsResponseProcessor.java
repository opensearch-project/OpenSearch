/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.SearchHit;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.StatefulSearchResponseProcessor;
import org.opensearch.search.pipeline.common.helpers.ContextUtils;
import org.opensearch.search.pipeline.common.helpers.SearchResponseUtil;

import java.util.Map;

import static org.opensearch.search.pipeline.common.helpers.ContextUtils.applyContextPrefix;

/**
 * Truncates the returned search hits from the {@link SearchResponse}. If no target size is specified in the pipeline, then
 * we try using the "original_size" value from the request context, which may have been set by {@link OversampleRequestProcessor}.
 */
public class TruncateHitsResponseProcessor extends AbstractProcessor implements StatefulSearchResponseProcessor {
    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "truncate_hits";
    static final String TARGET_SIZE = "target_size";
    private final int targetSize;
    private final String contextPrefix;

    @Override
    public String getType() {
        return TYPE;
    }

    private TruncateHitsResponseProcessor(String tag, String description, boolean ignoreFailure, int targetSize, String contextPrefix) {
        super(tag, description, ignoreFailure);
        this.targetSize = targetSize;
        this.contextPrefix = contextPrefix;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext) {
        int size;
        if (targetSize < 0) { // No value specified in processor config. Use context value instead.
            String key = applyContextPrefix(contextPrefix, OversampleRequestProcessor.ORIGINAL_SIZE);
            Object o = requestContext.getAttribute(key);
            if (o == null) {
                throw new IllegalStateException("Must specify " + TARGET_SIZE + " unless an earlier processor set " + key);
            }
            size = (int) o;
        } else {
            size = targetSize;
        }
        if (response.getHits() != null && response.getHits().getHits().length > size) {
            SearchHit[] newHits = new SearchHit[size];
            System.arraycopy(response.getHits().getHits(), 0, newHits, 0, size);
            return SearchResponseUtil.replaceHits(newHits, response);
        }
        return response;
    }

    static class Factory implements Processor.Factory<SearchResponseProcessor> {
        @Override
        public TruncateHitsResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            Integer targetSize = ConfigurationUtils.readIntProperty(TYPE, tag, config, TARGET_SIZE, null);
            if (targetSize == null) {
                // Use -1 as an "unset" marker to avoid repeated unboxing of an Integer.
                targetSize = -1;
            } else {
                // Explicitly set values must be >= 0.
                if (targetSize < 0) {
                    throw ConfigurationUtils.newConfigurationException(TYPE, tag, TARGET_SIZE, "Value must be >= 0");
                }
            }
            String contextPrefix = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, ContextUtils.CONTEXT_PREFIX_PARAMETER);
            return new TruncateHitsResponseProcessor(tag, description, ignoreFailure, targetSize, contextPrefix);
        }
    }
}
