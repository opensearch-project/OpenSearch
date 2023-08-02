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
import org.opensearch.search.SearchHits;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.StatefulSearchResponseProcessor;
import org.opensearch.search.pipeline.common.helpers.SearchResponseUtil;

import java.util.Map;

/**
 * Truncates the returned search hits from the {@link SearchResponse}. If no target size is specified in the pipeline, then
 * we try using the "original_size" value from the request context, which may have been set by {@link OversampleRequestProcessor}.
 */
public class TruncateHitsResponseProcessor extends AbstractProcessor implements StatefulSearchResponseProcessor {
    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "truncate_hits";
    private static final String TARGET_SIZE = "target_size";
    private final int targetSize;

    @Override
    public String getType() {
        return TYPE;
    }

    private TruncateHitsResponseProcessor(String tag, String description, boolean ignoreFailure, int targetSize) {
        super(tag, description, ignoreFailure);
        this.targetSize = targetSize;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response, Map<String, Object> requestContext) {

        int size;
        if (targetSize < 0) {
            size = (int) requestContext.get(OversampleRequestProcessor.ORIGINAL_SIZE);
        } else {
            size = targetSize;
        }
        if (response.getHits() != null && response.getHits().getHits().length > size) {
            SearchHit[] newHits = new SearchHit[size];
            System.arraycopy(response.getHits().getHits(), 0, newHits, 0, size);
            SearchHits searchHits = new SearchHits(
                newHits,
                response.getHits().getTotalHits(),
                response.getHits().getMaxScore(),
                response.getHits().getSortFields(),
                response.getHits().getCollapseField(),
                response.getHits().getCollapseValues()
            );
            return SearchResponseUtil.replaceHits(searchHits, response);
        }
        return response;
    }

    static class Factory implements Processor.Factory<SearchResponseProcessor> {

        @Override
        public SearchResponseProcessor create(
            Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            int targetSize = ConfigurationUtils.readIntProperty(TYPE, tag, config, TARGET_SIZE, -1);
            return new TruncateHitsResponseProcessor(tag, description, ignoreFailure, targetSize);
        }
    }

}
