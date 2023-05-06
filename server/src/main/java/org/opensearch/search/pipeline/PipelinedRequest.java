/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;

/**
 * Groups a search pipeline based on a request and the request after being transformed by the pipeline.
 *
 * @opensearch.internal
 */
public final class PipelinedRequest {
    private final Pipeline pipeline;
    private final SearchRequest transformedRequest;

    PipelinedRequest(Pipeline pipeline, SearchRequest transformedRequest) {
        this.pipeline = pipeline;
        this.transformedRequest = transformedRequest;
    }

    public SearchResponse transformResponse(SearchResponse response) {
        return pipeline.transformResponse(transformedRequest, response);
    }

    public SearchRequest transformedRequest() {
        return transformedRequest;
    }

    // Visible for testing
    Pipeline getPipeline() {
        return pipeline;
    }
}
