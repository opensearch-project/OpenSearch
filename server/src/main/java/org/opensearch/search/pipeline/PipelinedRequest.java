/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchPhaseResult;

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

    public <Result extends SearchPhaseResult> SearchPhaseResults<Result> transformSearchPhase(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext,
        final String currentPhase,
        final String nextPhase
    ) {
        return pipeline.runSearchPhaseTransformer(searchPhaseResult, searchPhaseContext, currentPhase, nextPhase);
    }

    // Visible for testing
    Pipeline getPipeline() {
        return pipeline;
    }

    /**
     * Wraps a search request with a no-op pipeline. Useful for testing.
     *
     * @param searchRequest the original search request
     * @return a search request associated with a pipeline that does nothing
     */
    public static PipelinedRequest wrapSearchRequest(SearchRequest searchRequest) {
        return new PipelinedRequest(Pipeline.NO_OP_PIPELINE, searchRequest);
    }

    /**
     * Wraps the given search request with this request's pipeline.
     */
    public PipelinedRequest replaceRequest(SearchRequest searchRequest) {
        return new PipelinedRequest(pipeline, searchRequest);
    }
}
