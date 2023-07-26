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
public final class PipelinedRequest extends SearchRequest {
    private final Pipeline pipeline;

    PipelinedRequest(Pipeline pipeline, SearchRequest transformedRequest) {
        super(transformedRequest);
        this.pipeline = pipeline;
    }

    public SearchResponse transformResponse(SearchResponse response) {
        return pipeline.transformResponse(this, response);
    }

    public <Result extends SearchPhaseResult> void transformSearchPhaseResults(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext,
        final String currentPhase,
        final String nextPhase
    ) {
        pipeline.runSearchPhaseResultsTransformer(searchPhaseResult, searchPhaseContext, currentPhase, nextPhase);
    }

    // Visible for testing
    Pipeline getPipeline() {
        return pipeline;
    }
}
