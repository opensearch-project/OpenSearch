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
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchPhaseResult;

import java.util.List;

/**
 * Groups a search pipeline based on a request and the request after being transformed by the pipeline.
 *
 * @opensearch.internal
 */
public final class PipelinedRequest extends SearchRequest {
    private final Pipeline pipeline;
    private final PipelineProcessingContext requestContext;
    private final SystemGeneratedPipelineHolder systemGeneratedPipelineHolder;

    PipelinedRequest(
        Pipeline pipeline,
        SearchRequest transformedRequest,
        PipelineProcessingContext requestContext,
        SystemGeneratedPipelineHolder systemGeneratedPipelineHolder
    ) {
        super(transformedRequest);
        this.pipeline = pipeline;
        this.requestContext = requestContext;
        this.systemGeneratedPipelineHolder = systemGeneratedPipelineHolder;
    }

    PipelinedRequest(SearchRequest transformedRequest, PipelinedRequest original) {
        super(transformedRequest);
        this.pipeline = original.pipeline;
        this.requestContext = original.requestContext;
        this.systemGeneratedPipelineHolder = original.systemGeneratedPipelineHolder;
    }

    public void transformRequest(ActionListener<SearchRequest> requestListener) {
        List<Pipeline> pipelines = getPipelines();

        ActionListener<SearchRequest> currentListener = requestListener;
        boolean hasProcessor = false;

        // Build listener chain backwards
        for (int i = pipelines.size() - 1; i >= 0; i--) {
            Pipeline p = pipelines.get(i);
            if (p.getSearchRequestProcessors().isEmpty()) {
                continue;
            }
            hasProcessor = true;
            ActionListener<SearchRequest> nextListener = currentListener;
            currentListener = ActionListener.wrap(
                searchRequest -> p.transformRequest((PipelinedRequest) searchRequest, nextListener),
                requestListener::onFailure
            );
        }

        if (hasProcessor) {
            // Kick off with the original request
            currentListener.onResponse(this);
        } else {
            requestListener.onResponse(this);
        }
    }

    public ActionListener<SearchResponse> transformResponseListener(ActionListener<SearchResponse> responseListener) {
        List<Pipeline> pipelines = getPipelines();

        ActionListener<SearchResponse> currentListener = ActionListener.wrap(response -> {
            // Always try to attach processor execution details even there is no search response processor
            // since search request and phase results processors also can add execution details.
            List<ProcessorExecutionDetail> details = requestContext.getProcessorExecutionDetails();
            if (!details.isEmpty() && response.getInternalResponse() != null) {
                response.getInternalResponse().getProcessorResult().addAll(details);
            }
            responseListener.onResponse(response);
        }, responseListener::onFailure);

        // Build chain backwards
        for (int i = pipelines.size() - 1; i >= 0; i--) {
            Pipeline p = pipelines.get(i);
            if (p.getSearchResponseProcessors().isEmpty()) {
                continue;
            }
            ActionListener<SearchResponse> nextListener = currentListener;
            currentListener = p.transformResponseListener(this, nextListener);
        }

        return currentListener;
    }

    public <Result extends SearchPhaseResult> void transformSearchPhaseResults(
        final SearchPhaseResults<Result> searchPhaseResult,
        final SearchPhaseContext searchPhaseContext,
        final String currentPhase,
        final String nextPhase
    ) {
        List<Pipeline> pipelines = getPipelines();

        // Run forward chain
        for (Pipeline p : pipelines) {
            if (p.getSearchPhaseResultsProcessors().isEmpty()) {
                continue;
            }
            p.runSearchPhaseResultsTransformer(searchPhaseResult, searchPhaseContext, currentPhase, nextPhase, requestContext);
        }
    }

    private List<Pipeline> getPipelines() {
        return List.of(systemGeneratedPipelineHolder.prePipeline(), pipeline, systemGeneratedPipelineHolder.postPipeline());
    }

    // Visible for testing
    Pipeline getPipeline() {
        return pipeline;
    }

    public PipelineProcessingContext getPipelineProcessingContext() {
        return requestContext;
    }

    public SystemGeneratedPipelineHolder getSystemGeneratedPipelineHolder() {
        return systemGeneratedPipelineHolder;
    }
}
