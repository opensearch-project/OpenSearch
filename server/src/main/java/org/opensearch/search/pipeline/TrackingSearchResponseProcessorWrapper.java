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
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper for SearchResponseProcessor to track execution details.
 *
 *  @opensearch.internal
 */
public class TrackingSearchResponseProcessorWrapper implements SearchResponseProcessor {

    private final SearchResponseProcessor wrappedProcessor;

    /**
     * Constructor for the wrapper.
     *
     * @param wrappedProcessor the actual processor to be wrapped
     */
    public TrackingSearchResponseProcessorWrapper(SearchResponseProcessor wrappedProcessor) {
        if (wrappedProcessor == null) {
            throw new IllegalArgumentException("Wrapped processor cannot be null.");
        }
        this.wrappedProcessor = wrappedProcessor;
    }

    @Override
    public String getType() {
        return wrappedProcessor.getType();
    }

    @Override
    public String getTag() {
        return wrappedProcessor.getTag();
    }

    @Override
    public String getDescription() {
        return wrappedProcessor.getDescription();
    }

    @Override
    public boolean isIgnoreFailure() {
        return wrappedProcessor.isIgnoreFailure();
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void processResponseAsync(
        SearchRequest request,
        SearchResponse response,
        PipelineProcessingContext requestContext,
        ActionListener<SearchResponse> responseListener
    ) {
        ProcessorExecutionDetail detail = new ProcessorExecutionDetail(getType(), getTag());
        long start = System.nanoTime();
        try {
            detail.addInput(Arrays.asList(response.getHits().deepCopy().getHits()));
        } catch (IOException e) {
            responseListener.onFailure(e);
            return;
        }
        wrappedProcessor.processResponseAsync(request, response, requestContext, ActionListener.wrap(result -> {
            detail.addOutput(Arrays.asList(result.getHits().deepCopy().getHits()));
            long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            detail.addTook(took);
            requestContext.addProcessorExecutionDetail(detail);
            responseListener.onResponse(result);
        }, e -> {
            detail.markProcessorAsFailed(ProcessorExecutionDetail.ProcessorStatus.FAIL, e.getMessage());
            requestContext.addProcessorExecutionDetail(detail);
            responseListener.onFailure(e);
        }));
    }

}
