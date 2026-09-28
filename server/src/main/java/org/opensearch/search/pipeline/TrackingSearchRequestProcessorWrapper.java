/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.TimeUnit;

/**
 * Wrapper for SearchRequestProcessor to track execution details.
 *
 *  @opensearch.internal
 */
public class TrackingSearchRequestProcessorWrapper implements SearchRequestProcessor {

    private final SearchRequestProcessor wrappedProcessor;

    /**
     * Constructor for the wrapper.
     *
     * @param wrappedProcessor the actual processor to be wrapped
     */
    public TrackingSearchRequestProcessorWrapper(SearchRequestProcessor wrappedProcessor) {
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
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public SearchRequest processRequest(SearchRequest request, PipelineProcessingContext requestContext) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void processRequestAsync(
        SearchRequest request,
        PipelineProcessingContext requestContext,
        ActionListener<SearchRequest> requestListener
    ) {
        ProcessorExecutionDetail detail = new ProcessorExecutionDetail(getType(), getTag());
        long start = System.nanoTime();
        detail.addInput(request.source().toString());
        wrappedProcessor.processRequestAsync(request, requestContext, ActionListener.wrap(result -> {
            detail.addOutput(result.source().toString());
            long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            detail.addTook(took);
            requestContext.addProcessorExecutionDetail(detail);
            requestListener.onResponse(result);
        }, e -> {
            detail.markProcessorAsFailed(ProcessorExecutionDetail.ProcessorStatus.FAIL, e.getMessage());
            requestContext.addProcessorExecutionDetail(detail);
            requestListener.onFailure(e);
        }));
    }
}
