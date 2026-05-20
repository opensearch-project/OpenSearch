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

/**
 * Interface for a search pipeline processor that modifies a search request.
 */
public interface SearchRequestProcessor extends Processor {
    /**
     * Process a SearchRequest without receiving request-scoped state.
     * Implement this method if the processor makes no asynchronous calls.
     * @param request the search request (which may have been modified by an earlier processor)
     * @return the modified search request
     * @throws Exception implementation-specific processing exception
     */
    SearchRequest processRequest(SearchRequest request) throws Exception;

    /**
     * Process a SearchRequest, with request-scoped state shared across processors in the pipeline
     * Implement this method if the processor makes no asynchronous calls.
     * @param request the search request (which may have been modified by an earlier processor)
     * @param requestContext request-scoped state shared across processors in the pipeline
     * @return the modified search request
     * @throws Exception implementation-specific processing exception
     */
    default SearchRequest processRequest(SearchRequest request, PipelineProcessingContext requestContext) throws Exception {
        return processRequest(request);
    }

    /**
     * Transform a {@link SearchRequest}. Executed on the coordinator node before any {@link org.opensearch.action.search.SearchPhase}
     * executes.
     * <p>
     * Expert method: Implement this if the processor needs to make asynchronous calls. Otherwise, implement processRequest.
     * @param request the executed {@link SearchRequest}
     * @param requestListener callback to be invoked on successful processing or on failure
     */
    default void processRequestAsync(
        SearchRequest request,
        PipelineProcessingContext requestContext,
        ActionListener<SearchRequest> requestListener
    ) {
        try {
            requestListener.onResponse(processRequest(request, requestContext));
        } catch (Exception e) {
            requestListener.onFailure(e);
        }
    }
}
