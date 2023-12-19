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

/**
 * Interface for a search pipeline processor that modifies a search response.
 */
public interface SearchResponseProcessor extends Processor {

    /**
     * Transform a {@link SearchResponse}, possibly based on the executed {@link SearchRequest}.
     * <p>
     * Implement this method if the processor makes no asynchronous calls.
     *
     * @param request  the executed {@link SearchRequest}
     * @param response the current {@link SearchResponse}, possibly modified by earlier processors
     * @return a modified {@link SearchResponse} (or the input {@link SearchResponse} if no changes)
     * @throws Exception if an error occurs during processing
     */
    SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception;

    /**
     * Process a SearchResponse, with request-scoped state shared across processors in the pipeline
     * <p>
     * Implement this method if the processor makes no asynchronous calls.
     *
     * @param request        the (maybe transformed) search request
     * @param response       the search response (which may have been modified by an earlier processor)
     * @param requestContext request-scoped state shared across processors in the pipeline
     * @return the modified search response
     * @throws Exception implementation-specific processing exception
     */
    default SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext)
        throws Exception {
        return processResponse(request, response);
    }

    /**
     * Transform a {@link SearchResponse}, possibly based on the executed {@link SearchRequest}.
     * <p>
     * Expert method: Implement this if the processor needs to make asynchronous calls. Otherwise, implement processResponse.
     *
     * @param request          the executed {@link SearchRequest}
     * @param response         the current {@link SearchResponse}, possibly modified by earlier processors
     * @param responseListener callback to be invoked on successful processing or on failure
     */
    default void processResponseAsync(
        SearchRequest request,
        SearchResponse response,
        PipelineProcessingContext requestContext,
        ActionListener<SearchResponse> responseListener
    ) {
        try {
            responseListener.onResponse(processResponse(request, response, requestContext));
        } catch (Exception e) {
            responseListener.onFailure(e);
        }
    }
}
