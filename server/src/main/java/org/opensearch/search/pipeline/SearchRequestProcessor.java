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
     * Transform a {@link SearchRequest}. Executed on the coordinator node before any {@link org.opensearch.action.search.SearchPhase}
     * executes.
     * <p>
     * Implement this method if the processor makes no asynchronous calls.
     * @param request the executed {@link SearchRequest}
     * @return a new {@link SearchRequest} (or the input {@link SearchRequest} if no changes)
     * @throws Exception if an error occurs during processing
     */
    SearchRequest processRequest(SearchRequest request) throws Exception;

    /**
     * Transform a {@link SearchRequest}. Executed on the coordinator node before any {@link org.opensearch.action.search.SearchPhase}
     * executes.
     * <p>
     * Expert method: Implement this if the processor needs to make asynchronous calls. Otherwise, implement processRequest.
     * @param request the executed {@link SearchRequest}
     * @param requestListener callback to be invoked on successful processing or on failure
     */
    default void asyncProcessRequest(SearchRequest request, ActionListener<SearchRequest> requestListener) {
        try {
            requestListener.onResponse(processRequest(request));
        } catch (Exception e) {
            requestListener.onFailure(e);
        }
    }
}
