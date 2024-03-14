/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchRequest;

/**
 * A specialization of {@link SearchRequestProcessor} that makes use of the request-scoped processor state.
 * Implementors must implement the processRequest method that accepts request-scoped processor state.
 */
public interface StatefulSearchRequestProcessor extends SearchRequestProcessor {
    @Override
    default SearchRequest processRequest(SearchRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    SearchRequest processRequest(SearchRequest request, PipelineProcessingContext requestContext) throws Exception;
}
