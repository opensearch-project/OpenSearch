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
 * A specialization of {@link SearchResponseProcessor} that makes use of the request-scoped processor state.
 * Implementors must implement the processResponse method that accepts request-scoped processor state.
 */
public interface StatefulSearchResponseProcessor extends SearchResponseProcessor {
    @Override
    default SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        throw new UnsupportedOperationException();
    }

    @Override
    SearchResponse processResponse(SearchRequest request, SearchResponse response, PipelineProcessingContext requestContext)
        throws Exception;
}
