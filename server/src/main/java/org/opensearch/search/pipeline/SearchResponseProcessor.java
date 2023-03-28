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
 * Interface for a search pipeline processor that modifies a search response.
 */
public interface SearchResponseProcessor extends Processor {
    SearchResponse processResponse(SearchRequest request, SearchResponse response) throws Exception;
}
