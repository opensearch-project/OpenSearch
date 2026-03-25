/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.SearchHits;

import java.util.List;

/**
 * Builds a {@link SearchResponse} from execution results.
 */
public class SearchResponseBuilder {

    private SearchResponseBuilder() {}

    /**
     * Builds a SearchResponse from the given results and timing.
     *
     * @param results execution results from the plan executor
     * @param tookInMillis total execution time in milliseconds
     * @return a SearchResponse
     */
    public static SearchResponse build(List<ExecutionResult> results, long tookInMillis) {
        // TODO: populate hits and aggregations from results
        SearchHits hits = SearchHits.empty(true);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, null, null, 0);
        return new SearchResponse(
            sections,
            null,
            0,
            0,
            0,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY);
    }
}
