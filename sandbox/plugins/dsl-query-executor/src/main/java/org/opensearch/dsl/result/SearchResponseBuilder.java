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
     * @param convertTimeNanos time spent in DSL-to-RelNode conversion, in nanoseconds
     * @return a SearchResponse
     */
    // TODO: Analytics plugin should return execution metadata alongside Iterable<Object[]> rows:
    // - executionTimeNanos: query execution time
    // - totalDocCount: total matching documents for hits.total
    // - terminatedEarly: whether execution was terminated early
    // - timedOut: whether execution timed out
    // - shardInfo: total/successful/skipped/failed shard counts
    public static SearchResponse build(List<ExecutionResult> results, long convertTimeNanos) {
        // TODO: populate HITS and AGGREGATION plan types from results
        long tookInMillis = convertTimeNanos / 1_000_000;
        SearchHits hits = SearchHits.empty(true);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, null, null, 0);
        return new SearchResponse(sections, null, 0, 0, 0, tookInMillis, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
