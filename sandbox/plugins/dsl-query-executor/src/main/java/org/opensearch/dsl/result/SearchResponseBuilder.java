/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds a {@link SearchResponse} from execution results.
 * Handles conversion of flat execution results into OpenSearch response format.
 */
public class SearchResponseBuilder {

    private SearchResponseBuilder() {}

    /**
     * Builds a SearchResponse from execution results.
     *
     * @param results execution results from the query executor
     * @param request the original search request
     * @param registry aggregation registry for building aggregations
     * @param tookInMillis total execution time in milliseconds
     * @return a SearchResponse
     */
    public static SearchResponse build(
            List<ExecutionResult> results,
            SearchRequest request,
            AggregationRegistry registry,
            long tookInMillis) throws ConversionException {

        SearchHits hits = buildHits(results);
        InternalAggregations aggregations = buildAggregations(results, request, registry);

        SearchResponseSections sections = new SearchResponseSections(
            hits,
            aggregations,
            null,
            false,
            null,
            null,
            0
        );

        return new SearchResponse(
            sections,
            null,
            aggregations != null ? 1 : 0,
            aggregations != null ? 1 : 0,
            0,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    private static SearchHits buildHits(List<ExecutionResult> results) {
        // TODO: Build hits from HITS results
        return SearchHits.empty(true);
    }

    private static InternalAggregations buildAggregations(
            List<ExecutionResult> results,
            SearchRequest request,
            AggregationRegistry registry) throws ConversionException {

        List<ExecutionResult> aggResults = results.stream()
            .filter(r -> r.getType() == QueryPlans.Type.AGGREGATION)
            .collect(Collectors.toList());

        if (aggResults.isEmpty() || request.source() == null || request.source().aggregations() == null) {
            return null;
        }

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, aggResults);
        return builder.build(new ArrayList<>(request.source().aggregations().getAggregatorFactories()));
    }
}
