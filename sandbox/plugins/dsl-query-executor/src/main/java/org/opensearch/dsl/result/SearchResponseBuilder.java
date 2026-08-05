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
import org.opensearch.dsl.aggregation.pipeline.PipelineRegistry;
import org.opensearch.dsl.aggregation.pipeline.PipelineTranslator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;

import java.util.ArrayList;
import java.util.Iterator;
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
     * @param pipelineRegistry pipeline translator registry for building pipeline results
     * @param tookInMillis total execution time in milliseconds
     * @return a SearchResponse
     */
    public static SearchResponse build(
        List<ExecutionResult> results,
        SearchRequest request,
        AggregationRegistry registry,
        PipelineRegistry pipelineRegistry,
        long tookInMillis
    ) throws ConversionException {

        SearchHits hits = buildHits(results);
        InternalAggregations aggregations = buildAggregations(results, request, registry);
        aggregations = appendPipelineResults(aggregations, results, request, pipelineRegistry);

        SearchResponseSections sections = new SearchResponseSections(hits, aggregations, null, false, null, null, 0);

        // TODO: shard counts, timed_out, and engine-side took require execution metadata from
        // the analytics plugin (returned alongside rows). Until then report a constant 1/1 —
        // the analytics path has no per-shard fan-out to report.
        return new SearchResponse(sections, null, 1, 1, 0, tookInMillis, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private static SearchHits buildHits(List<ExecutionResult> results) {
        // TODO: Build hits from HITS results
        return SearchHits.empty(true);
    }

    private static InternalAggregations buildAggregations(
        List<ExecutionResult> results,
        SearchRequest request,
        AggregationRegistry registry
    ) throws ConversionException {

        List<ExecutionResult> aggResults = results.stream()
            .filter(r -> r.getType() == QueryPlans.Type.AGGREGATION)
            .collect(Collectors.toList());

        if (aggResults.isEmpty() || request.source() == null || request.source().aggregations() == null) {
            return null;
        }

        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, aggResults);
        return builder.build(new ArrayList<>(request.source().aggregations().getAggregatorFactories()));
    }

    /**
     * Appends sibling pipeline results to the aggregations. Each PIPELINE result carries one
     * row with one column per pipeline aggregation; columns map back to their builders by
     * name. An absent row or SQL NULL cell (empty sibling) maps to the translator's empty
     * representation. Mirrors vanilla's reduce ordering: pipeline results follow regular
     * aggregations.
     */
    private static InternalAggregations appendPipelineResults(
        InternalAggregations aggregations,
        List<ExecutionResult> results,
        SearchRequest request,
        PipelineRegistry pipelineRegistry
    ) throws ConversionException {

        List<ExecutionResult> pipelineResults = results.stream()
            .filter(r -> r.getType() == QueryPlans.Type.PIPELINE)
            .collect(Collectors.toList());

        if (pipelineResults.isEmpty() || request.source() == null || request.source().aggregations() == null) {
            return aggregations;
        }

        List<InternalAggregation> combined = new ArrayList<>();
        if (aggregations != null) {
            combined.addAll(aggregations.copyResults());
        }
        for (ExecutionResult result : pipelineResults) {
            appendPipelineResult(combined, result, request, pipelineRegistry);
        }
        return InternalAggregations.from(combined);
    }

    private static void appendPipelineResult(
        List<InternalAggregation> combined,
        ExecutionResult result,
        SearchRequest request,
        PipelineRegistry pipelineRegistry
    ) throws ConversionException {
        Iterator<Object[]> rows = result.getRows().iterator();
        Object[] row = rows.hasNext() ? rows.next() : null;
        List<String> fieldNames = result.getFieldNames();
        for (int i = 0; i < fieldNames.size(); i++) {
            PipelineAggregationBuilder pipeline = findPipelineBuilder(fieldNames.get(i), request);
            PipelineTranslator<PipelineAggregationBuilder> translator = pipelineRegistry.get(pipeline.getClass());
            if (translator == null) {
                throw new ConversionException(
                    "No pipeline translator registered for [" + pipeline.getName() + "] of type [" + pipeline.getWriteableName() + "]"
                );
            }
            Object cell = row == null ? null : row[i];
            combined.add(translator.toInternalAggregation(pipeline, cell));
        }
    }

    private static PipelineAggregationBuilder findPipelineBuilder(String name, SearchRequest request) throws ConversionException {
        for (PipelineAggregationBuilder pipeline : request.source().aggregations().getPipelineAggregatorFactories()) {
            if (pipeline.getName().equals(name)) {
                return pipeline;
            }
        }
        throw new ConversionException("Pipeline result column [" + name + "] has no matching pipeline aggregation in the request");
    }
}
