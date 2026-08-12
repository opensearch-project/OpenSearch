/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.router;

import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryTranslator;
import org.opensearch.dsl.query.ValidationResult;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

/**
 * Decides whether a {@link SearchSourceBuilder} can be handled by the Calcite path.
 *
 * <p>Strategy: registry-lookup is the safe list. Any query/aggregation whose class has a
 * translator registered is accepted structurally; per-parameter restrictions are layered on
 * top only where the translator has known request-shape limitations. Compound queries
 * ({@code bool}, {@code constant_score}) are transparent — recursed into, never looked up.
 *
 * <p>Reject reasons are returned as short reason codes (e.g. {@code "query:function_score"},
 * {@code "range.relation:DISJOINT"}, {@code "pipeline_agg:cumulative_sum"}) for observability
 * without leaking user data.
 *
 * <p>v1 scope:
 * <ul>
 *   <li>Query walker with translator-backed leaf validation.</li>
 *   <li>Aggregation walker with registry check + pipeline-agg rejection. Nested aggregation
 *       trees are blanket-rejected for now and stay on the codec path until their Calcite /
 *       DataFusion performance is validated. Per-parameter checks per aggregation type are
 *       TODO and tracked inside {@code visitAggregation}.</li>
 *   <li>Top-level checks (size, sort, highlight, post_filter, etc.) are TODO — the response
 *       builder's hits are still stubbed, so top-level gating will be added when
 *       {@code buildHits} lands.</li>
 * </ul>
 */
public class DslCalciteGrammar {

    private final QueryRegistry queryRegistry;
    private final AggregationRegistry aggRegistry;

    /**
     * @param queryRegistry the registry consulted for query-leaf safe list
     * @param aggRegistry the registry consulted for aggregation safe list
     */
    public DslCalciteGrammar(QueryRegistry queryRegistry, AggregationRegistry aggRegistry) {
        this.queryRegistry = queryRegistry;
        this.aggRegistry = aggRegistry;
    }

    /**
     * Validates a search source, returning a routing decision. Short-circuits at the first
     * failing section: top-level issues skip the query walk, query issues skip the aggregation
     * walk.
     *
     * @param source the request body; a {@code null} source is rejected up front
     *        ({@link org.opensearch.dsl.converter.SearchSourceConverter} would NPE)
     */
    public RouteDecision validate(SearchSourceBuilder source) {
        if (source == null) {
            // SearchSourceConverter dereferences source.size()/source.aggregations() with
            // no null guard — a null source would NPE the Calcite path. Semantically the
            // request is a match_all, but that has to be expressed with an actual body.
            return RouteDecision.rejected(List.of("source:null"));
        }

        List<String> issues = new ArrayList<>();

        if (source.query() != null && !visitQuery(source.query(), issues)) {
            return RouteDecision.rejected(issues);
        }

        if (source.aggregations() != null && !visitAggregationTree(source.aggregations(), issues)) {
            return RouteDecision.rejected(issues);
        }

        return RouteDecision.accepted();
    }

    private boolean visitQuery(QueryBuilder q, List<String> issues) {
        // Compound queries are transparent to the registry — recurse into children.
        switch (q) {
            case BoolQueryBuilder b -> {
                return visitBool(b, issues);
            }
            case ConstantScoreQueryBuilder csq -> {
                return visitQuery(csq.innerQuery(), issues);
            }
            default -> {
            }
        }

        QueryTranslator translator = queryRegistry.get(q.getClass());
        if (translator == null) {
            return reject("query:" + q.getName(), issues);
        }

        ValidationResult validationResult = translator.validate(q);
        if (!validationResult.isAccepted()) {
            return reject(validationResult.reasonCode(), issues);
        }

        return true;
    }

    /**
     * Recurses into every clause of a {@code bool} query. {@code allMatch} short-circuits on
     * the first failing child so the reject reason reflects the exact node that broke.
     */
    private boolean visitBool(BoolQueryBuilder b, List<String> issues) {
        return Stream.of(b.must(), b.filter(), b.should(), b.mustNot()).flatMap(List::stream).allMatch(inner -> visitQuery(inner, issues));
    }

    /**
     * Entry point for the aggregation section: rejects top-level pipeline aggregations, then
     * walks each top-level regular aggregation.
     */
    private boolean visitAggregationTree(AggregatorFactories.Builder aggs, List<String> issues) {
        return visitPipelineAggregations(aggs.getPipelineAggregatorFactories(), issues)
            && visitAggregations(aggs.getAggregatorFactories(), issues);
    }

    /** Collection-level driver: short-circuits at the first failing aggregation. */
    private boolean visitAggregations(Collection<AggregationBuilder> aggs, List<String> issues) {
        return aggs == null || aggs.stream().allMatch(agg -> visitAggregation(agg, issues));
    }

    /**
     * Pipeline aggregations have no Calcite equivalent — any presence is a hard reject.
     * Called from both the top-level tree walk and the per-node recursion, since pipelines
     * can appear as siblings of any level's regular aggregations.
     *
     * <p>TODO: when a {@code PipelineAggregationRegistry} (or equivalent) is introduced,
     * replace the blanket reject with a registry check + per-type per-parameter switch,
     * mirroring the query and normal-aggregation paths.
     */
    private boolean visitPipelineAggregations(Collection<PipelineAggregationBuilder> pipelines, List<String> issues) {
        if (pipelines == null || pipelines.isEmpty()) {
            return true;
        }

        return reject("pipeline_agg:" + pipelines.iterator().next().getName(), issues);
    }

    private boolean visitAggregation(AggregationBuilder agg, List<String> issues) {
        if (!aggRegistry.hasTranslator(agg.getClass())) {
            return reject("agg:" + agg.getType(), issues);
        }

        if (agg.getSubAggregations() != null && agg.getSubAggregations().isEmpty() == false) {
            return reject("agg.nested", issues);
        }

        // TODO: per-parameter checks per aggregation type. To be filled in as each
        // aggregation translator (avg/sum/min/max/value_count/stats/extended_stats/terms/...)
        // is reviewed for the exact params it consumes vs silently ignores. Same pattern as
        // the query-side switch above — mirror the translator's rejects here to route to
        // codec early instead of failing at conversion time.

        return visitPipelineAggregations(agg.getPipelineAggregations(), issues);
    }

    private static boolean reject(String reason, List<String> issues) {
        issues.add(reason);
        return false;
    }
}
