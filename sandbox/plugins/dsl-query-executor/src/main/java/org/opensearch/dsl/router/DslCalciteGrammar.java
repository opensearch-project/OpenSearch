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
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
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
 *   <li>Query walker with per-parameter checks mirroring each registered query translator.</li>
 *   <li>Aggregation walker with registry check + pipeline-agg rejection; per-parameter checks
 *       per aggregation type are TODO and tracked inside {@code visitAggregation}.</li>
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

        if (!queryRegistry.hasTranslator(q.getClass())) {
            return reject("query:" + q.getName(), issues);
        }

        // Per-parameter restrictions for registered types. Types not listed have
        // translators that accept anything the class carries — the default arm passes
        // them through structurally. Examples:
        //   - TermQueryTranslator: reads only fieldName/value, raises no rejects.
        //   - MatchAllQueryTranslator: reads nothing, always returns TRUE. Note that a
        //     null source.query() is treated by the converter as an implicit match_all
        //     (no filter) and is also accepted — but a null source itself is rejected
        //     up front (see validate()).
        return switch (q) {
            case RangeQueryBuilder r -> visitRangeQuery(r, issues);
            case TermsQueryBuilder t -> visitTermsQuery(t, issues);
            case ExistsQueryBuilder e -> visitExistsQuery(e, issues);
            case PrefixQueryBuilder p -> visitPrefixQuery(p, issues);
            case WildcardQueryBuilder w -> visitWildcardQuery(w, issues);
            default -> true;
        };
    }

    /**
     * Per-parameter checks for {@code wildcard} query, mirroring
     * {@code WildcardQueryTranslator}'s rejects. Same shape as {@code prefix}:
     * {@code case_insensitive} is consumed, {@code boost}/{@code rewrite} rejected.
     */
    private boolean visitWildcardQuery(WildcardQueryBuilder w, List<String> issues) {
        if (w.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            return reject(WildcardQueryBuilder.NAME + ".boost", issues);
        }
        if (w.rewrite() != null) {
            return reject(WildcardQueryBuilder.NAME + ".rewrite", issues);
        }
        return true;
    }

    /**
     * Per-parameter checks for {@code prefix} query, mirroring {@code PrefixQueryTranslator}'s
     * rejects. {@code case_insensitive} is consumed by the translator (folds to LOWER) and
     * intentionally not rejected here.
     */
    private boolean visitPrefixQuery(PrefixQueryBuilder p, List<String> issues) {
        if (p.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            return reject(PrefixQueryBuilder.NAME + ".boost", issues);
        }
        if (p.rewrite() != null) {
            return reject(PrefixQueryBuilder.NAME + ".rewrite", issues);
        }
        return true;
    }

    /**
     * Per-parameter checks for {@code exists} query, mirroring {@code ExistsQueryTranslator}'s
     * rejects. Only {@code boost} is checked — the translator silently ignores other params
     * (including {@code _name}), matching the translator literally.
     */
    private boolean visitExistsQuery(ExistsQueryBuilder e, List<String> issues) {
        if (e.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            return reject(ExistsQueryBuilder.NAME + ".boost", issues);
        }
        return true;
    }

    /**
     * Recurses into every clause of a {@code bool} query. {@code allMatch} short-circuits on
     * the first failing child so the reject reason reflects the exact node that broke.
     */
    private boolean visitBool(BoolQueryBuilder b, List<String> issues) {
        return Stream.of(b.must(), b.filter(), b.should(), b.mustNot())
            .flatMap(List::stream)
            .allMatch(inner -> visitQuery(inner, issues));
    }

    /**
     * Per-parameter checks for {@code terms} query, mirroring {@code TermsQueryTranslator}'s
     * rejects. Field-existence and value-type-compatibility stay with the translator.
     */
    private boolean visitTermsQuery(TermsQueryBuilder t, List<String> issues) {
        if (t.termsLookup() != null) {
            return reject(TermsQueryBuilder.NAME + ".terms_lookup", issues);
        }
        if (t.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            return reject(TermsQueryBuilder.NAME + ".boost", issues);
        }
        if (t.queryName() != null) {
            return reject(TermsQueryBuilder.NAME + ".name", issues);
        }
        if (t.valueType() != TermsQueryBuilder.ValueType.DEFAULT) {
            return reject(TermsQueryBuilder.NAME + ".value_type:" + t.valueType(), issues);
        }
        if (t.values() == null || t.values().isEmpty()) {
            return reject(TermsQueryBuilder.NAME + ".no_values", issues);
        }
        return true;
    }

    /**
     * Per-parameter checks for {@code range} query, mirroring {@code RangeQueryTranslator}'s
     * rejects. Field-existence, binary-field guards, and date_nanos-precision checks stay
     * with the translator (schema context is not available here).
     *
     * <p>The translator also rejects {@code relation=DISJOINT}, but that path is
     * unreachable: {@link RangeQueryBuilder#relation(String)} rejects {@code DISJOINT} at
     * construction time, so no such request can ever reach the grammar.
     */
    private boolean visitRangeQuery(RangeQueryBuilder r, List<String> issues) {
        if (r.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            return reject(RangeQueryBuilder.NAME + ".boost", issues);
        }
        if (r.queryName() != null) {
            return reject(RangeQueryBuilder.NAME + ".name", issues);
        }
        if (r.from() == null && r.to() == null) {
            return reject(RangeQueryBuilder.NAME + ".no_bounds", issues);
        }
        return true;
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

        // TODO: per-parameter checks per aggregation type. To be filled in as each
        // aggregation translator (avg/sum/min/max/value_count/stats/extended_stats/terms/...)
        // is reviewed for the exact params it consumes vs silently ignores. Same pattern as
        // the query-side switch above — mirror the translator's rejects here to route to
        // codec early instead of failing at conversion time.

        return visitPipelineAggregations(agg.getPipelineAggregations(), issues)
            && visitAggregations(agg.getSubAggregations(), issues);
    }

    private static boolean reject(String reason, List<String> issues) {
        issues.add(reason);
        return false;
    }
}
