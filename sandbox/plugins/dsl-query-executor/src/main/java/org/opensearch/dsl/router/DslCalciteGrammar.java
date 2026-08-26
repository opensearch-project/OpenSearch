/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.router;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.QueryTranslator;
import org.opensearch.dsl.query.ValidationResult;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Decides whether a {@link SearchSourceBuilder} can be handled by the Calcite path.
 *
 * <p>Strategy: registry-lookup is the safe list. Any query/aggregation whose class has a
 * translator registered is accepted structurally; per-parameter restrictions are layered on
 * top only where the translator has known request-shape limitations. The {@code bool} compound
 * query is transparent — recursed into, never looked up.
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
 *   <li>Top-level fields are reject-unless-supported: a field is supported only if we have a
 *       handler for it. For now only {@code query} and {@code aggregations} have handlers, so a
 *       request that sets any other top-level field ({@code size}, {@code sort}, {@code _source},
 *       {@code highlight}, {@code post_filter}, …) is rejected and falls back to codec. Handlers
 *       for the remaining supported fields are added incrementally.</li>
 * </ul>
 */
public class DslCalciteGrammar {

    /**
     * Compound (container) queries are transparent: structurally supported, but only if every
     * child query is supported. Each entry declares how to extract that type's child clauses,
     * so recursion follows the query tree rather than a hardcoded switch — adding a container
     * type is a single entry here, and validation of its children comes for free.
     *
     * <p>Consulted before the leaf/registry path in {@link #visitQuery}, so a compound query
     * can never be mistaken for a leaf and have its children skipped.
     *
     * <p>Only {@code bool} is recursed today. Other compound queries ({@code constant_score},
     * {@code dis_max}, {@code function_score}, {@code boosting}, {@code hybrid}) carry scoring
     * semantics the DataFusion path does not support, so they have no entry here and fall back to
     * codec.
     */
    private static final Map<Class<? extends QueryBuilder>, Function<QueryBuilder, Stream<QueryBuilder>>> COMPOUND_CHILDREN = Map.of(
        BoolQueryBuilder.class,
        q -> {
            BoolQueryBuilder b = (BoolQueryBuilder) q;
            return Stream.of(b.must(), b.filter(), b.should(), b.mustNot()).flatMap(List::stream);
        }
    );

    /**
     * Validates the contents of one top-level field. Returns {@code true} if supported; otherwise
     * adds a reason code to {@code issues} and returns {@code false}.
     */
    @FunctionalInterface
    private interface TopLevelFieldHandler {
        boolean isSupported(SearchSourceBuilder source, List<String> issues);
    }

    /**
     * Handler for fields the converter supports in full — every value is honored, so there is
     * nothing to validate (e.g. {@code size}, {@code _source}).
     */
    private static final TopLevelFieldHandler ALWAYS_SUPPORTED = (source, issues) -> true;

    /**
     * Handlers for the top-level {@link SearchSourceBuilder} fields we support, keyed by field
     * name. A field with no handler here is rejected (routed to codec) — reject-unless-supported;
     * a field with a handler is accepted only if the handler validates its contents. Contents are
     * validated for {@code query} and {@code aggregations}; {@code size}, {@code from},
     * {@code _source} and {@code track_total_hits} are supported in full. {@code sort} has no
     * handler yet, so it still routes to codec until {@code visitSort} lands. Populated in the
     * constructor because the query/aggregation handlers call instance methods.
     */
    private final Map<String, TopLevelFieldHandler> topLevelHandlers;

    private final QueryRegistry queryRegistry;
    private final AggregationRegistry aggRegistry;

    /**
     * @param queryRegistry the registry consulted for query-leaf safe list
     * @param aggRegistry the registry consulted for aggregation safe list
     */
    public DslCalciteGrammar(QueryRegistry queryRegistry, AggregationRegistry aggRegistry) {
        this.queryRegistry = queryRegistry;
        this.aggRegistry = aggRegistry;

        Map<String, TopLevelFieldHandler> handlers = new HashMap<>();
        handlers.put(SearchSourceBuilder.QUERY_FIELD.getPreferredName(), (source, issues) -> visitQuery(source.query(), issues));
        handlers.put(
            SearchSourceBuilder.AGGREGATIONS_FIELD.getPreferredName(),
            (source, issues) -> visitAggregationTree(source.aggregations(), issues)
        );
        handlers.put(SearchSourceBuilder.SIZE_FIELD.getPreferredName(), ALWAYS_SUPPORTED);
        handlers.put(SearchSourceBuilder.FROM_FIELD.getPreferredName(), ALWAYS_SUPPORTED);
        handlers.put(SearchSourceBuilder._SOURCE_FIELD.getPreferredName(), ALWAYS_SUPPORTED);
        handlers.put(SearchSourceBuilder.TRACK_TOTAL_HITS_FIELD.getPreferredName(), ALWAYS_SUPPORTED);
        this.topLevelHandlers = Map.copyOf(handlers);
    }

    /**
     * Validates a search source, returning a routing decision. Short-circuits at the first
     * failing section: top-level issues skip the query walk, query issues skip the aggregation
     * walk.
     *
     * @param source the request body; a {@code null} source (bodyless {@code _search}) is
     *        accepted as an implicit match_all — {@link org.opensearch.dsl.converter.SearchSourceConverter}
     *        normalizes it to an empty source
     */
    public RouteDecision validate(SearchSourceBuilder source) {
        if (source == null) {
            return RouteDecision.accepted();
        }

        List<String> issues = new ArrayList<>();
        for (String field : topLevelFields(source)) {
            TopLevelFieldHandler handler = topLevelHandlers.get(field);
            if (handler == null) {
                return RouteDecision.rejected(List.of("source." + field));
            }
            if (handler.isSupported(source, issues) == false) {
                return RouteDecision.rejected(issues);
            }
        }

        // Reached when every field the request set had a passing handler, or the request set no
        // fields at all (empty {} — the loop never ran). Both accept. (A null source is handled
        // by the early return above.)
        return RouteDecision.accepted();
    }

    /**
     * The top-level field names the request actually set. There is no getter that enumerates set
     * fields, so the source is serialized ({@link SearchSourceBuilder#toXContent} emits only set
     * fields) and its JSON keys are read — this sees every field, including ones added to
     * OpenSearch later. Ordered, so a query failure is reported before an aggregation failure.
     */
    private static Set<String> topLevelFields(SearchSourceBuilder source) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            source.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), true).v2().keySet();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read search source top-level fields", e);
        }
    }

    /**
     * Visits a single query node. Compound queries (see {@link #COMPOUND_CHILDREN}) are
     * transparent — recursed into and supported only if every child is supported. Every other
     * query type is treated as a leaf and resolved against the translator registry. Driving the
     * recursion off {@code COMPOUND_CHILDREN} rather than a hardcoded switch means a nested
     * unsupported leaf inside any container type is always found, never silently accepted.
     */
    private boolean visitQuery(QueryBuilder q, List<String> issues) {
        Function<QueryBuilder, Stream<QueryBuilder>> childrenOf = COMPOUND_CHILDREN.get(q.getClass());
        if (childrenOf != null) {
            return childrenOf.apply(q).filter(Objects::nonNull).allMatch(child -> visitQuery(child, issues));
        }
        return visitLeaf(q, issues);
    }

    /**
     * Resolves a leaf (non-compound) query against the registry: rejects with a
     * {@code query:<name>} reason when no translator is registered, otherwise delegates
     * request-shape validation to that translator so routing and conversion share one source
     * of truth.
     */
    private boolean visitLeaf(QueryBuilder q, List<String> issues) {
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
