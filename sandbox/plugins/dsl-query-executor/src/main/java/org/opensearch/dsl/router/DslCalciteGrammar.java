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
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.dsl.aggregation.AggregationRegistry;
import org.opensearch.dsl.aggregation.AggregationTranslator;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.query.ValidationResult;
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
import java.util.Set;

/**
 * Decides whether a {@link SearchSourceBuilder} can run on the Calcite path.
 *
 * <p>Reject-unless-supported: a request is accepted only if every top-level field has a handler and
 * every query and aggregation type has a translator that accepts its parameters — otherwise it
 * routes to codec. Parameter validation is delegated to the translators, so routing and conversion
 * agree. Query-tree recursion is driven by {@link QueryBuilder#visit}, so a nested query in any
 * container type is always reached without a hand-maintained list; nested aggregation trees are
 * blanket-rejected for now, pending performance validation.
 *
 * <p>Reject reasons are short codes (e.g. {@code "query:function_score"}, {@code "terms.min_doc_count"})
 * for observability without leaking user data.
 */
public class DslCalciteGrammar {

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
     * Supported top-level fields keyed by name: a field with no handler is rejected (routed to
     * codec), one with a handler is accepted only if the handler validates its contents. Populated
     * in the constructor because the query/aggregation handlers call instance methods.
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
     * Validates a search source, returning a routing decision; short-circuits at the first failing
     * field.
     *
     * @param source the request body; a {@code null} source (bodyless {@code _search}) is accepted
     *        as an implicit match_all — {@link org.opensearch.dsl.converter.SearchSourceConverter}
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

        // Reached when every field the request set had a passing handler, or it set no fields at all
        // (empty {} — the loop never ran). Both accept; a null source already returned early above.
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
            return XContentHelper.convertToMap(BytesReference.bytes(builder), true, MediaTypeRegistry.JSON).v2().keySet();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read search source top-level fields", e);
        }
    }

    /**
     * Validates the query tree via {@link QueryBuilder#visit}, which reaches every node;
     * {@link ValidatingQueryVisitor} checks each one (translator lookup + parameter validation).
     */
    private boolean visitQuery(QueryBuilder root, List<String> issues) {
        ValidatingQueryVisitor visitor = new ValidatingQueryVisitor(queryRegistry, issues);
        root.visit(visitor);
        return visitor.failed() == false;
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
     * Pipeline aggregations have no Calcite equivalent — any presence is a hard reject. Called at
     * each level, since pipelines can be siblings of regular aggregations.
     *
     * <p>TODO: replace the blanket reject with a registry + per-type validation once pipeline aggs
     * are supported.
     */
    private boolean visitPipelineAggregations(Collection<PipelineAggregationBuilder> pipelines, List<String> issues) {
        if (pipelines == null || pipelines.isEmpty()) {
            return true;
        }

        return reject("pipeline_agg:" + pipelines.iterator().next().getName(), issues);
    }

    @SuppressWarnings("unchecked")
    private boolean visitAggregation(AggregationBuilder agg, List<String> issues) {
        AggregationTranslator<?> translator = aggRegistry.get(agg.getClass());
        if (translator == null) {
            return reject("agg:" + agg.getType(), issues);
        }

        if (agg.getSubAggregations() != null && agg.getSubAggregations().isEmpty() == false) {
            return reject("agg.nested", issues);
        }

        // Delegate to the same validate() the converter uses, so routing and conversion agree.
        // Schema-dependent checks (e.g. terms on a date field) need a MapperService the routing
        // layer lacks, so they no-op here and stay enforced in convert().
        ValidationResult validationResult = ((AggregationTranslator<AggregationBuilder>) translator).validate(agg);
        if (validationResult.isAccepted() == false) {
            return reject(validationResult.reasonCode(), issues);
        }

        return visitPipelineAggregations(agg.getPipelineAggregations(), issues);
    }

    private static boolean reject(String reason, List<String> issues) {
        issues.add(reason);
        return false;
    }
}
