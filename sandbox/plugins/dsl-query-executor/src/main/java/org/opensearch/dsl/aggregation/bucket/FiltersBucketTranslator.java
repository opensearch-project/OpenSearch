/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.aggregation.EmptyGrouping;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.query.QueryRegistry;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Translates a {@link FiltersAggregationBuilder} — a multi-bucket aggregation where each
 * bucket is associated with its own filter query. Unlike the singular {@link FilterBucketTranslator},
 * this translator exposes the list of keyed filters so the tree walker can iterate and produce
 * one plan per filter, plus an optional "other" bucket for documents not matching any filter.
 *
 * <p>Example: {@code {"aggs": {"messages": {"filters": {"filters": {"errors": {"match": {"body": "error"}},
 * "warnings": {"match": {"body": "warning"}}}}, "aggs": {"avg_bytes": {"avg": {"field": "bytes"}}}}}}}
 * produces one plan per filter, each with its own {@code LogicalFilter} condition.
 */
public class FiltersBucketTranslator implements BucketTranslator<FiltersAggregationBuilder> {

    private final QueryRegistry queryRegistry;

    /**
     * Creates a filters bucket translator.
     *
     * @param queryRegistry the registry for converting query clauses to RexNode expressions
     */
    public FiltersBucketTranslator(QueryRegistry queryRegistry) {
        this.queryRegistry = queryRegistry;
    }

    @Override
    public Class<FiltersAggregationBuilder> getAggregationType() {
        return FiltersAggregationBuilder.class;
    }

    @Override
    public WalkStrategy getWalkStrategy() {
        return WalkStrategy.MULTI_FILTER;
    }

    @Override
    public GroupingInfo getGrouping(FiltersAggregationBuilder agg) {
        return new EmptyGrouping();
    }

    @Override
    public Collection<AggregationBuilder> getSubAggregations(FiltersAggregationBuilder agg) {
        return agg.getSubAggregations();
    }

    /**
     * Returns the list of {@link FiltersAggregator.KeyedFilter} entries from the aggregation.
     * Each entry has a {@code key()} and a {@code filter()} {@link QueryBuilder}.
     *
     * @param agg the filters aggregation builder
     * @return the list of keyed filters
     * @throws ConversionException if the filters list is null or empty
     */
    public List<FiltersAggregator.KeyedFilter> getKeyedFilters(FiltersAggregationBuilder agg) throws ConversionException {
        List<FiltersAggregator.KeyedFilter> filters = agg.filters();
        if (filters == null || filters.isEmpty()) {
            throw new ConversionException("Filters aggregation '" + agg.getName() + "' requires at least one filter");
        }
        return filters;
    }

    /**
     * Converts a single filter's {@link QueryBuilder} to a {@link RexNode} using the {@link QueryRegistry}.
     *
     * @param filterQuery the filter query builder to convert
     * @param ctx the conversion context
     * @return the RexNode representing the filter condition
     * @throws ConversionException if conversion fails
     */
    public RexNode convertFilter(QueryBuilder filterQuery, ConversionContext ctx) throws ConversionException {
        return queryRegistry.convert(filterQuery, ctx);
    }

    /**
     * Builds the other bucket condition: {@code NOT(filter1 OR filter2 OR ... OR filterN)}.
     * Combines all filter RexNodes with OR, then wraps in NOT.
     *
     * @param filterConditions the list of filter RexNodes (must not be empty)
     * @param ctx the conversion context providing the RexBuilder
     * @return the negated disjunction RexNode
     */
    public RexNode buildOtherBucketCondition(List<RexNode> filterConditions, ConversionContext ctx) throws ConversionException {
        if (filterConditions.isEmpty()) {
            throw new ConversionException("Filter list is empty");
        }
        RexNode disjunction = filterConditions.getFirst();
        for (int i = 1; i < filterConditions.size(); i++) {
            disjunction = ctx.getRexBuilder().makeCall(SqlStdOperatorTable.OR, disjunction, filterConditions.get(i));
        }
        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.NOT, disjunction);
    }

    /**
     * Combines a filter condition with an optional parent filter condition using AND.
     * If the parent condition is null, returns the filter condition as-is.
     *
     * @param parentCondition the parent filter condition, or null if no parent filter
     * @param filterCondition the filter condition to combine
     * @param ctx the conversion context providing the RexBuilder
     * @return the combined condition
     */
    public RexNode combineWithParent(RexNode parentCondition, RexNode filterCondition, ConversionContext ctx) {
        if (parentCondition == null) {
            return filterCondition;
        }
        return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, parentCondition, filterCondition);
    }

    // TODO: Handle query metadata to tag aggregation results, currently its not getting processed in
    //  AggregationTreeWalker
    @Override
    public InternalAggregation toBucketAggregation(FiltersAggregationBuilder agg, List<BucketEntry> buckets) {
        boolean keyed = agg.isKeyed();
        List<InternalFilters.InternalBucket> internalBuckets = new ArrayList<>(buckets.size());
        for (BucketEntry entry : buckets) {
            String key = entry.keys().isEmpty() ? null : entry.keys().getFirst().toString();
            internalBuckets.add(new InternalFilters.InternalBucket(key, entry.docCount(), entry.subAggs(), keyed));
        }
        return new InternalFilters(agg.getName(), internalBuckets, keyed, Map.of());
    }

    /**
     * Describes a single filter plan: a bucket key paired with its filter condition.
     *
     * @param bucketKey the bucket key for response assembly
     * @param condition the RexNode filter condition
     */
    public record FilterPlanDescriptor(String bucketKey, RexNode condition) {}

    /**
     * Resolves all filter plans for this aggregation, including the other bucket if enabled.
     * The walker iterates over the returned list.
     *
     * @param agg the filters aggregation builder
     * @param parentCondition the parent filter condition, or null if no parent filter
     * @param ctx the conversion context
     * @return list of filter plan descriptors (N for filters, plus 1 for other bucket if enabled)
     * @throws ConversionException if filter conversion fails
     */
    public List<FilterPlanDescriptor> resolveFilterPlans(
        FiltersAggregationBuilder agg,
        RexNode parentCondition,
        ConversionContext ctx
    ) throws ConversionException {
        List<FiltersAggregator.KeyedFilter> keyedFilters = getKeyedFilters(agg);
        List<RexNode> filterConditions = new ArrayList<>();
        for (FiltersAggregator.KeyedFilter kf : keyedFilters) {
            filterConditions.add(convertFilter(kf.filter(), ctx));
        }

        List<FilterPlanDescriptor> plans = new ArrayList<>();

        // One plan per keyed filter
        for (int i = 0; i < keyedFilters.size(); i++) {
            RexNode combined = combineWithParent(parentCondition, filterConditions.get(i), ctx);
            plans.add(new FilterPlanDescriptor(keyedFilters.get(i).key(), combined));
        }

        // Other bucket: NOT(OR(all filters))
        if (agg.otherBucket()) {
            RexNode otherCondition = buildOtherBucketCondition(filterConditions, ctx);
            RexNode combined = combineWithParent(parentCondition, otherCondition, ctx);
            plans.add(new FilterPlanDescriptor(agg.otherBucketKey(), combined));
        }

        return plans;
    }
}
