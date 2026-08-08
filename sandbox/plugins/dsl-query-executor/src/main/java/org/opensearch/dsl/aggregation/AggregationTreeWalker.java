/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.bucket.FilterBucketTranslator;
import org.opensearch.dsl.aggregation.bucket.FiltersBucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Recursively walks the DSL aggregation tree and produces one {@link AggregationMetadata}
 * per distinct granularity level.
 *
 * <p>A "granularity" is a unique GROUP BY key set determined by the accumulated bucket
 * nesting path. Metrics at different nesting depths produce separate metadata instances,
 * each yielding its own {@code LogicalAggregate} and {@code QueryPlan}.
 */
public class AggregationTreeWalker {

    private final AggregationRegistry registry;

    /**
     * Creates a tree walker.
     *
     * @param registry the aggregation registry for looking up translators
     */
    public AggregationTreeWalker(AggregationRegistry registry) {
        this.registry = registry;
    }

    /**
     * Walks the aggregation tree and returns one AggregationMetadata per granularity level.
     *
     * @param aggs the top-level aggregation builders
     * @return metadata list, one per granularity (only levels with metrics or implicit count)
     * @throws ConversionException if any aggregation fails to convert
     */
    public List<AggregationMetadata> walk(Collection<AggregationBuilder> aggs, ConversionContext ctx) throws ConversionException {
        RelDataType rowType = ctx.getRowType();
        RelDataTypeFactory typeFactory = ctx.getCluster().getTypeFactory();
        Map<String, AggregationMetadataBuilder> granularities = new LinkedHashMap<>();
        walkRecursive(aggs, new ArrayList<>(), null, null, granularities, ctx);

        List<AggregationMetadata> result = new ArrayList<>();
        for (AggregationMetadataBuilder builder : granularities.values()) {
            if (builder.hasAggregateCalls()) {
                result.add(builder.build(rowType, typeFactory));
            }
        }
        return result;
    }

    /**
     * Recursively processes a collection of sibling aggregation builders at the current nesting level.
     *
     * <p>Each aggregation is dispatched based on its translator type:
     * <ul>
     *   <li>{@link BucketTranslator} with {@code MULTI_FILTER} strategy — delegates to
     *       {@link #handleMultiFiltersBucket}, which creates one plan per keyed filter.</li>
     *   <li>{@link BucketTranslator} with {@code FILTER} or {@code STANDARD} strategy — delegates to
     *       {@link #handleBucket}, which accumulates groupings and filter conditions, then recurses.</li>
     *   <li>{@link MetricTranslator} — delegates to {@link #handleMetric}, which adds an aggregate
     *       call to the builder for the current granularity level.</li>
     * </ul>
     *
     * @param aggs the sibling aggregation builders at this nesting level
     * @param currentGroupings accumulated {@link GroupingInfo} from all parent bucket aggregations;
     *                         determines the GROUP BY columns for any metrics at this level
     * @param currentFilterCondition the combined {@link RexNode} filter condition inherited from
     *                               parent filter bucket aggregations, or {@code null} if no parent filter
     * @param currentFilterKey the granularity map key of the nearest ancestor filter bucket, or
     *                         {@code null} if no ancestor is a filter bucket; used by metrics to
     *                         look up the correct {@link AggregationMetadataBuilder}
     * @param granularities the shared map of granularity keys to metadata builders, accumulated
     *                      across the entire tree walk
     * @param ctx the conversion context providing schema, RexBuilder, and cluster access
     * @throws ConversionException if no translator is registered for an aggregation type, or if
     *                             any translator fails during conversion
     */
    @SuppressWarnings("unchecked")
    private void walkRecursive(
        Collection<AggregationBuilder> aggs,
        List<GroupingInfo> currentGroupings,
        RexNode currentFilterCondition,
        String currentFilterKey,
        Map<String, AggregationMetadataBuilder> granularities,
        ConversionContext ctx
    ) throws ConversionException {
        for (AggregationBuilder aggBuilder : aggs) {
            AggregationTranslator<?> type = registry.get(aggBuilder.getClass());

            if (type == null) {
                throw new ConversionException("No translator registered for aggregation type: " + aggBuilder.getClass().getSimpleName());
            } else if (type instanceof BucketTranslator<?> bucketTranslator) {
                switch (bucketTranslator.getWalkStrategy()) {
                    case MULTI_FILTER:
                        handleMultiFiltersBucket(
                            (FiltersBucketTranslator) bucketTranslator,
                            (FiltersAggregationBuilder) aggBuilder,
                            currentGroupings,
                            currentFilterCondition,
                            granularities,
                            ctx
                        );
                        break;
                    case FILTER, STANDARD:
                    default:
                        handleBucket(
                            (BucketTranslator<AggregationBuilder>) bucketTranslator,
                            aggBuilder,
                            currentGroupings,
                            currentFilterCondition,
                            currentFilterKey,
                            granularities,
                            ctx
                        );
                        break;
                }
            } else if (type instanceof MetricTranslator) {
                handleMetric(
                    (MetricTranslator<AggregationBuilder>) type,
                    aggBuilder,
                    currentGroupings,
                    currentFilterKey,
                    granularities,
                    ctx.getRowType()
                );
            } else {
                throw new ConversionException("Unsupported aggregation translator kind: " + type.getClass().getSimpleName());
            }
        }
    }

    private void handleBucket(
        BucketTranslator<AggregationBuilder> translator,
        AggregationBuilder aggBuilder,
        List<GroupingInfo> currentGroupings,
        RexNode currentFilterCondition,
        String currentFilterKey,
        Map<String, AggregationMetadataBuilder> granularities,
        ConversionContext ctx
    ) throws ConversionException {
        GroupingInfo grouping = translator.getGrouping(aggBuilder);
        RexNode combinedFilter = currentFilterCondition;
        String filterKey = currentFilterKey;

        // For filter buckets: extract filter condition and compute unique key
        if (translator.getWalkStrategy() == BucketTranslator.WalkStrategy.FILTER) {
            FilterBucketTranslator filterTranslator = (FilterBucketTranslator) (BucketTranslator<?>) translator;
            FilterAggregationBuilder filterAgg = (FilterAggregationBuilder) aggBuilder;
            RexNode filterCondition = filterTranslator.getFilterCondition(filterAgg, ctx);

            combinedFilter = filterTranslator.combineWithParent(currentFilterCondition, filterCondition, ctx);
            filterKey = buildBucketGranularityKey(currentGroupings, "filter", aggBuilder.getName());
        }

        List<GroupingInfo> accumulatedGroupings = new ArrayList<>(currentGroupings);
        accumulatedGroupings.add(grouping);

        // Get or create builder — for filter buckets, use the filter key
        if (translator.getWalkStrategy() == BucketTranslator.WalkStrategy.FILTER) {
            AggregationMetadataBuilder builder = getOrCreateBuilder(filterKey, accumulatedGroupings, granularities);
            builder.setFilterCondition(combinedFilter);
        } else {
            // For standard buckets nested under a filter, use a filter-scoped key
            // so the parent filter condition is preserved in the plan.
            AggregationMetadataBuilder builder;
            if (currentFilterKey != null) {
                String scopedKey = currentFilterKey + "|" + granularityKey(accumulatedGroupings);
                filterKey = scopedKey;
                builder = getOrCreateBuilder(scopedKey, accumulatedGroupings, granularities);
                builder.setFilterCondition(combinedFilter);
            } else {
                builder = getOrCreateBuilder(accumulatedGroupings, granularities);
            }
            builder.addBucketOrder(translator.getBucketOrder(aggBuilder));
        }

        // Recurse into sub-aggregations
        Collection<AggregationBuilder> subAggs = translator.getSubAggregations(aggBuilder);
        if (subAggs != null && !subAggs.isEmpty()) {
            walkRecursive(subAggs, accumulatedGroupings, combinedFilter, filterKey, granularities, ctx);
        }
    }

    /**
     * Handles a multi-filter bucket aggregation (e.g., {@code filters}). Resolves all filter plans
     * from the translator and creates a separate granularity builder for each filter, then recurses
     * into sub-aggregations for each filter plan.
     *
     * @param translator the filters bucket translator
     * @param aggBuilder the filters aggregation builder
     * @param currentGroupings accumulated groupings from parent buckets
     * @param currentFilterCondition the combined filter condition from parent filter buckets, or null
     * @param granularities the map of granularity keys to metadata builders
     * @param ctx the conversion context
     * @throws ConversionException if filter conversion fails
     */
    private void handleMultiFiltersBucket(
        FiltersBucketTranslator translator,
        FiltersAggregationBuilder aggBuilder,
        List<GroupingInfo> currentGroupings,
        RexNode currentFilterCondition,
        Map<String, AggregationMetadataBuilder> granularities,
        ConversionContext ctx
    ) throws ConversionException {
        GroupingInfo grouping = translator.getGrouping(aggBuilder);
        Collection<AggregationBuilder> subAggs = translator.getSubAggregations(aggBuilder);
        List<FiltersBucketTranslator.FilterPlanDescriptor> filterPlans = translator.resolveFilterPlans(
            aggBuilder,
            currentFilterCondition,
            ctx
        );

        for (FiltersBucketTranslator.FilterPlanDescriptor plan : filterPlans) {
            String granKey = buildBucketGranularityKey(currentGroupings, "filter", aggBuilder.getName(), plan.bucketKey());

            List<GroupingInfo> accumulatedGroupings = new ArrayList<>(currentGroupings);
            accumulatedGroupings.add(grouping);

            AggregationMetadataBuilder builder = getOrCreateBuilder(granKey, accumulatedGroupings, granularities);
            builder.setFilterCondition(plan.condition());

            if (subAggs != null && subAggs.isEmpty() == false) {
                walkRecursive(subAggs, accumulatedGroupings, plan.condition(), granKey, granularities, ctx);
            }
        }
    }

    /**
     * Handles a metric aggregation (e.g., AVG, SUM, MIN, MAX, VALUE_COUNT) at the current
     * nesting level.
     *
     * <p>Locates the correct {@link AggregationMetadataBuilder} for this metric:
     * <ul>
     *   <li>If {@code currentFilterKey} is non-null, the metric is nested under a filter bucket,
     *       so it looks up the builder by the filter key to ensure the filter condition is included
     *       in the same plan.</li>
     *   <li>Otherwise, it uses the standard grouping-based key to find or create the builder.</li>
     * </ul>
     *
     * <p>The metric's {@link org.apache.calcite.rel.core.AggregateCall} is added to the builder,
     * which will later become part of the {@code LogicalAggregate} node.</p>
     *
     * @param translator the metric translator for this aggregation type
     * @param aggBuilder the metric aggregation builder instance
     * @param currentGroupings accumulated groupings from parent buckets (used for key lookup)
     * @param currentFilterKey the granularity key of the nearest ancestor filter bucket, or null
     * @param granularities the map of granularity keys to metadata builders
     * @param rowType the input row type for resolving field references
     * @throws ConversionException if the metric field is not found in the schema
     */
    private void handleMetric(
        MetricTranslator<AggregationBuilder> translator,
        AggregationBuilder aggBuilder,
        List<GroupingInfo> currentGroupings,
        String currentFilterKey,
        Map<String, AggregationMetadataBuilder> granularities,
        RelDataType rowType
    ) throws ConversionException {
        AggregationMetadataBuilder builder;
        if (currentFilterKey != null) {
            builder = granularities.get(currentFilterKey);
        } else {
            builder = getOrCreateBuilder(currentGroupings, granularities);
        }
        builder.addAggregateCall(translator.toAggregateCall(aggBuilder, rowType), translator.getAggregateFieldName(aggBuilder));
    }

    /**
     * Gets or creates a metadata builder for the given groupings, using the grouping fields
     * as the granularity key.
     *
     * @param groupings the groupings that define the granularity level
     * @param granularities the map of granularity keys to metadata builders
     * @return the existing or newly created builder
     */
    private AggregationMetadataBuilder getOrCreateBuilder(
        List<GroupingInfo> groupings,
        Map<String, AggregationMetadataBuilder> granularities
    ) {
        String key = granularityKey(groupings);
        return getOrCreateBuilder(key, groupings, granularities);
    }

    private AggregationMetadataBuilder getOrCreateBuilder(
        String key,
        List<GroupingInfo> groupings,
        Map<String, AggregationMetadataBuilder> granularities
    ) {
        AggregationMetadataBuilder existing = granularities.get(key);
        if (existing != null) {
            return existing;
        }

        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        for (GroupingInfo g : groupings) {
            builder.addGrouping(g);
        }
        if (!groupings.isEmpty()) {
            builder.requestImplicitCount();
        }
        granularities.put(key, builder);
        return builder;
    }

    private static String granularityKey(List<GroupingInfo> groupings) {
        if (groupings.isEmpty()) {
            return "";
        }
        return groupingFieldsBase(groupings);
    }

    /**
     * Builds a bucket-scoped granularity key: {@code <fields>|__<bucketType>__<aggName>}.
     *
     * @param groupings the accumulated groupings from parent buckets
     * @param bucketType the bucket type identifier (e.g., "filter", "adjacency")
     * @param aggName the aggregation name
     * @return the granularity key
     */
    static String buildBucketGranularityKey(List<GroupingInfo> groupings, String bucketType, String aggName) {
        String base = groupingFieldsBase(groupings);
        String suffix = "__" + bucketType + "__" + aggName;
        return base.isEmpty() ? suffix : base + "|" + suffix;
    }

    /**
     * Builds a bucket-scoped granularity key with a sub-key: {@code <fields>|__<bucketType>__<aggName>/<subKey>}.
     *
     * @param groupings the accumulated groupings from parent buckets
     * @param bucketType the bucket type identifier (e.g., "filter", "range")
     * @param aggName the aggregation name
     * @param subKey the per-plan sub-key (e.g., filter key, range key)
     * @return the granularity key
     */
    static String buildBucketGranularityKey(List<GroupingInfo> groupings, String bucketType, String aggName, String subKey) {
        String base = groupingFieldsBase(groupings);
        String suffix = "__" + bucketType + "__" + aggName + "/" + subKey;
        return base.isEmpty() ? suffix : base + "|" + suffix;
    }

    private static String groupingFieldsBase(List<GroupingInfo> groupings) {
        return IntStream.range(0, groupings.size())
            .mapToObj(i -> i + ":" + String.join(",", groupings.get(i).getFieldNames()))
            .collect(Collectors.joining("|"));
    }
}
