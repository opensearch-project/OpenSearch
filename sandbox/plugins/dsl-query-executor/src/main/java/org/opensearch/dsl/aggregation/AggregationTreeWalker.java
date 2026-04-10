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
import org.opensearch.dsl.aggregation.pipeline.BucketsPathResolver;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Recursively walks the DSL aggregation tree and produces one {@link AggregationMetadata}
 * per distinct granularity level.
 *
 * <p>A "granularity" is a unique GROUP BY key set determined by the accumulated bucket
 * nesting path. Metrics at different nesting depths produce separate metadata instances,
 * each yielding its own {@code LogicalAggregate} and {@code QueryPlan}.
 *
 * <p>Filter buckets are special: they do not add GROUP BY columns but instead carry a
 * filter condition that produces a separate plan with a {@code LogicalFilter} node.
 * Each filter bucket gets a unique granularity key using a {@code __filter__} prefix
 * to avoid collisions with field-name-based keys.
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
     * @param ctx the conversion context providing row type, type factory, and RexBuilder
     * @return metadata list, one per granularity (only levels with metrics or implicit count)
     * @throws ConversionException if any aggregation fails to convert
     */
    public List<AggregationMetadata> walk(
        Collection<AggregationBuilder> aggs,
        ConversionContext ctx
    ) throws ConversionException {
        return walk(aggs, List.of(), ctx);
    }

    /**
     * Walks the aggregation tree and returns one AggregationMetadata per granularity level.
     *
     * @param aggs the top-level aggregation builders
     * @param pipelineAggs the top-level pipeline aggregation builders
     * @param ctx the conversion context providing row type, type factory, and RexBuilder
     * @return metadata list, one per granularity (only levels with metrics or implicit count)
     * @throws ConversionException if any aggregation fails to convert
     */
    public List<AggregationMetadata> walk(
        Collection<AggregationBuilder> aggs,
        Collection<PipelineAggregationBuilder> pipelineAggs,
        ConversionContext ctx
    ) throws ConversionException {
        RelDataType rowType = ctx.getRowType();
        RelDataTypeFactory typeFactory = ctx.getCluster().getTypeFactory();
        Map<String, AggregationMetadataBuilder> granularities = new LinkedHashMap<>();
        walkRecursive(aggs, pipelineAggs, new ArrayList<>(), null, null, granularities, ctx);

        List<AggregationMetadata> result = new ArrayList<>();
        for (AggregationMetadataBuilder builder : granularities.values()) {
            if (builder.hasAggregateCalls()) {
                result.add(builder.build(rowType, typeFactory));
            }
        }
        return result;
    }

    /**
     * Recursively walks the aggregation tree, dispatching each aggregation to the appropriate
     * handler based on its type (bucket, metric, or pipeline).
     *
     * @param aggs the aggregation builders at the current nesting level
     * @param currentGroupings accumulated groupings from parent bucket aggregations
     * @param currentFilterCondition the combined filter condition from parent filter buckets, or null
     * @param currentFilterKey the granularity key for the current filter scope, or null
     * @param granularities the map of granularity keys to metadata builders being accumulated
     * @param ctx the conversion context
     * @throws ConversionException if any aggregation fails to convert
     */
    @SuppressWarnings("unchecked")
    private void walkRecursive(
        Collection<AggregationBuilder> aggs,
        Collection<PipelineAggregationBuilder> pipelineAggs,
        List<GroupingInfo> currentGroupings,
        RexNode currentFilterCondition,
        String currentFilterKey,
        Map<String, AggregationMetadataBuilder> granularities,
        ConversionContext ctx
    ) throws ConversionException {
        for (AggregationBuilder aggBuilder : aggs) {
            AggregationType<?> type = registry.get(aggBuilder.getClass());

            if (type instanceof BucketTranslator<?> bucketTranslator) {
                switch (bucketTranslator.getWalkStrategy()) {
                    case MULTI_FILTER:
                        handleMultiFiltersBucket(
                            (FiltersBucketTranslator) bucketTranslator, (FiltersAggregationBuilder) aggBuilder,
                            currentGroupings, currentFilterCondition, currentFilterKey, granularities, ctx
                        );
                        break;
                    case FILTER, STANDARD:
                    default:
                        handleBucket(
                            (BucketTranslator<AggregationBuilder>) bucketTranslator, aggBuilder, currentGroupings,
                            currentFilterCondition, currentFilterKey, granularities, ctx
                        );
                        break;
                }
            } else if (type instanceof MetricTranslator) {
                handleMetric(
                    (MetricTranslator<AggregationBuilder>) type, aggBuilder, currentGroupings,
                    currentFilterKey, granularities, ctx
                );
            } else {
                throw new ConversionException("Unsupported aggregation type: " + aggBuilder.getClass().getSimpleName());
            }
        }

        // Collect pipeline aggregation builders into the current granularity builder.
        // Sibling pipeline aggs (e.g., avg_bucket) operate on the output of their parent
        // bucket aggregation. The buckets_path may reference metrics in deeper granularities,
        // so we use BucketsPathResolver to find the correct builder based on the full path.
        if (pipelineAggs != null && !pipelineAggs.isEmpty()) {
            for (PipelineAggregationBuilder pipelineBuilder : pipelineAggs) {
                AggregationMetadataBuilder builder;
                if (currentFilterKey != null) {
                    builder = granularities.get(currentFilterKey);
                } else {
                    builder = BucketsPathResolver.findBuilderForPipeline(
                        pipelineBuilder, currentGroupings, granularities);
                }
                builder.addPipelineBuilder(pipelineBuilder);
            }
        }
    }

    /**
     * Handles a standard or single-filter bucket aggregation. Accumulates groupings,
     * extracts filter conditions for filter buckets, and recurses into sub-aggregations.
     *
     * @param translator the bucket translator
     * @param aggBuilder the aggregation builder
     * @param currentGroupings accumulated groupings from parent buckets
     * @param currentFilterCondition the combined filter condition from parent filter buckets, or null
     * @param currentFilterKey the granularity key for the current filter scope, or null
     * @param granularities the map of granularity keys to metadata builders
     * @param ctx the conversion context
     * @throws ConversionException if conversion fails
     */
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

        // Accumulate groupings (empty for filter buckets, non-empty for terms etc.)
        List<GroupingInfo> accumulatedGroupings = new ArrayList<>(currentGroupings);
        accumulatedGroupings.add(grouping);

        // Get or create builder — for filter buckets, use the filter key
        if (translator.getWalkStrategy() == BucketTranslator.WalkStrategy.FILTER) {
            AggregationMetadataBuilder builder = getOrCreateBuilder(filterKey, accumulatedGroupings, granularities);
            builder.setFilterCondition(combinedFilter);
            builder.addAggNameMapping(aggBuilder.getName(), "__filter__" + aggBuilder.getName());
        } else {
            AggregationMetadataBuilder builder = getOrCreateBuilder(accumulatedGroupings, granularities);
            builder.addAggNameMapping(aggBuilder.getName(), String.join(",", grouping.getFieldNames()));
        }

        // Recurse into sub-aggregations and pipeline sub-aggregations
        Collection<AggregationBuilder> subAggs = translator.getSubAggregations(aggBuilder);
        Collection<PipelineAggregationBuilder> pipelineSubAggs = aggBuilder.getPipelineAggregations();
        boolean hasSubAggs = subAggs != null && !subAggs.isEmpty();
        boolean hasPipelineSubAggs = pipelineSubAggs != null && !pipelineSubAggs.isEmpty();
        if (hasSubAggs || hasPipelineSubAggs) {
            walkRecursive(
                hasSubAggs ? subAggs : List.of(),
                hasPipelineSubAggs ? pipelineSubAggs : List.of(),
                accumulatedGroupings, combinedFilter, filterKey, granularities, ctx
            );
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
     * @param currentFilterKey the granularity key for the current filter scope, or null
     * @param granularities the map of granularity keys to metadata builders
     * @param ctx the conversion context
     * @throws ConversionException if filter conversion fails
     */
    private void handleMultiFiltersBucket(
        FiltersBucketTranslator translator,
        FiltersAggregationBuilder aggBuilder,
        List<GroupingInfo> currentGroupings,
        RexNode currentFilterCondition,
        String currentFilterKey,
        Map<String, AggregationMetadataBuilder> granularities,
        ConversionContext ctx
    ) throws ConversionException {
        GroupingInfo grouping = translator.getGrouping(aggBuilder);
        Collection<AggregationBuilder> subAggs = translator.getSubAggregations(aggBuilder);

        List<FiltersBucketTranslator.FilterPlanDescriptor> filterPlans =
            translator.resolveFilterPlans(aggBuilder, currentFilterCondition, ctx);

        for (FiltersBucketTranslator.FilterPlanDescriptor plan : filterPlans) {
            String granKey = buildBucketGranularityKey(currentGroupings, "filter", aggBuilder.getName(), plan.bucketKey());

            List<GroupingInfo> accumulatedGroupings = new ArrayList<>(currentGroupings);
            accumulatedGroupings.add(grouping);

            AggregationMetadataBuilder builder = getOrCreateBuilder(granKey, accumulatedGroupings, granularities);
            builder.setFilterCondition(plan.condition());
            builder.setBucketKey(plan.bucketKey());
            builder.setAggregationName(aggBuilder.getName());
            builder.addAggNameMapping(aggBuilder.getName(),
                "__filter__" + aggBuilder.getName() + "/" + plan.bucketKey());

            if (subAggs != null && subAggs.isEmpty() == false) {
                walkRecursive(subAggs, aggBuilder.getPipelineAggregations(), accumulatedGroupings, plan.condition(), granKey, granularities, ctx);
            } else if (aggBuilder.getPipelineAggregations() != null && !aggBuilder.getPipelineAggregations().isEmpty()) {
                walkRecursive(List.of(), aggBuilder.getPipelineAggregations(), accumulatedGroupings, plan.condition(), granKey, granularities, ctx);
            }
        }
    }

    /**
     * Handles a metric aggregation by resolving it to an {@link org.apache.calcite.rel.core.AggregateCall}
     * and adding it to the appropriate granularity builder.
     *
     * @param translator the metric translator
     * @param aggBuilder the aggregation builder
     * @param currentGroupings accumulated groupings from parent buckets
     * @param currentFilterKey the granularity key for the current filter scope, or null
     * @param granularities the map of granularity keys to metadata builders
     * @param ctx the conversion context
     * @throws ConversionException if metric conversion fails
     */
    private void handleMetric(
        MetricTranslator<AggregationBuilder> translator,
        AggregationBuilder aggBuilder,
        List<GroupingInfo> currentGroupings,
        String currentFilterKey,
        Map<String, AggregationMetadataBuilder> granularities,
        ConversionContext ctx
    ) throws ConversionException {
        RelDataType rowType = ctx.getRowType();
        AggregationMetadataBuilder builder;
        if (currentFilterKey != null) {
            builder = granularities.get(currentFilterKey);
        } else {
            builder = getOrCreateBuilder(currentGroupings, granularities);
        }
        builder.addAggregateCall(
            translator.toAggregateCall(aggBuilder, rowType),
            translator.getAggregateFieldName(aggBuilder)
        );
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

    /**
     * Gets or creates a metadata builder for the given explicit key and groupings.
     * If a builder already exists for the key, it is returned as-is. Otherwise, a new builder
     * is created with the given groupings and an implicit COUNT(*) if groupings are non-empty.
     *
     * @param key the granularity key
     * @param groupings the groupings to initialize the builder with
     * @param granularities the map of granularity keys to metadata builders
     * @return the existing or newly created builder
     */
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

    /**
     * Computes a granularity key from the accumulated groupings. Returns an empty string
     * for no-GROUP-BY (top-level metrics only) granularity.
     *
     * @param groupings the accumulated groupings
     * @return the granularity key string
     */
    private static String granularityKey(List<GroupingInfo> groupings) {
        if (groupings.isEmpty()) {
            return "";
        }
        return groupingFieldsBase(groupings);
    }

    /**
     * Builds a bucket-scoped granularity key: {@code <fields>,__<bucketType>__<aggName>}.
     * Uses comma separation consistent with field-based keys.
     *
     * @param groupings the accumulated groupings from parent buckets
     * @param bucketType the bucket type identifier (e.g., "filter", "adjacency")
     * @param aggName the aggregation name
     * @return the granularity key
     */
    static String buildBucketGranularityKey(List<GroupingInfo> groupings, String bucketType, String aggName) {
        String base = groupingFieldsBase(groupings);
        String suffix = "__" + bucketType + "__" + aggName;
        return base.isEmpty() ? suffix : base + "," + suffix;
    }

    /**
     * Builds a bucket-scoped granularity key with a sub-key: {@code <fields>,__<bucketType>__<aggName>/<subKey>}.
     * Uses comma separation consistent with field-based keys.
     *
     * @param groupings the accumulated groupings from parent buckets
     * @param bucketType the bucket type identifier (e.g., "filter", "range")
     * @param aggName the aggregation name
     * @param subKey the per-plan sub-key (e.g., filter key, range key)
     * @return the granularity key
     */
    static String buildBucketGranularityKey(
        List<GroupingInfo> groupings,
        String bucketType,
        String aggName,
        String subKey
    ) {
        String base = groupingFieldsBase(groupings);
        String suffix = "__" + bucketType + "__" + aggName + "/" + subKey;
        return base.isEmpty() ? suffix : base + "," + suffix;
    }

    /**
     * Builds the base portion of a granularity key by joining all field names from the
     * accumulated groupings with commas.
     *
     * @param groupings the accumulated groupings
     * @return comma-separated field names, or empty string if no fields
     */
    private static String groupingFieldsBase(List<GroupingInfo> groupings) {
        return groupings.stream()
            .flatMap(g -> g.getFieldNames().stream())
            .collect(Collectors.joining(","));
    }
}
