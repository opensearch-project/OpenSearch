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
import org.opensearch.dsl.aggregation.bucket.BucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;

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
     * @param rowType the input row type for field resolution
     * @param typeFactory the type factory for creating aggregate return types
     * @return metadata list, one per granularity (only levels with metrics or implicit count)
     * @throws ConversionException if any aggregation fails to convert
     */
    public List<AggregationMetadata> walk(
        Collection<AggregationBuilder> aggs,
        RelDataType rowType,
        RelDataTypeFactory typeFactory
    ) throws ConversionException {
        Map<String, AggregationMetadataBuilder> granularities = new LinkedHashMap<>();
        walkRecursive(aggs, new ArrayList<>(), granularities, rowType);

        List<AggregationMetadata> result = new ArrayList<>();
        for (AggregationMetadataBuilder builder : granularities.values()) {
            if (builder.hasAggregateCalls()) {
                result.add(builder.build(rowType, typeFactory));
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private void walkRecursive(
        Collection<AggregationBuilder> aggs,
        List<GroupingInfo> currentGroupings,
        Map<String, AggregationMetadataBuilder> granularities,
        RelDataType rowType
    ) throws ConversionException {
        for (AggregationBuilder aggBuilder : aggs) {
            AggregationType<?> type = registry.get(aggBuilder.getClass());

            if (type instanceof BucketTranslator) {
                handleBucket((BucketTranslator<AggregationBuilder>) type, aggBuilder, currentGroupings, granularities, rowType);
            } else if (type instanceof MetricTranslator) {
                handleMetric((MetricTranslator<AggregationBuilder>) type, aggBuilder, currentGroupings, granularities, rowType);
            } else {
                throw new ConversionException("Unsupported aggregation type: " + aggBuilder.getClass().getSimpleName());
            }
        }
    }

    private void handleBucket(
        BucketTranslator<AggregationBuilder> translator,
        AggregationBuilder aggBuilder,
        List<GroupingInfo> currentGroupings,
        Map<String, AggregationMetadataBuilder> granularities,
        RelDataType rowType
    ) throws ConversionException {
        GroupingInfo grouping = translator.getGrouping(aggBuilder);

        List<GroupingInfo> accumulatedGroupings = new ArrayList<>(currentGroupings);
        accumulatedGroupings.add(grouping);

        // Ensure builder exists for this granularity
        getOrCreateBuilder(accumulatedGroupings, granularities);

        // Recurse into sub-aggregations
        Collection<AggregationBuilder> subAggs = translator.getSubAggregations(aggBuilder);
        if (subAggs != null && !subAggs.isEmpty()) {
            walkRecursive(subAggs, accumulatedGroupings, granularities, rowType);
        }
    }

    private void handleMetric(
        MetricTranslator<AggregationBuilder> translator,
        AggregationBuilder aggBuilder,
        List<GroupingInfo> currentGroupings,
        Map<String, AggregationMetadataBuilder> granularities,
        RelDataType rowType
    ) throws ConversionException {
        AggregationMetadataBuilder builder = getOrCreateBuilder(currentGroupings, granularities);
        builder.addAggregateCall(
            translator.toAggregateCall(aggBuilder, rowType),
            translator.getAggregateFieldName(aggBuilder)
        );
    }

    private AggregationMetadataBuilder getOrCreateBuilder(
        List<GroupingInfo> groupings,
        Map<String, AggregationMetadataBuilder> granularities
    ) {
        String key = granularityKey(groupings);
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
        return groupings.stream()
            .flatMap(g -> g.getFieldNames().stream())
            .collect(Collectors.joining(","));
    }
}
