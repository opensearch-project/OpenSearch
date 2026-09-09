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
import org.opensearch.dsl.aggregation.bucket.SizedBucketTranslator;
import org.opensearch.dsl.aggregation.metric.MetricTranslator;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.search.aggregations.AggregationBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Recursively walks the DSL aggregation tree and produces one {@link AggregationMetadata}
 * per plan.
 *
 * <p>Plans are per bucket aggregation, keyed by the aggregation-name path (unique among
 * siblings by DSL contract): each bucket aggregation's plan bakes in its own {@code size},
 * {@code min_doc_count}, and order, so sibling aggregations over the same field produce
 * separate plans rather than sharing one that cannot satisfy both. Metrics ride in their
 * enclosing bucket aggregation's plan; root-level metrics share one global no-GROUP-BY plan.
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

    /** One step of the accumulated nesting path: the aggregation name and its grouping. */
    private record PathStep(String aggName, GroupingInfo grouping) {
    }

    /**
     * Walks the aggregation tree and returns one AggregationMetadata per plan.
     *
     * @param aggs the top-level aggregation builders
     * @param rowType the input row type for field resolution
     * @param typeFactory the type factory for creating aggregate return types
     * @return metadata list, one per plan (only plans with metrics or implicit count). A parent
     *         plan always precedes its children — nested plan construction consumes the parent's
     *         built plan and depends on this order.
     * @throws ConversionException if any aggregation fails to convert
     */
    public List<AggregationMetadata> walk(Collection<AggregationBuilder> aggs, RelDataType rowType, RelDataTypeFactory typeFactory)
        throws ConversionException {
        Map<String, AggregationMetadataBuilder> plans = new LinkedHashMap<>();
        walkRecursive(aggs, new ArrayList<>(), plans, rowType);

        List<AggregationMetadata> result = new ArrayList<>();
        for (AggregationMetadataBuilder builder : plans.values()) {
            if (builder.hasAggregateCalls()) {
                result.add(builder.build(rowType, typeFactory));
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private void walkRecursive(
        Collection<AggregationBuilder> aggs,
        List<PathStep> currentPath,
        Map<String, AggregationMetadataBuilder> plans,
        RelDataType rowType
    ) throws ConversionException {
        for (AggregationBuilder aggBuilder : aggs) {
            AggregationTranslator<?> type = registry.get(aggBuilder.getClass());

            if (type == null) {
                throw new ConversionException("No translator registered for aggregation type: " + aggBuilder.getClass().getSimpleName());
            }
            // Reject unsupported parameters before any plan state accumulates.
            ((AggregationTranslator<AggregationBuilder>) type).validate(aggBuilder);
            if (type instanceof BucketTranslator) {
                handleBucket((BucketTranslator<AggregationBuilder>) type, aggBuilder, currentPath, plans, rowType);
            } else if (type instanceof MetricTranslator) {
                handleMetric((MetricTranslator<AggregationBuilder>) type, aggBuilder, currentPath, plans, rowType);
            } else {
                throw new ConversionException("Unsupported aggregation translator kind: " + type.getClass().getSimpleName());
            }
        }
    }

    private void handleBucket(
        BucketTranslator<AggregationBuilder> translator,
        AggregationBuilder aggBuilder,
        List<PathStep> currentPath,
        Map<String, AggregationMetadataBuilder> plans,
        RelDataType rowType
    ) throws ConversionException {
        GroupingInfo grouping = translator.getGrouping(aggBuilder);

        List<PathStep> accumulatedPath = new ArrayList<>(currentPath);
        accumulatedPath.add(new PathStep(aggBuilder.getName(), grouping));

        // Every bucket aggregation defines its own plan; sibling names are unique by DSL
        // contract, so the path key cannot collide with another plan's.
        AggregationMetadataBuilder builder = getOrCreateBuilder(accumulatedPath, plans);
        if (translator instanceof SizedBucketTranslator<AggregationBuilder> sized) {
            builder.setBucketDefinition(translator.getBucketOrder(aggBuilder), sized.size(aggBuilder), sized.minDocCount(aggBuilder));
        } else {
            // Base-contract bucket types return their full bucket set; the plan stays unbounded.
            builder.setBucketDefinition(translator.getBucketOrder(aggBuilder), null, null);
        }

        // Recurse into sub-aggregations
        Collection<AggregationBuilder> subAggs = translator.getSubAggregations(aggBuilder);
        if (subAggs != null && !subAggs.isEmpty()) {
            walkRecursive(subAggs, accumulatedPath, plans, rowType);
        }
    }

    private void handleMetric(
        MetricTranslator<AggregationBuilder> translator,
        AggregationBuilder aggBuilder,
        List<PathStep> currentPath,
        Map<String, AggregationMetadataBuilder> plans,
        RelDataType rowType
    ) throws ConversionException {
        AggregationMetadataBuilder builder = getOrCreateBuilder(currentPath, plans);
        builder.addAggregateCall(translator.toAggregateCall(aggBuilder, rowType), translator.getAggregateFieldName(aggBuilder));
    }

    private AggregationMetadataBuilder getOrCreateBuilder(List<PathStep> path, Map<String, AggregationMetadataBuilder> plans) {
        String key = pathKey(path);
        AggregationMetadataBuilder existing = plans.get(key);
        if (existing != null) {
            return existing;
        }

        List<String> aggNamePath = path.stream().map(PathStep::aggName).toList();
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder(aggNamePath);
        for (PathStep step : path) {
            builder.addGrouping(step.grouping());
        }
        if (!path.isEmpty()) {
            builder.requestImplicitCount();
        }
        plans.put(key, builder);
        return builder;
    }

    private static String pathKey(List<PathStep> path) {
        return AggregationMetadata.pathKey(path.stream().map(PathStep::aggName).toList());
    }
}
