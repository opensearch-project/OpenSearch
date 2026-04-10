/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;

import java.util.List;

/**
 * Pre-computed metadata for one aggregation granularity level.
 * Contains everything needed to build a single {@code LogicalAggregate}.
 *
 * <p>A multi-level aggregation tree (e.g., terms → terms → avg) produces
 * multiple metadata instances — one per distinct GROUP BY key set.
 */
public class AggregationMetadata {

    private final ImmutableBitSet groupByBitSet;
    private final List<String> groupByFieldNames;
    private final List<AggregateCall> aggregateCalls;
    private final List<String> aggregateFieldNames;
    private final RexNode filterCondition;
    private final String bucketKey;
    private final String aggregationName;
    private final List<PipelineAggregationBuilder> pipelineBuilders;

    /**
     * Creates aggregation metadata.
     *
     * @param groupByBitSet column indices for GROUP BY
     * @param groupByFieldNames field names for GROUP BY columns
     * @param aggregateCalls Calcite aggregate calls (AVG, SUM, etc.)
     * @param aggregateFieldNames output names for aggregate results
     * @param filterCondition filter condition from a filter bucket, or null if no filter applies
     * @param bucketKey bucket key for response assembly, or null if no bucket key applies
     * @param aggregationName the DSL aggregation name for response assembly, or null
     * @param pipelineBuilders pipeline aggregation builders collected during tree walk
     */
    public AggregationMetadata(
        ImmutableBitSet groupByBitSet,
        List<String> groupByFieldNames,
        List<AggregateCall> aggregateCalls,
        List<String> aggregateFieldNames,
        RexNode filterCondition,
        String bucketKey,
        String aggregationName,
        List<PipelineAggregationBuilder> pipelineBuilders
    ) {
        this.groupByBitSet = groupByBitSet;
        this.groupByFieldNames = List.copyOf(groupByFieldNames);
        this.aggregateCalls = List.copyOf(aggregateCalls);
        this.aggregateFieldNames = List.copyOf(aggregateFieldNames);
        this.filterCondition = filterCondition;
        this.bucketKey = bucketKey;
        this.aggregationName = aggregationName;
        this.pipelineBuilders = pipelineBuilders != null ? List.copyOf(pipelineBuilders) : List.of();
    }

    /**
     * Creates aggregation metadata.
     *
     * @param groupByBitSet column indices for GROUP BY
     * @param groupByFieldNames field names for GROUP BY columns
     * @param aggregateCalls Calcite aggregate calls (AVG, SUM, etc.)
     * @param aggregateFieldNames output names for aggregate results
     * @param filterCondition filter condition from a filter bucket, or null if no filter applies
     * @param bucketKey bucket key for response assembly, or null if no bucket key applies
     * @param aggregationName the DSL aggregation name for response assembly, or null
     */
    public AggregationMetadata(
        ImmutableBitSet groupByBitSet,
        List<String> groupByFieldNames,
        List<AggregateCall> aggregateCalls,
        List<String> aggregateFieldNames,
        RexNode filterCondition,
        String bucketKey,
        String aggregationName
    ) {
        this(groupByBitSet, groupByFieldNames, aggregateCalls, aggregateFieldNames,
            filterCondition, bucketKey, aggregationName, List.of());
    }

    /**
     * Creates aggregation metadata without an aggregation name.
     *
     * @param groupByBitSet column indices for GROUP BY
     * @param groupByFieldNames field names for GROUP BY columns
     * @param aggregateCalls Calcite aggregate calls (AVG, SUM, etc.)
     * @param aggregateFieldNames output names for aggregate results
     * @param filterCondition filter condition from a filter bucket, or null if no filter applies
     * @param bucketKey bucket key for response assembly, or null if no bucket key applies
     */
    public AggregationMetadata(
        ImmutableBitSet groupByBitSet,
        List<String> groupByFieldNames,
        List<AggregateCall> aggregateCalls,
        List<String> aggregateFieldNames,
        RexNode filterCondition,
        String bucketKey
    ) {
        this(groupByBitSet, groupByFieldNames, aggregateCalls, aggregateFieldNames,
            filterCondition, bucketKey, null, List.of());
    }

    /** Returns the GROUP BY column indices. */
    public ImmutableBitSet getGroupByBitSet() {
        return groupByBitSet;
    }

    /** Returns the GROUP BY field names. */
    public List<String> getGroupByFieldNames() {
        return groupByFieldNames;
    }

    /** Returns the aggregate calls. */
    public List<AggregateCall> getAggregateCalls() {
        return aggregateCalls;
    }

    /** Returns the output field names for aggregate results. */
    public List<String> getAggregateFieldNames() {
        return aggregateFieldNames;
    }

    /** Returns the filter condition, or null if no filter bucket applies. */
    public RexNode getFilterCondition() {
        return filterCondition;
    }

    /** Returns the bucket key for response assembly, or null if no bucket key applies. */
    public String getBucketKey() {
        return bucketKey;
    }

    /** Returns the DSL aggregation name for response assembly, or null if not set. */
    public String getAggregationName() {
        return aggregationName;
    }

    /** Returns the pipeline aggregation builders collected during tree walk. */
    public List<PipelineAggregationBuilder> getPipelineBuilders() {
        return pipelineBuilders;
    }
}
