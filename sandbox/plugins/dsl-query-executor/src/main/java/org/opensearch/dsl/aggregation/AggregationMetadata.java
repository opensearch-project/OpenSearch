/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.search.aggregations.BucketOrder;

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
    private final List<BucketOrder> bucketOrders;
    private final List<GroupingInfo> groupings;

    /**
     * Creates aggregation metadata.
     *
     * @param groupByBitSet column indices for GROUP BY
     * @param groupByFieldNames field names for GROUP BY columns
     * @param aggregateCalls Calcite aggregate calls (AVG, SUM, etc.)
     * @param aggregateFieldNames output names for aggregate results
     * @param bucketOrders bucket ordering for sorting
     * @param groupings grouping strategies for bucket aggregations
     */
    public AggregationMetadata(
        ImmutableBitSet groupByBitSet,
        List<String> groupByFieldNames,
        List<AggregateCall> aggregateCalls,
        List<String> aggregateFieldNames,
        List<BucketOrder> bucketOrders,
        List<GroupingInfo> groupings
    ) {
        this.groupByBitSet = groupByBitSet;
        this.groupByFieldNames = List.copyOf(groupByFieldNames);
        this.aggregateCalls = List.copyOf(aggregateCalls);
        this.aggregateFieldNames = List.copyOf(aggregateFieldNames);
        this.bucketOrders = List.copyOf(bucketOrders);
        this.groupings = List.copyOf(groupings);
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

    /** Returns the grouping strategies. */
    public List<GroupingInfo> getGroupings() {
        return groupings;
    }

    /** Returns the bucket orders. */
    public List<BucketOrder> getBucketOrders() {
        return bucketOrders;
    }
}
