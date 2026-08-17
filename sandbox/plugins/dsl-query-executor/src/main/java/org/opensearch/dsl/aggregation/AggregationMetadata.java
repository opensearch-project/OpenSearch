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
import java.util.Map;

/**
 * Pre-computed metadata for one aggregation plan.
 *
 * <p>A plan is defined by its bucket aggregation, not merely its GROUP BY columns: the
 * aggregation's {@code size}, {@code min_doc_count}, and order are baked into the plan as
 * LIMIT, HAVING, and SORT. Two sibling aggregations over the same field therefore produce two
 * plans — one per aggregation — each bounded and ordered for its own request parameters.
 * Metrics ride in their enclosing bucket aggregation's plan; root-level metrics form one
 * global no-GROUP-BY plan.
 *
 * <p>Identity is the {@link #getAggNamePath() aggregation-name path} (names are unique among
 * siblings by DSL contract), which is also the key the response builder uses to match results
 * back to the request tree.
 */
public class AggregationMetadata {

    /** NUL cannot appear in aggregation names, so joined path keys cannot collide. */
    private static final String PATH_SEPARATOR = "\u0000";

    private final List<String> aggNamePath;
    private final ImmutableBitSet groupByBitSet;
    private final List<String> groupByFieldNames;
    private final List<AggregateCall> aggregateCalls;
    private final List<String> aggregateFieldNames;
    private final List<BucketOrder> bucketOrders;
    private final Integer fetch;
    private final Integer perParentFetch;
    private final Long havingMinDocCount;
    private final Map<String, Object> missingValues;

    /**
     * Creates aggregation metadata.
     *
     * @param aggNamePath the defining aggregation-name path (outer bucket first); empty for
     *        the global no-GROUP-BY metrics plan
     * @param groupByBitSet column indices for GROUP BY
     * @param groupByFieldNames field names for GROUP BY columns
     * @param aggregateCalls Calcite aggregate calls (AVG, SUM, etc.)
     * @param aggregateFieldNames output names for aggregate results
     * @param bucketOrders bucket orders for post-aggregation sorting
     * @param fetch plan-level row limit, or null for no limit
     * @param perParentFetch per-parent row limit for nested levels (ROW_NUMBER window over the
     *        parent partition), or null
     * @param havingMinDocCount minimum bucket doc count for a plan-level HAVING filter, or null
     *        for none
     * @param missingValues null-substitution value per group field ({@code missing} parameter);
     *        fields absent from the map get an {@code IS NOT NULL} filter instead
     */
    public AggregationMetadata(
        List<String> aggNamePath,
        ImmutableBitSet groupByBitSet,
        List<String> groupByFieldNames,
        List<AggregateCall> aggregateCalls,
        List<String> aggregateFieldNames,
        List<BucketOrder> bucketOrders,
        Integer fetch,
        Integer perParentFetch,
        Long havingMinDocCount,
        Map<String, Object> missingValues
    ) {
        this.aggNamePath = List.copyOf(aggNamePath);
        this.groupByBitSet = groupByBitSet;
        this.groupByFieldNames = List.copyOf(groupByFieldNames);
        this.aggregateCalls = List.copyOf(aggregateCalls);
        this.aggregateFieldNames = List.copyOf(aggregateFieldNames);
        this.bucketOrders = List.copyOf(bucketOrders);
        this.fetch = fetch;
        this.perParentFetch = perParentFetch;
        this.havingMinDocCount = havingMinDocCount;
        this.missingValues = Map.copyOf(missingValues);
    }

    /**
     * Returns the defining aggregation-name path, outer bucket first (e.g.
     * {@code [by_brand, by_category]} for a nested terms tree). Empty for the global
     * no-GROUP-BY metrics plan. This is the plan's identity: sibling aggregations over the
     * same fields have distinct paths and distinct plans.
     */
    public List<String> getAggNamePath() {
        return aggNamePath;
    }

    /**
     * Canonical string form of an aggregation-name path (see {@link #getAggNamePath()}): names
     * in nesting order, NUL-joined. This is the one key protocol shared by plan construction,
     * result registration, and response-side lookup — a plan's results are found only if all
     * sides build the identical key.
     *
     * @param aggNamePath aggregation names, outer bucket first
     * @return the joined key
     */
    public static String pathKey(List<String> aggNamePath) {
        return String.join(PATH_SEPARATOR, aggNamePath);
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

    /** Returns the bucket orders for post-aggregation sorting. */
    public List<BucketOrder> getBucketOrders() {
        return bucketOrders;
    }

    /** Returns true if bucket orders are present. */
    public boolean hasBucketOrders() {
        return !bucketOrders.isEmpty();
    }

    /**
     * Returns the plan-level row limit, or null when the plan's bound is per-parent
     * ({@link #getPerParentFetch()}) or absent. Root-level sized bucket plans carry this limit.
     */
    public Integer getFetch() {
        return fetch;
    }

    /**
     * Returns the per-parent row limit for a nested level's plan — enforced by
     * {@code ROW_NUMBER() OVER (PARTITION BY parentFields ORDER BY bucketOrder) <= K} with the
     * plan semi-joined to the parent plan's top-N — or null for non-nested plans. Each
     * surviving row also carries the parent's eligible-document total for
     * {@code sum_other_doc_count}.
     */
    public Integer getPerParentFetch() {
        return perParentFetch;
    }

    /**
     * Returns the minimum bucket doc count to apply as a plan-level HAVING filter between the
     * aggregate and the sort, or null when no filtering is needed ({@code min_doc_count} ≤ 1).
     */
    public Long getHavingMinDocCount() {
        return havingMinDocCount;
    }

    /**
     * Returns the null-substitution value per group field (the {@code missing} parameter).
     * Group fields absent from this map exclude null keys via a pre-aggregate
     * {@code IS NOT NULL} filter instead.
     */
    public Map<String, Object> getMissingValues() {
        return missingValues;
    }

    /**
     * Returns true when this bounded plan's eligible-doc count is the plain
     * matching-document total ({@code COUNT(*)}): a {@code missing} value on the group field
     * makes every query-matching document eligible. False when the plan carries a
     * {@code min_doc_count} HAVING — the threshold drops whole groups from eligibility even with
     * {@code missing} — or when no {@code missing} value is configured; those plans use a
     * per-aggregation eligible count instead. Meaningful only for plans with {@link #getFetch()}
     * set (root-level, single-field by eligibility).
     */
    public boolean eligibleDocCountIsTotal() {
        return havingMinDocCount == null && !groupByFieldNames.isEmpty() && missingValues.containsKey(groupByFieldNames.get(0));
    }
}
