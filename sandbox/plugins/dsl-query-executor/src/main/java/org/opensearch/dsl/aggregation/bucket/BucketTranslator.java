/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.aggregation.AggregationType;
import org.opensearch.dsl.aggregation.GroupingInfo;
import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

import java.util.Collection;
import java.util.List;

/**
 * Translates a bucket aggregation (terms, multi_terms, etc.) to a {@link GroupingInfo}
 * for GROUP BY resolution, and converts results back to InternalAggregation for response building.
 */
public interface BucketTranslator<T extends AggregationBuilder> extends AggregationType<T> {

    /**
     * Determines how the {@link org.opensearch.dsl.aggregation.AggregationTreeWalker}
     * processes this bucket translator during tree traversal.
     */
    enum WalkStrategy {
        /** Standard bucket walk — adds grouping columns (e.g., terms). */
        STANDARD,
        /** Single filter walk — produces one plan with a filter condition (e.g., filter). */
        FILTER,
        /** Multi-filter walk — produces N plans, one per filter, plus optional other bucket (e.g., filters). */
        MULTI_FILTER
    }

    /**
     * Returns the walk strategy for this bucket translator.
     * Defaults to {@link WalkStrategy#STANDARD}.
     *
     * @return the walk strategy
     */
    default WalkStrategy getWalkStrategy() {
        return WalkStrategy.STANDARD;
    }

    /**
     * Returns the grouping contribution for this bucket.
     *
     * @param agg the bucket aggregation builder
     * @return the grouping info
     */
    GroupingInfo getGrouping(T agg);

    /**
     * Returns sub-aggregations to recurse into.
     *
     * @param agg the bucket aggregation builder
     * @return the sub-aggregations
     */
    Collection<AggregationBuilder> getSubAggregations(T agg);

    /**
     * Converts grouped bucket entries into an OpenSearch InternalAggregation for response building.
     *
     * @param agg the original aggregation builder
     * @param buckets the bucket entries with keys, doc counts, and sub-aggs
     * @return the InternalAggregation
     */
    InternalAggregation toBucketAggregation(T agg, List<BucketEntry> buckets);
}
