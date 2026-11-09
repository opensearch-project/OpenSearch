/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation.bucket;

import org.opensearch.dsl.result.BucketEntry;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

/**
 * A {@link BucketTranslator} for bucket types with top-K semantics: the response carries the
 * top {@code size} buckets by the requested order, and the remaining documents are reported as
 * {@code sum_other_doc_count}. Terms and its relatives fit this contract; types that return
 * their full bucket set (histogram, range, filters) do not implement it.
 *
 * <p>Plans for these aggregations enforce {@code size}, {@code min_doc_count}, and order
 * engine-side (LIMIT, HAVING, SORT), so the engine returns exactly the response's bucket set.
 * Rendering goes through
 * {@link #toBucketAggregation(AggregationBuilder, Iterable, long)}, which receives that set
 * already filtered, ordered, and truncated, plus the eligible-document total that
 * {@code sum_other_doc_count} is derived from.
 */
public interface SizedBucketTranslator<T extends AggregationBuilder> extends BucketTranslator<T> {

    /**
     * Returns the requested bucket count (the {@code size} parameter).
     *
     * @param agg the bucket aggregation builder
     * @return the requested size
     */
    int size(T agg);

    /**
     * Returns the minimum per-bucket doc count (the {@code min_doc_count} parameter).
     * Values above 1 become a plan-level HAVING filter.
     *
     * @param agg the bucket aggregation builder
     * @return the minimum doc count
     */
    long minDocCount(T agg);

    /**
     * Renders a level whose plan enforced {@code size}. The entries are the final bucket set —
     * already filtered (nulls, {@code min_doc_count}), ordered, and truncated by the plan —
     * and are rendered as received.
     *
     * @param agg the original aggregation builder
     * @param buckets the top bucket entries
     * @param eligibleDocCount total documents eligible for this level's buckets;
     *        {@code sum_other_doc_count} is this value minus the received buckets' doc counts
     * @return the InternalAggregation
     */
    InternalAggregation toBucketAggregation(T agg, Iterable<BucketEntry> buckets, long eligibleDocCount);
}
