/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.opensearch.search.aggregations.InternalAggregations;

import java.util.List;

/**
 * A single bucket entry for response construction.
 *
 * @param keys the bucket key values (single for terms, multiple for multi_terms)
 * @param docCount the document count for this bucket
 * @param subAggs the sub-aggregation results for this bucket
 */
public record BucketEntry(List<Object> keys, long docCount, InternalAggregations subAggs) {
    /**
     * Creates a bucket entry.
     *
     * @param keys the bucket key values
     * @param docCount the document count
     * @param subAggs the sub-aggregation results
     */
    public BucketEntry {}

    /** Returns the bucket key values. */
    @Override
    public List<Object> keys() {
        return keys;
    }

    /** Returns the document count. */
    @Override
    public long docCount() {
        return docCount;
    }

    /** Returns the sub-aggregation results. */
    @Override
    public InternalAggregations subAggs() {
        return subAggs;
    }
}
