/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.mapper.FieldValueConverter;

/**
 * A value aggregator that pre-aggregates on the input values for a specific type of aggregation.
 *
 * @opensearch.experimental
 */
public interface ValueAggregator<A> {

    /**
     * Returns the data type of the aggregated value.
     */
    FieldValueConverter getAggregatedValueType();

    /**
     * Returns the initial aggregated value.
     */
    A getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue);

    /**
     * Applies a segment doc value to the current aggregated value.
     */
    default A mergeAggregatedValueAndSegmentValue(A value, Long segmentDocValue) {
        A aggregatedValue = getInitialAggregatedValueForSegmentDocValue(segmentDocValue);
        return mergeAggregatedValues(value, aggregatedValue);
    }

    /**
     * Applies an aggregated value to the current aggregated value.
     */
    A mergeAggregatedValues(A value, A aggregatedValue);

    /**
     * Clones an aggregated value.
     */
    default A getInitialAggregatedValue(A value) {
        if (value == null) {
            return getIdentityMetricValue();
        }
        return value;
    }

    /**
     * Converts a segment long value to an aggregated value.
     */
    A toAggregatedValueType(Long rawValue);

    /**
     * Fetches a value that does not alter the result of aggregations
     */
    A getIdentityMetricValue();
}
