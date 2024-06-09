/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.startree.aggregators;

import org.opensearch.index.compositeindex.MetricType;

/**
 * A value aggregator that pre-aggregates on the input values for a specific type of aggregation.
 * @opensearch.experimental
 */
public interface ValueAggregator<R, A> {

    /**
     * Returns the type of the aggregation.
     */
    MetricType getAggregationType();

    /**
     * Returns the data type of the aggregated value.
     */
    DataType getAggregatedValueType();

    /**
     * Returns the initial aggregated value.
     */
    A getInitialAggregatedValue(R rawValue);

    /**
     * Applies a raw value to the current aggregated value.
     */
    A applyRawValue(A value, R rawValue);

    /**
     * Applies an aggregated value to the current aggregated value.
     */
    A applyAggregatedValue(A value, A aggregatedValue);

    /**
     * Clones an aggregated value.
     */
    A cloneAggregatedValue(A value);

    /**
     * Returns the maximum size in bytes of the aggregated values seen so far.
     */
    int getMaxAggregatedValueByteSize();

    /**
     * Serializes an aggregated value into a byte array.
     */
    byte[] serializeAggregatedValue(A value);

    /**
     * De-serializes an aggregated value from a byte array.
     */
    A deserializeAggregatedValue(byte[] bytes);
}
