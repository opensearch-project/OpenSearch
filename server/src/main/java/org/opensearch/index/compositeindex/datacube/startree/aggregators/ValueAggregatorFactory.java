/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.data.DataType;

/**
 * Value aggregator factory for a given aggregation type
 * @opensearch.experimental
 */
public class ValueAggregatorFactory {
    private ValueAggregatorFactory() {}

    /**
     * Returns a new instance of value aggregator for the given aggregation type.
     *
     * @param aggregationType Aggregation type
     * @return Value aggregator
     */
    public static ValueAggregator getValueAggregator(MetricStat aggregationType) {
        switch (aggregationType) {
            // other metric types (count, min, max, avg) will be supported in the future
            case SUM:
                return new SumValueAggregator();
            default:
                throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
        }
    }

    /**
     * Returns the data type of the aggregated value for the given aggregation type.
     *
     * @param aggregationType Aggregation type
     * @return Data type of the aggregated value
     */
    public static DataType getAggregatedValueType(MetricStat aggregationType) {
        switch (aggregationType) {
            // other metric types (count, min, max, avg) will be supported in the future
            case SUM:
                return SumValueAggregator.AGGREGATED_VALUE_TYPE;
            default:
                throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
        }
    }
}
