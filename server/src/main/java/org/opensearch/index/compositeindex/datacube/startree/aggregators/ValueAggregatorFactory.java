/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

/**
 * Value aggregator factory for a given aggregation type
 *
 * @opensearch.experimental
 */
public class ValueAggregatorFactory {
    private ValueAggregatorFactory() {}

    /**
     * Returns a new instance of value aggregator for the given aggregation type.
     *
     * @param aggregationType     Aggregation type
     * @param starTreeNumericType Numeric type associated with star tree field ( as specified in index mapping )
     * @return Value aggregator
     */
    public static ValueAggregator getValueAggregator(MetricStat aggregationType, StarTreeNumericType starTreeNumericType) {
        switch (aggregationType) {
            // other metric types (count, min, max, avg) will be supported in the future
            case SUM:
                return new SumValueAggregator(starTreeNumericType);
            case COUNT:
                return new CountValueAggregator(starTreeNumericType);
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
    public static StarTreeNumericType getAggregatedValueType(MetricStat aggregationType) {
        switch (aggregationType) {
            // other metric types (count, min, max, avg) will be supported in the future
            case SUM:
                return SumValueAggregator.VALUE_AGGREGATOR_TYPE;
            case COUNT:
                return CountValueAggregator.VALUE_AGGREGATOR_TYPE;
            default:
                throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
        }
    }
}
