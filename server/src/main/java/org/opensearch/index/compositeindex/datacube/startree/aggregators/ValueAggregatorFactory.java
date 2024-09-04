/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.mapper.FieldValueConverter;

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
     * @param fieldValueConverter Numeric type converter associated with star tree field ( as specified in index mapping )
     * @return Value aggregator
     */
    public static ValueAggregator getValueAggregator(MetricStat aggregationType, FieldValueConverter fieldValueConverter) {
        switch (aggregationType) {
            // avg aggregator will be covered in the part of query (using count and sum)
            case SUM:
                return new SumValueAggregator(fieldValueConverter);
            case VALUE_COUNT:
                return new CountValueAggregator();
            case MIN:
                return new MinValueAggregator(fieldValueConverter);
            case MAX:
                return new MaxValueAggregator(fieldValueConverter);
            case DOC_COUNT:
                return new DocCountAggregator();
            default:
                throw new IllegalStateException("Unsupported aggregation type: " + aggregationType);
        }
    }

}
