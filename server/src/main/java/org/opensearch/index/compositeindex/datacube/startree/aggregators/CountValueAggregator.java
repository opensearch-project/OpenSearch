/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

/**
 * Count value aggregator for star tree
 *
 * @opensearch.experimental
 */
class CountValueAggregator implements ValueAggregator<Long> {

    public static final long DEFAULT_INITIAL_VALUE = 1L;
    private final StarTreeNumericType starTreeNumericType;
    private static final StarTreeNumericType VALUE_AGGREGATOR_TYPE = StarTreeNumericType.LONG;

    public CountValueAggregator(StarTreeNumericType starTreeNumericType) {
        this.starTreeNumericType = starTreeNumericType;
    }

    @Override
    public StarTreeNumericType getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public Long getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue) {

        if (segmentDocValue == null) {
            return getIdentityMetricValue();
        }

        return DEFAULT_INITIAL_VALUE;
    }

    @Override
    public Long mergeAggregatedValueAndSegmentValue(Long value, Long segmentDocValue) {
        assert value != null;
        if (segmentDocValue != null) {
            return value + 1;
        }
        return value;
    }

    @Override
    public Long mergeAggregatedValues(Long value, Long aggregatedValue) {
        if (value == null) {
            value = getIdentityMetricValue();
        }
        if (aggregatedValue == null) {
            aggregatedValue = getIdentityMetricValue();
        }
        return value + aggregatedValue;
    }

    @Override
    public Long toAggregatedValueType(Long value) {
        return value;
    }

    @Override
    public Long getIdentityMetricValue() {
        // in present aggregations, if the metric behind count is missing, we treat it as 0
        return 0L;
    }
}
