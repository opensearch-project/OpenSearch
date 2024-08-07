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
 * Count value aggregator for star tree
 *
 * @opensearch.experimental
 */
public class CountValueAggregator implements ValueAggregator<Long> {
    public static final StarTreeNumericType VALUE_AGGREGATOR_TYPE = StarTreeNumericType.LONG;
    public static final long DEFAULT_INITIAL_VALUE = 1L;
    private StarTreeNumericType starTreeNumericType;

    public CountValueAggregator(StarTreeNumericType starTreeNumericType) {
        this.starTreeNumericType = starTreeNumericType;
    }

    @Override
    public MetricStat getAggregationType() {
        return MetricStat.VALUE_COUNT;
    }

    @Override
    public StarTreeNumericType getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public Long getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue) {
        return DEFAULT_INITIAL_VALUE;
    }

    @Override
    public Long mergeAggregatedValueAndSegmentValue(Long value, Long segmentDocValue) {
        return value + 1;
    }

    @Override
    public Long mergeAggregatedValues(Long value, Long aggregatedValue) {
        return value + aggregatedValue;
    }

    @Override
    public Long getInitialAggregatedValue(Long value) {
        return value;
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Long.BYTES;
    }

    @Override
    public Long toLongValue(Long value) {
        return value;
    }

    @Override
    public Long toStarTreeNumericTypeValue(Long value) {
        return value;
    }

    @Override
    public Long getIdentityMetricValue() {
        return 0L;
    }
}
