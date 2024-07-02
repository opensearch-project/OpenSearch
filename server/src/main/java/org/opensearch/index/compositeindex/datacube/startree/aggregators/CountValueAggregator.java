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

    @Override
    public MetricStat getAggregationType() {
        return MetricStat.COUNT;
    }

    @Override
    public StarTreeNumericType getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public Long getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue, StarTreeNumericType starTreeNumericType) {
        return 1L;
    }

    @Override
    public Long mergeAggregatedValueAndSegmentValue(Long value, Long segmentDocValue, StarTreeNumericType starTreeNumericType) {
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
    public Long toStarTreeNumericTypeValue(Long value, StarTreeNumericType type) {
        return value;
    }
}
