/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

/**
 * Max value aggregator for star tree
 *
 * @opensearch.experimental
 */
public class MaxValueAggregator implements ValueAggregator<Double> {

    public static final StarTreeNumericType VALUE_AGGREGATOR_TYPE = StarTreeNumericType.DOUBLE;

    @Override
    public MetricStat getAggregationType() {
        return MetricStat.MAX;
    }

    @Override
    public StarTreeNumericType getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public Double getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue, StarTreeNumericType starTreeNumericType) {
        return starTreeNumericType.getDoubleValue(segmentDocValue);
    }

    @Override
    public Double mergeAggregatedValueAndSegmentValue(Double value, Long segmentDocValue, StarTreeNumericType starTreeNumericType) {
        return Math.max(value, starTreeNumericType.getDoubleValue(segmentDocValue));
    }

    @Override
    public Double mergeAggregatedValues(Double value, Double aggregatedValue) {
        return Math.max(value, aggregatedValue);
    }

    @Override
    public Double getInitialAggregatedValue(Double value) {
        return value;
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Double.BYTES;
    }

    @Override
    public Long toLongValue(Double value) {
        try {
            return NumericUtils.doubleToSortableLong(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable long", e);
        }
    }

    @Override
    public Double toStarTreeNumericTypeValue(Long value, StarTreeNumericType type) {
        try {
            return type.getDoubleValue(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable aggregation type", e);
        }
    }
}
