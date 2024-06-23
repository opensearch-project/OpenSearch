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
import org.opensearch.index.compositeindex.datacube.startree.data.DataType;
import org.opensearch.search.aggregations.metrics.CompensatedSum;

/**
 * Sum value aggregator for star tree
 *
 * @opensearch.internal
 */
public class SumValueAggregator implements ValueAggregator<Double> {
    public static final DataType AGGREGATED_VALUE_TYPE = DataType.DOUBLE;

    @Override
    public MetricStat getAggregationType() {
        return MetricStat.SUM;
    }

    @Override
    public DataType getAggregatedValueType() {
        return AGGREGATED_VALUE_TYPE;
    }

    @Override
    public Double getInitialAggregatedValue(Long rawValue, StarTreeNumericType starTreeNumericType) {
        return starTreeNumericType.getDoubleValue(rawValue);
    }

    @Override
    public Double applySegmentRawValue(Double value, Long rawValue, StarTreeNumericType starTreeNumericType) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        kahanSummation.add(value);
        kahanSummation.add(starTreeNumericType.getDoubleValue(rawValue));
        return kahanSummation.value();
    }

    @Override
    public Double applyAggregatedValue(Double value, Double aggregatedValue) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        kahanSummation.add(value);
        kahanSummation.add(aggregatedValue);
        return kahanSummation.value();
    }

    @Override
    public Double getAggregatedValue(Double value) {
        return value;
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Double.BYTES;
    }

    @Override
    public Long convertAggregationTypeToSortableLongValue(Double value) {
        try {
            return NumericUtils.doubleToSortableLong(value);
        } catch (IllegalArgumentException | NullPointerException | IllegalStateException e) {
            throw new IllegalArgumentException("Cannot convert " + value + " to sortable long", e);
        }
    }

    @Override
    public Double convertSortableLongToAggregatedTypeValue(Long value, StarTreeNumericType type) {
        try {
            return type.getDoubleValue(value);
        } catch (IllegalArgumentException | NullPointerException | IllegalStateException e) {
            throw new IllegalArgumentException("Cannot convert " + value + " to sortable aggregation type", e);
        }
    }
}
