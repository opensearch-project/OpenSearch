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
import org.opensearch.search.aggregations.metrics.CompensatedSum;

/**
 * Sum value aggregator for star tree
 *
 * @opensearch.experimental
 */
public class SumValueAggregator implements ValueAggregator<Double> {

    public static final StarTreeNumericType VALUE_AGGREGATOR_TYPE = StarTreeNumericType.DOUBLE;
    private double sum = 0;
    private double compensation = 0;
    private CompensatedSum kahanSummation = new CompensatedSum(0, 0);

    private final StarTreeNumericType starTreeNumericType;

    public SumValueAggregator(StarTreeNumericType starTreeNumericType) {
        this.starTreeNumericType = starTreeNumericType;
    }

    @Override
    public MetricStat getAggregationType() {
        return MetricStat.SUM;
    }

    @Override
    public StarTreeNumericType getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public Double getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue) {
        kahanSummation.reset(0, 0);
        if (segmentDocValue != null) {
            kahanSummation.add(starTreeNumericType.getDoubleValue(segmentDocValue));
        } else {
            kahanSummation.add(getIdentityMetricValue());
        }
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
    }

    @Override
    public Double mergeAggregatedValueAndSegmentValue(Double value, Long segmentDocValue) {
        assert value == null || kahanSummation.value() == value;
        kahanSummation.reset(sum, compensation);
        if (segmentDocValue != null) {
            kahanSummation.add(starTreeNumericType.getDoubleValue(segmentDocValue));
        } else {
            kahanSummation.add(getIdentityMetricValue());
        }
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
    }

    @Override
    public Double mergeAggregatedValues(Double value, Double aggregatedValue) {
        assert aggregatedValue == null || kahanSummation.value() == aggregatedValue;
        kahanSummation.reset(sum, compensation);
        if (value != null) {
            kahanSummation.add(value);
        } else {
            kahanSummation.add(getIdentityMetricValue());
        }
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
    }

    @Override
    public Double getInitialAggregatedValue(Double value) {
        kahanSummation.reset(0, 0);
        if (value != null) {
            kahanSummation.add(value);
        } else {
            kahanSummation.add(getIdentityMetricValue());
        }
        compensation = kahanSummation.delta();
        sum = kahanSummation.value();
        return kahanSummation.value();
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Double.BYTES;
    }

    @Override
    public Long toLongValue(Double value) {
        try {
            if (value == null) {
                return 0L;
            }
            return NumericUtils.doubleToSortableLong(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable long", e);
        }
    }

    @Override
    public Double toStarTreeNumericTypeValue(Long value) {
        try {
            if (value == null) {
                return getIdentityMetricValue();
            }
            return VALUE_AGGREGATOR_TYPE.getDoubleValue(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable aggregation type", e);
        }
    }

    @Override
    public Double getIdentityMetricValue() {
        return 0D;
    }
}
