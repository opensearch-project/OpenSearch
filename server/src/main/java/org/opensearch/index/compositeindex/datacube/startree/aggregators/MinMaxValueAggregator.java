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
 * A value aggregator for star tree to perform min and max aggregations
 *
 * @opensearch.experimental
 */
public abstract class MinMaxValueAggregator implements ValueAggregator<Double> {

    private static final StarTreeNumericType VALUE_AGGREGATOR_TYPE = StarTreeNumericType.DOUBLE;
    protected final StarTreeNumericType starTreeNumericType;
    private final MetricStat metricStat;

    public MinMaxValueAggregator(MetricStat metricStat, StarTreeNumericType starTreeNumericType) {
        this.starTreeNumericType = starTreeNumericType;
        this.metricStat = metricStat;
    }

    @Override
    public MetricStat getAggregationType() {
        return metricStat;
    }

    @Override
    public StarTreeNumericType getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public Double getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue) {
        if (segmentDocValue == null) {
            return getIdentityMetricValue();
        }
        return starTreeNumericType.getDoubleValue(segmentDocValue);
    }

    @Override
    public Double mergeAggregatedValueAndSegmentValue(Double value, Long segmentDocValue) {
        if (segmentDocValue == null && value != null) {
            return value;
        } else if (segmentDocValue != null && value == null) {
            return starTreeNumericType.getDoubleValue(segmentDocValue);
        } else if (segmentDocValue == null) {
            return getIdentityMetricValue();
        }
        return performValueAggregation(value, starTreeNumericType.getDoubleValue(segmentDocValue));
    }

    @Override
    public Double mergeAggregatedValues(Double value, Double aggregatedValue) {
        if (value == null && aggregatedValue != null) {
            return aggregatedValue;
        } else if (value != null && aggregatedValue == null) {
            return value;
        } else if (value == null) {
            return getIdentityMetricValue();
        }
        return performValueAggregation(value, aggregatedValue);
    }

    @Override
    public Double getInitialAggregatedValue(Double value) {
        if (value == null) {
            return getIdentityMetricValue();
        }
        return value;
    }

    @Override
    public int getMaxAggregatedValueByteSize() {
        return Double.BYTES;
    }

    @Override
    public Long toLongValue(Double value) {
        try {
            if (value == null) {
                return null;
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
            return starTreeNumericType.getDoubleValue(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable aggregation type", e);
        }
    }

    @Override
    public Double getIdentityMetricValue() {
        // in present aggregations, if the metric behind min or max aggregation is missing, we treat it as null
        return null;
    }

    /**
     * Performs min or max aggregation on the value and the segmentDocValue based on the implementation
     *
     * @param aggregatedValue aggregated value for the segment so far
     * @param segmentDocValue current segment doc value
     * @return aggregated value
     */
    public abstract Double performValueAggregation(Double aggregatedValue, Double segmentDocValue);

}
