/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.mapper.FieldValueConverter;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.metrics.CompensatedSum;

/**
 * Sum value aggregator for star tree
 *
 * <p>This implementation follows the Kahan summation algorithm to improve the accuracy
 * of the sum by tracking and compensating for the accumulated error in each iteration.
 *
 * @see <a href="http://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan Summation Algorithm</a>
 *
 * @opensearch.experimental
 */
class SumValueAggregator implements ValueAggregator<Double> {

    private final FieldValueConverter fieldValueConverter;
    private static final FieldValueConverter VALUE_AGGREGATOR_TYPE = NumberFieldMapper.NumberType.DOUBLE;

    private CompensatedSum kahanSummation = new CompensatedSum(0, 0);

    public SumValueAggregator(FieldValueConverter fieldValueConverter) {
        this.fieldValueConverter = fieldValueConverter;
    }

    @Override
    public FieldValueConverter getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public Double getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue) {
        kahanSummation.reset(0, 0);
        // add takes care of the sum and compensation internally
        if (segmentDocValue != null) {
            kahanSummation.add(fieldValueConverter.toDoubleValue(segmentDocValue));
        } else {
            kahanSummation.add(getIdentityMetricValue());
        }
        return kahanSummation.value();
    }

    // we have overridden this method because the reset with sum and compensation helps us keep
    // track of precision and avoids a potential loss in accuracy of sums.
    @Override
    public Double mergeAggregatedValueAndSegmentValue(Double value, Long segmentDocValue) {
        assert value == null || kahanSummation.value() == value;
        // add takes care of the sum and compensation internally
        if (segmentDocValue != null) {
            kahanSummation.add(fieldValueConverter.toDoubleValue(segmentDocValue));
        } else {
            kahanSummation.add(getIdentityMetricValue());
        }
        return kahanSummation.value();
    }

    @Override
    public Double mergeAggregatedValues(Double value, Double aggregatedValue) {
        assert aggregatedValue == null || kahanSummation.value() == aggregatedValue;
        // add takes care of the sum and compensation internally
        if (value != null) {
            kahanSummation.add(value);
        } else {
            kahanSummation.add(getIdentityMetricValue());
        }
        return kahanSummation.value();
    }

    @Override
    public Double getInitialAggregatedValue(Double value) {
        kahanSummation.reset(0, 0);
        // add takes care of the sum and compensation internally
        if (value != null) {
            kahanSummation.add(value);
        } else {
            kahanSummation.add(getIdentityMetricValue());
        }
        return kahanSummation.value();
    }

    @Override
    public Double toAggregatedValueType(Long value) {
        try {
            if (value == null) {
                return getIdentityMetricValue();
            }
            return VALUE_AGGREGATOR_TYPE.toDoubleValue(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable aggregation type", e);
        }
    }

    @Override
    public Double getIdentityMetricValue() {
        // in present aggregations, if the metric behind sum is missing, we treat it as 0
        return 0D;
    }
}
