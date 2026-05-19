/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.startree.utils.CompensatedSumType;
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
class SumValueAggregator implements ValueAggregator<CompensatedSum> {

    private final FieldValueConverter fieldValueConverter;
    private final CompensatedSumType compensatedSumConverter;
    private static final FieldValueConverter VALUE_AGGREGATOR_TYPE = NumberFieldMapper.NumberType.DOUBLE;

    public SumValueAggregator(FieldValueConverter fieldValueConverter) {
        this.fieldValueConverter = fieldValueConverter;
        this.compensatedSumConverter = new CompensatedSumType();
    }

    @Override
    public FieldValueConverter getAggregatedValueType() {
        return compensatedSumConverter;
    }

    @Override
    public CompensatedSum getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        // add takes care of the sum and compensation internally
        if (segmentDocValue != null) {
            kahanSummation.reset(fieldValueConverter.toDoubleValue(segmentDocValue), 0);
        } else {
            kahanSummation.reset(getIdentityMetricDoubleValue(), 0);
        }
        return kahanSummation;
    }

    // we have overridden this method because the reset with sum and compensation helps us keep
    // track of precision and avoids a potential loss in accuracy of sums.
    @Override
    public CompensatedSum mergeAggregatedValueAndSegmentValue(CompensatedSum value, Long segmentDocValue) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        if (value != null) {
            kahanSummation.reset(value.value(), value.delta());
        }
        // add takes care of the sum and compensation internally
        if (segmentDocValue != null) {
            kahanSummation.add(fieldValueConverter.toDoubleValue(segmentDocValue));
        } else {
            kahanSummation.add(getIdentityMetricDoubleValue());
        }
        return kahanSummation;
    }

    @Override
    public CompensatedSum mergeAggregatedValues(CompensatedSum value, CompensatedSum aggregatedValue) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        if (aggregatedValue != null) {
            kahanSummation.reset(aggregatedValue.value(), aggregatedValue.delta());
        }
        if (value != null) {
            kahanSummation.add(value.value(), value.delta());
        } else {
            kahanSummation.add(getIdentityMetricDoubleValue());
        }
        return kahanSummation;
    }

    @Override
    public CompensatedSum getInitialAggregatedValue(CompensatedSum value) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        // add takes care of the sum and compensation internally
        if (value == null) {
            kahanSummation.reset(getIdentityMetricDoubleValue(), 0);
        } else {
            kahanSummation.reset(value.value(), value.delta());
        }
        return kahanSummation;
    }

    @Override
    public CompensatedSum toAggregatedValueType(Long value) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        try {
            if (value == null) {
                kahanSummation.reset(getIdentityMetricDoubleValue(), 0);
                return kahanSummation;
            }
            kahanSummation.reset(VALUE_AGGREGATOR_TYPE.toDoubleValue(value), 0);
            return kahanSummation;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable aggregation type", e);
        }
    }

    /**
     * Since getIdentityMetricValue is called for every null document, and it creates a new object,
     * in this class, calling getIdentityMetricDoubleValue to avoid initializing an object
     */
    private double getIdentityMetricDoubleValue() {
        return 0.0;
    }

    /**
     * Since getIdentityMetricValue is called for every null document, and it creates a new object,
     * in this class, calling getIdentityMetricDoubleValue to avoid initializing an object
     */
    @Override
    public CompensatedSum getIdentityMetricValue() {
        // in present aggregations, if the metric behind sum is missing, we treat it as 0
        return new CompensatedSum(0, 0);
    }
}
