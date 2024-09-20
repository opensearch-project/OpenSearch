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

/**
 * This is an abstract class that defines the common methods for all double value aggregators
 * It is stateless.
 *
 * @opensearch.experimental
 */
abstract class StatelessDoubleValueAggregator implements ValueAggregator<Double> {

    protected final FieldValueConverter fieldValueConverter;
    protected final Double identityValue;
    private static final FieldValueConverter VALUE_AGGREGATOR_TYPE = NumberFieldMapper.NumberType.DOUBLE;

    public StatelessDoubleValueAggregator(FieldValueConverter fieldValueConverter, Double identityValue) {
        this.fieldValueConverter = fieldValueConverter;
        this.identityValue = identityValue;
    }

    @Override
    public FieldValueConverter getAggregatedValueType() {
        return VALUE_AGGREGATOR_TYPE;
    }

    @Override
    public Double getInitialAggregatedValueForSegmentDocValue(Long segmentDocValue) {
        if (segmentDocValue == null) {
            return getIdentityMetricValue();
        }
        return fieldValueConverter.toDoubleValue(segmentDocValue);
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
        // the identity value that we return should be inline with the existing aggregations
        return identityValue;
    }

    /**
     * Performs stateless aggregation on the value and the segmentDocValue based on the implementation
     *
     * @param aggregatedValue aggregated value for the segment so far
     * @param segmentDocValue current segment doc value
     * @return aggregated value
     */
    protected abstract Double performValueAggregation(Double aggregatedValue, Double segmentDocValue);

}
