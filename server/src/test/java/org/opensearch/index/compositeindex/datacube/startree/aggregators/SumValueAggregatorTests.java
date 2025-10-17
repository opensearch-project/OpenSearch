/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.mapper.FieldValueConverter;
import org.opensearch.search.aggregations.metrics.CompensatedSum;

public class SumValueAggregatorTests extends AbstractValueAggregatorTests {

    private SumValueAggregator aggregator;

    public SumValueAggregatorTests(FieldValueConverter fieldValueConverter) {
        super(fieldValueConverter);
    }

    @Override
    public ValueAggregator getValueAggregator(FieldValueConverter fieldValueConverter) {
        aggregator = new SumValueAggregator(fieldValueConverter);
        return aggregator;
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        double randomDouble = randomDouble();
        Long randomLong = randomLong();
        CompensatedSum initial = aggregator.getInitialAggregatedValue(new CompensatedSum(randomDouble, 0));
        CompensatedSum result = aggregator.mergeAggregatedValueAndSegmentValue(initial, randomLong);
        assertEquals(randomDouble + fieldValueConverter.toDoubleValue(randomLong), result.value(), 0.0);
    }

    public void testMergeAggregatedValueAndSegmentValue_nullSegmentDocValue() {
        double randomDouble1 = randomDouble();
        Long randomLong = randomLong();
        CompensatedSum aggregatedValue = aggregator.getInitialAggregatedValue(new CompensatedSum(randomDouble1, 0));

        CompensatedSum resultWithNull = aggregator.mergeAggregatedValueAndSegmentValue(aggregatedValue, null);
        assertEquals(randomDouble1, resultWithNull.value(), 0.0);

        CompensatedSum resultWithValue = aggregator.mergeAggregatedValueAndSegmentValue(aggregatedValue, randomLong);
        assertEquals(randomDouble1 + fieldValueConverter.toDoubleValue(randomLong), resultWithValue.value(), 0.0);
    }

    public void testMergeAggregatedValueAndSegmentValue_nullInitialDocValue() {
        Long randomLong = randomLong();
        CompensatedSum result = aggregator.mergeAggregatedValueAndSegmentValue(null, randomLong);
        assertEquals(fieldValueConverter.toDoubleValue(randomLong), result.value(), 0.0);
    }

    public void testMergeAggregatedValues() {
        double randomDouble1 = randomDouble();
        double randomDouble2 = randomDouble();
        CompensatedSum value1 = new CompensatedSum(randomDouble1, 0);
        CompensatedSum value2 = new CompensatedSum(randomDouble2, 0);

        CompensatedSum resultWithNull = aggregator.mergeAggregatedValues(null, value1);
        assertEquals(randomDouble1, resultWithNull.value(), 0.0);

        CompensatedSum result = aggregator.mergeAggregatedValues(value2, value1);
        assertEquals(randomDouble1 + randomDouble2, result.value(), 0.0);
    }

    public void testGetInitialAggregatedValue() {
        double randomDouble = randomDouble();
        CompensatedSum value = new CompensatedSum(randomDouble, 0);
        CompensatedSum result = aggregator.getInitialAggregatedValue(value);
        assertEquals(randomDouble, result.value(), 0.0);

        CompensatedSum result1 = aggregator.getInitialAggregatedValue(null);
        assertEquals(0.0, result1.value(), 0.0);
    }

    public void testToAggregatedValueType() {
        long randomLong = randomLong();
        CompensatedSum result1 = aggregator.toAggregatedValueType(randomLong);
        CompensatedSum result2 = aggregator.toAggregatedValueType(randomLong);
        assertEquals(result1.value(), result2.value(), 0.0);
        CompensatedSum result3 = aggregator.toAggregatedValueType(null);
        assertEquals(0.0, result3.value(), 0.0);
    }

    public void testIdentityMetricValue() {
        CompensatedSum identity = aggregator.getIdentityMetricValue();
        assertEquals(0.0, identity.value(), 0);
    }
}
