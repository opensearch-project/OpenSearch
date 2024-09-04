/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.mapper.FieldValueConverter;
import org.opensearch.index.mapper.NumberFieldMapper;

public class MaxValueAggregatorTests extends AbstractValueAggregatorTests {

    private MaxValueAggregator aggregator;

    public MaxValueAggregatorTests(FieldValueConverter fieldValueConverter) {
        super(fieldValueConverter);
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        Long randomLong = randomLong();
        double randomDouble = randomDouble();
        assertEquals(
            Math.max(fieldValueConverter.toDoubleValue(randomLong), randomDouble),
            aggregator.mergeAggregatedValueAndSegmentValue(randomDouble, randomLong),
            0.0
        );
        assertEquals(fieldValueConverter.toDoubleValue(randomLong), aggregator.mergeAggregatedValueAndSegmentValue(null, randomLong), 0.0);
        assertEquals(randomDouble, aggregator.mergeAggregatedValueAndSegmentValue(randomDouble, null), 0.0);
        assertEquals(Math.max(2.0, fieldValueConverter.toDoubleValue(3L)), aggregator.mergeAggregatedValueAndSegmentValue(2.0, 3L), 0.0);
    }

    public void testMergeAggregatedValues() {
        double randomDouble = randomDouble();
        assertEquals(randomDouble, aggregator.mergeAggregatedValues(Double.MIN_VALUE, randomDouble), 0.0);
        assertEquals(randomDouble, aggregator.mergeAggregatedValues(null, randomDouble), 0.0);
        assertEquals(randomDouble, aggregator.mergeAggregatedValues(randomDouble, null), 0.0);
        assertEquals(3.0, aggregator.mergeAggregatedValues(2.0, 3.0), 0.0);
    }

    public void testGetInitialAggregatedValue() {
        double randomDouble = randomDouble();
        assertEquals(randomDouble, aggregator.getInitialAggregatedValue(randomDouble), 0.0);
    }

    public void testToAggregatedValueType() {
        MaxValueAggregator aggregator = new MaxValueAggregator(NumberFieldMapper.NumberType.DOUBLE);
        long randomLong = randomLong();
        assertEquals(NumericUtils.sortableLongToDouble(randomLong), aggregator.toAggregatedValueType(randomLong), 0.0);
    }

    public void testIdentityMetricValue() {
        assertNull(aggregator.getIdentityMetricValue());
    }

    @Override
    public ValueAggregator getValueAggregator(FieldValueConverter fieldValueConverter) {
        aggregator = new MaxValueAggregator(fieldValueConverter);
        return aggregator;
    }

}
