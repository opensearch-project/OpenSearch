/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.mapper.FieldValueConverter;

public class CountValueAggregatorTests extends AbstractValueAggregatorTests {

    private CountValueAggregator aggregator;

    public CountValueAggregatorTests(FieldValueConverter fieldValueConverter) {
        super(fieldValueConverter);
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        long randomLong = randomLong();
        assertEquals(randomLong + 1, aggregator.mergeAggregatedValueAndSegmentValue(randomLong, 3L), 0.0);
    }

    public void testMergeAggregatedValues() {
        long randomLong1 = randomLong();
        long randomLong2 = randomLong();
        assertEquals(randomLong1 + randomLong2, aggregator.mergeAggregatedValues(randomLong1, randomLong2), 0.0);
        assertEquals(randomLong1, aggregator.mergeAggregatedValues(randomLong1, null), 0.0);
        assertEquals(randomLong2, aggregator.mergeAggregatedValues(null, randomLong2), 0.0);
    }

    @Override
    public void testMergeAggregatedNullValueAndSegmentNullValue() {
        assertThrows(AssertionError.class, () -> aggregator.mergeAggregatedValueAndSegmentValue(null, null));
    }

    public void testGetInitialAggregatedValue() {
        long randomLong = randomLong();
        assertEquals(randomLong, aggregator.getInitialAggregatedValue(randomLong), 0.0);
    }

    public void testToAggregatedValueType() {
        long randomLong = randomLong();
        assertEquals(randomLong, aggregator.toAggregatedValueType(randomLong), 0.0);
        assertNull(aggregator.toAggregatedValueType(null));
    }

    public void testIdentityMetricValue() {
        assertEquals(0L, aggregator.getIdentityMetricValue(), 0);
    }

    @Override
    public ValueAggregator getValueAggregator(FieldValueConverter fieldValueConverter) {
        aggregator = new CountValueAggregator();
        return aggregator;
    }

    @Override
    public void testGetInitialAggregatedValueForSegmentDocValue() {
        long randomLong = randomLong();
        assertEquals(CountValueAggregator.DEFAULT_INITIAL_VALUE, (long) aggregator.getInitialAggregatedValueForSegmentDocValue(randomLong));
    }
}
