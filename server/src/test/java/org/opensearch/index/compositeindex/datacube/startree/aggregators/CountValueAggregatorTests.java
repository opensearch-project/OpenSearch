/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

public class CountValueAggregatorTests extends AbstractValueAggregatorTests {

    private CountValueAggregator aggregator;

    public CountValueAggregatorTests(StarTreeNumericType starTreeNumericType) {
        super(starTreeNumericType);
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

    public void testGetInitialAggregatedValue() {
        long randomLong = randomLong();
        assertEquals(randomLong, aggregator.getInitialAggregatedValue(randomLong), 0.0);
    }

    public void testToStarTreeNumericTypeValue() {
        long randomLong = randomLong();
        assertEquals(randomLong, aggregator.toStarTreeNumericTypeValue(randomLong), 0.0);
        assertNull(aggregator.toStarTreeNumericTypeValue(null));
    }

    public void testIdentityMetricValue() {
        assertEquals(0L, aggregator.getIdentityMetricValue(), 0);
    }

    @Override
    public ValueAggregator getValueAggregator(StarTreeNumericType starTreeNumericType) {
        aggregator = new CountValueAggregator(starTreeNumericType);
        return aggregator;
    }

}
