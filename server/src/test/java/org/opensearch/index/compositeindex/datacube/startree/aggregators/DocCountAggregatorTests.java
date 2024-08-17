/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

/**
 * Unit tests for {@link DocCountAggregator}.
 */
public class DocCountAggregatorTests extends AbstractValueAggregatorTests {

    private DocCountAggregator aggregator;

    public DocCountAggregatorTests(StarTreeNumericType starTreeNumericType) {
        super(starTreeNumericType);
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        long randomLong = randomLong();
        assertEquals(randomLong + 3L, (long) aggregator.mergeAggregatedValueAndSegmentValue(randomLong, 3L));
    }

    public void testMergeAggregatedValues() {
        long randomLong1 = randomLong();
        long randomLong2 = randomLong();
        assertEquals(randomLong1 + randomLong2, (long) aggregator.mergeAggregatedValues(randomLong1, randomLong2));
        assertEquals(randomLong1 + 1L, (long) aggregator.mergeAggregatedValues(randomLong1, null));
        assertEquals(randomLong2 + 1L, (long) aggregator.mergeAggregatedValues(null, randomLong2));
    }

    @Override
    public void testMergeAggregatedNullValueAndSegmentNullValue() {
        assertThrows(AssertionError.class, () -> aggregator.mergeAggregatedValueAndSegmentValue(null, null));
    }

    @Override
    public void testMergeAggregatedNullValues() {
        assertEquals(
            (aggregator.getIdentityMetricValue() + aggregator.getIdentityMetricValue()),
            (long) aggregator.mergeAggregatedValues(null, null)
        );
    }

    public void testGetInitialAggregatedValue() {
        long randomLong = randomLong();
        assertEquals(randomLong, (long) aggregator.getInitialAggregatedValue(randomLong));
    }

    public void testToStarTreeNumericTypeValue() {
        long randomLong = randomLong();
        assertEquals(randomLong, aggregator.toStarTreeNumericTypeValue(randomLong), 0.0);
        assertNull(aggregator.toStarTreeNumericTypeValue(null));
    }

    public void testIdentityMetricValue() {
        assertEquals(1L, (long) aggregator.getIdentityMetricValue());
    }

    @Override
    public ValueAggregator getValueAggregator(StarTreeNumericType starTreeNumericType) {
        aggregator = new DocCountAggregator();
        return aggregator;
    }
}
