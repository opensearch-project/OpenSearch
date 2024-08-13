/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

public class SumValueAggregatorTests extends AbstractValueAggregatorTests {

    private SumValueAggregator aggregator;

    public SumValueAggregatorTests(StarTreeNumericType starTreeNumericType) {
        super(starTreeNumericType);
    }

    @Override
    public ValueAggregator getValueAggregator(StarTreeNumericType starTreeNumericType) {
        aggregator = new SumValueAggregator(starTreeNumericType);
        return aggregator;
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        double randomDouble = randomDouble();
        Long randomLong = randomLong();
        aggregator.getInitialAggregatedValue(randomDouble);
        assertEquals(
            randomDouble + aggregator.toStarTreeNumericTypeValue(randomLong),
            aggregator.mergeAggregatedValueAndSegmentValue(randomDouble, randomLong),
            0.0
        );
    }

    public void testMergeAggregatedValueAndSegmentValue_nullSegmentDocValue() {
        double randomDouble1 = randomDouble();
        Long randomLong = randomLong();
        aggregator.getInitialAggregatedValue(randomDouble1);
        assertEquals(randomDouble1, aggregator.mergeAggregatedValueAndSegmentValue(randomDouble1, null), 0.0);
        assertEquals(
            randomDouble1 + aggregator.toStarTreeNumericTypeValue(randomLong),
            aggregator.mergeAggregatedValueAndSegmentValue(randomDouble1, randomLong),
            0.0
        );
    }

    public void testMergeAggregatedValueAndSegmentValue_nullInitialDocValue() {
        Long randomLong = randomLong();
        aggregator.getInitialAggregatedValue(null);
        assertEquals(
            aggregator.toStarTreeNumericTypeValue(randomLong),
            aggregator.mergeAggregatedValueAndSegmentValue(null, randomLong),
            0.0
        );
    }

    public void testMergeAggregatedValues() {
        double randomDouble1 = randomDouble();
        double randomDouble2 = randomDouble();
        aggregator.getInitialAggregatedValue(randomDouble1);
        assertEquals(randomDouble1, aggregator.mergeAggregatedValues(null, randomDouble1), 0.0);
        assertEquals(randomDouble1 + randomDouble2, aggregator.mergeAggregatedValues(randomDouble2, randomDouble1), 0.0);
    }

    public void testGetInitialAggregatedValue() {
        double randomDouble = randomDouble();
        assertEquals(randomDouble, aggregator.getInitialAggregatedValue(randomDouble), 0.0);
    }

    public void testToStarTreeNumericTypeValue() {
        long randomLong = randomLong();
        assertEquals(aggregator.toStarTreeNumericTypeValue(randomLong), aggregator.toStarTreeNumericTypeValue(randomLong), 0.0);
    }

    public void testIdentityMetricValue() {
        assertEquals(0.0, aggregator.getIdentityMetricValue(), 0);
    }

}
