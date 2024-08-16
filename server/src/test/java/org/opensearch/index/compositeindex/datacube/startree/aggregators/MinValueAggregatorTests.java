/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeTestUtils;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

public class MinValueAggregatorTests extends AbstractValueAggregatorTests {
    private MinValueAggregator aggregator;

    public MinValueAggregatorTests(StarTreeNumericType starTreeNumericType) {
        super(starTreeNumericType);
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        Long randomLong = randomLong();
        double randomDouble = randomDouble();
        assertEquals(
            Math.min(StarTreeTestUtils.toStarTreeNumericTypeValue(randomLong, starTreeNumericType), randomDouble),
            aggregator.mergeAggregatedValueAndSegmentValue(randomDouble, randomLong),
            0.0
        );
        assertEquals(
            StarTreeTestUtils.toStarTreeNumericTypeValue(randomLong, starTreeNumericType),
            aggregator.mergeAggregatedValueAndSegmentValue(null, randomLong),
            0.0
        );
        assertEquals(randomDouble, aggregator.mergeAggregatedValueAndSegmentValue(randomDouble, null), 0.0);
        assertEquals(
            Math.min(2.0, StarTreeTestUtils.toStarTreeNumericTypeValue(3L, starTreeNumericType)),
            aggregator.mergeAggregatedValueAndSegmentValue(2.0, 3L),
            0.0
        );
    }

    public void testMergeAggregatedValues() {
        double randomDouble = randomDouble();
        assertEquals(randomDouble, aggregator.mergeAggregatedValues(Double.MAX_VALUE, randomDouble), 0.0);
        assertEquals(randomDouble, aggregator.mergeAggregatedValues(null, randomDouble), 0.0);
        assertEquals(randomDouble, aggregator.mergeAggregatedValues(randomDouble, null), 0.0);
        assertEquals(2.0, aggregator.mergeAggregatedValues(2.0, 3.0), 0.0);
    }

    public void testGetInitialAggregatedValue() {
        double randomDouble = randomDouble();
        assertEquals(randomDouble, aggregator.getInitialAggregatedValue(randomDouble), 0.0);
    }

    public void testToAggregatedValueType() {
        MinValueAggregator aggregator = new MinValueAggregator(StarTreeNumericType.DOUBLE);
        long randomLong = randomLong();
        assertEquals(NumericUtils.sortableLongToDouble(randomLong), aggregator.toAggregatedValueType(randomLong), 0.0);
    }

    public void testIdentityMetricValue() {
        assertNull(aggregator.getIdentityMetricValue());
    }

    @Override
    public ValueAggregator getValueAggregator(StarTreeNumericType starTreeNumericType) {
        aggregator = new MinValueAggregator(starTreeNumericType);
        return aggregator;
    }

}
