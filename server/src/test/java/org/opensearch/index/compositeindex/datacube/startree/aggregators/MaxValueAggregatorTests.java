/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;

public class MaxValueAggregatorTests extends AbstractValueAggregatorTests {

    private MaxValueAggregator aggregator;

    public void testMergeAggregatedValueAndSegmentValue() {
        Long randomLong = randomLong();
        double randomDouble = randomDouble();
        assertEquals(
            Math.max(aggregator.toStarTreeNumericTypeValue(randomLong), randomDouble),
            aggregator.mergeAggregatedValueAndSegmentValue(randomDouble, randomLong),
            0.0
        );
        assertEquals(
            aggregator.toStarTreeNumericTypeValue(randomLong),
            aggregator.mergeAggregatedValueAndSegmentValue(null, randomLong),
            0.0
        );
        assertEquals(randomDouble, aggregator.mergeAggregatedValueAndSegmentValue(randomDouble, null), 0.0);
        assertEquals(
            Math.max(2.0, aggregator.toStarTreeNumericTypeValue(3L)),
            aggregator.mergeAggregatedValueAndSegmentValue(2.0, 3L),
            0.0
        );
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

    public void testGetMaxAggregatedValueByteSize() {
        assertEquals(Double.BYTES, aggregator.getMaxAggregatedValueByteSize());
    }

    public void testToLongValue() {
        double randomDouble = randomDouble();
        assertEquals(NumericUtils.doubleToSortableLong(randomDouble), aggregator.toLongValue(randomDouble), 0.0);
    }

    public void testToStarTreeNumericTypeValue() {
        MaxValueAggregator aggregator = new MaxValueAggregator(StarTreeNumericType.DOUBLE);
        long randomLong = randomLong();
        assertEquals(NumericUtils.sortableLongToDouble(randomLong), aggregator.toStarTreeNumericTypeValue(randomLong), 0.0);
    }

    public void testIdentityMetricValue() {
        assertNull(aggregator.getIdentityMetricValue());
    }

    @Override
    public ValueAggregator getValueAggregator(StarTreeNumericType starTreeNumericType) {
        aggregator = new MaxValueAggregator(starTreeNumericType);
        return aggregator;
    }

    @Override
    public MetricStat getMetricStat() {
        return MetricStat.MAX;
    }

    @Override
    public StarTreeNumericType getValueAggregatorType() {
        return aggregator.getAggregatedValueType();
    }
}
