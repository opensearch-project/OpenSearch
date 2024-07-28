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

public class SumValueAggregatorTests extends AbstractValueAggregatorTests {

    private SumValueAggregator aggregator;

    @Override
    public ValueAggregator getValueAggregator() {
        aggregator = new SumValueAggregator(StarTreeNumericType.LONG);
        return aggregator;
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        double randomDouble = randomDouble();
        Long randomLong = randomLong();
        aggregator.getInitialAggregatedValue(randomDouble);
        assertEquals(
            randomDouble + randomLong.doubleValue(),
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
            randomDouble1 + randomLong.doubleValue(),
            aggregator.mergeAggregatedValueAndSegmentValue(randomDouble1, randomLong),
            0.0
        );
    }

    public void testMergeAggregatedValueAndSegmentValue_nullInitialDocValue() {
        Long randomLong = randomLong();
        aggregator.getInitialAggregatedValue(null);
        assertEquals(randomLong.doubleValue(), aggregator.mergeAggregatedValueAndSegmentValue(null, randomLong), 0.0);
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

    public void testGetMaxAggregatedValueByteSize() {
        assertEquals(Double.BYTES, aggregator.getMaxAggregatedValueByteSize());
    }

    public void testToLongValue() {
        double randomDouble = randomDouble();
        assertEquals(NumericUtils.doubleToSortableLong(randomDouble), aggregator.toLongValue(randomDouble), 0.0);
    }

    public void testToStarTreeNumericTypeValue() {
        long randomLong = randomLong();
        assertEquals(NumericUtils.sortableLongToDouble(randomLong), aggregator.toStarTreeNumericTypeValue(randomLong), 0.0);
    }

    public void testIdentityMetricValue() {
        assertEquals(0.0, aggregator.getIdentityMetricValue(), 0);
    }

    @Override
    public MetricStat getMetricStat() {
        return MetricStat.SUM;
    }

    @Override
    public StarTreeNumericType getValueAggregatorType() {
        return StarTreeNumericType.DOUBLE;
    }
}
