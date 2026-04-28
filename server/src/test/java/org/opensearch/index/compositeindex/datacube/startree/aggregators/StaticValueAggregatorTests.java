/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.metrics.CompensatedSum;
import org.opensearch.test.OpenSearchTestCase;

public class StaticValueAggregatorTests extends OpenSearchTestCase {

    // tests the extreme case where normal sum will lose precision
    public void testKahanSummation() {
        double[] numbers = { 1e-16, 1, -1e-16 };
        double expected = 1;

        // initializing our sum aggregator to derive exact sum using kahan summation
        CompensatedSum aggregatedSum = getAggregatedValue(numbers);
        assertEquals(expected, aggregatedSum.value(), 0);

        // assert kahan summation plain logic with our aggregated value
        double actual = kahanSum(numbers);
        assertEquals(actual, aggregatedSum.value(), 0);

        // assert that normal sum fails for this case
        double normalSum = normalSum(numbers);
        assertNotEquals(expected, normalSum, 0);
        assertNotEquals(actual, normalSum, 0);
        assertNotEquals(aggregatedSum.value(), normalSum, 0);
    }

    private static CompensatedSum getAggregatedValue(double[] numbers) {
        SumValueAggregator aggregator = new SumValueAggregator(NumberFieldMapper.NumberType.DOUBLE);
        long sortableLong1 = NumericUtils.doubleToSortableLong(numbers[0]);
        CompensatedSum aggregatedValue = aggregator.getInitialAggregatedValueForSegmentDocValue(sortableLong1);
        long sortableLong2 = NumericUtils.doubleToSortableLong(numbers[1]);
        aggregatedValue = aggregator.mergeAggregatedValueAndSegmentValue(aggregatedValue, sortableLong2);
        long sortableLong3 = NumericUtils.doubleToSortableLong(numbers[2]);
        aggregatedValue = aggregator.mergeAggregatedValueAndSegmentValue(aggregatedValue, sortableLong3);
        return aggregatedValue;
    }

    private double kahanSum(double[] numbers) {
        CompensatedSum compensatedSum = new CompensatedSum(0, 0);
        for (double num : numbers) {
            compensatedSum.add(num);
        }
        return compensatedSum.value();
    }

    private double normalSum(double[] numbers) {
        double sum = 0.0;
        for (double num : numbers) {
            sum += num;
        }
        return sum;
    }

    public void testMaxAggregatorExtremeValues() {
        double[] numbers = { Double.MAX_VALUE, Double.MIN_VALUE, 0.0, Double.MAX_VALUE + 1 };
        double expected = Double.MAX_VALUE + 1;
        MaxValueAggregator aggregator = new MaxValueAggregator(NumberFieldMapper.NumberType.DOUBLE);
        double aggregatedValue = aggregator.getInitialAggregatedValueForSegmentDocValue(NumericUtils.doubleToSortableLong(numbers[0]));
        for (int i = 1; i < numbers.length; i++) {
            aggregatedValue = aggregator.mergeAggregatedValueAndSegmentValue(
                aggregatedValue,
                NumericUtils.doubleToSortableLong(numbers[i])
            );
        }
        assertEquals(expected, aggregatedValue, 0);
    }

    public void testMaxAggregatorExtremeValues_Infinity() {
        double[] numbers = {
            Double.MAX_VALUE,
            Double.MIN_VALUE,
            0.0,
            Double.MAX_VALUE + 1,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY };
        double expected = Double.POSITIVE_INFINITY;
        MaxValueAggregator aggregator = new MaxValueAggregator(NumberFieldMapper.NumberType.DOUBLE);
        double aggregatedValue = aggregator.getInitialAggregatedValueForSegmentDocValue(NumericUtils.doubleToSortableLong(numbers[0]));
        for (int i = 1; i < numbers.length; i++) {
            aggregatedValue = aggregator.mergeAggregatedValueAndSegmentValue(
                aggregatedValue,
                NumericUtils.doubleToSortableLong(numbers[i])
            );
        }
        assertEquals(expected, aggregatedValue, 0);
    }

    public void testMinAggregatorExtremeValues() {
        double[] numbers = { Double.MAX_VALUE, Double.MIN_VALUE - 1, 0.0, Double.MAX_VALUE + 1 };
        double expected = Double.MIN_VALUE - 1;
        MinValueAggregator aggregator = new MinValueAggregator(NumberFieldMapper.NumberType.DOUBLE);
        double aggregatedValue = aggregator.getInitialAggregatedValueForSegmentDocValue(NumericUtils.doubleToSortableLong(numbers[0]));
        for (int i = 1; i < numbers.length; i++) {
            aggregatedValue = aggregator.mergeAggregatedValueAndSegmentValue(
                aggregatedValue,
                NumericUtils.doubleToSortableLong(numbers[i])
            );
        }
        assertEquals(expected, aggregatedValue, 0);
    }

    public void testMinAggregatorExtremeValues_Infinity() {
        double[] numbers = {
            Double.MAX_VALUE,
            Double.MIN_VALUE,
            0.0,
            Double.MAX_VALUE + 1,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY };
        double expected = Double.NEGATIVE_INFINITY;
        MinValueAggregator aggregator = new MinValueAggregator(NumberFieldMapper.NumberType.DOUBLE);
        double aggregatedValue = aggregator.getInitialAggregatedValueForSegmentDocValue(NumericUtils.doubleToSortableLong(numbers[0]));
        for (int i = 1; i < numbers.length; i++) {
            aggregatedValue = aggregator.mergeAggregatedValueAndSegmentValue(
                aggregatedValue,
                NumericUtils.doubleToSortableLong(numbers[i])
            );
        }
        assertEquals(expected, aggregatedValue, 0);
    }
}
