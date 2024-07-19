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
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

public class SumValueAggregatorTests extends OpenSearchTestCase {

    private SumValueAggregator aggregator;

    @Before
    public void setup() {
        aggregator = new SumValueAggregator();
    }

    public void testGetAggregationType() {
        assertEquals(MetricStat.SUM.getTypeName(), aggregator.getAggregationType().getTypeName());
    }

    public void testGetAggregatedValueType() {
        assertEquals(SumValueAggregator.VALUE_AGGREGATOR_TYPE, aggregator.getAggregatedValueType());
    }

    public void testGetInitialAggregatedValueForSegmentDocValue() {
        assertEquals(1.0, aggregator.getInitialAggregatedValueForSegmentDocValue(1L, StarTreeNumericType.LONG), 0.0);
        assertThrows(
            NullPointerException.class,
            () -> aggregator.getInitialAggregatedValueForSegmentDocValue(null, StarTreeNumericType.DOUBLE)
        );
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        aggregator.getInitialAggregatedValue(2.0);
        assertEquals(5.0, aggregator.mergeAggregatedValueAndSegmentValue(2.0, 3L, StarTreeNumericType.LONG), 0.0);
    }

    public void testMergeAggregatedValueAndSegmentValue_nullSegmentDocValue() {
        aggregator.getInitialAggregatedValue(2.0);
        assertThrows(NullPointerException.class, () -> aggregator.mergeAggregatedValueAndSegmentValue(2.0, null, StarTreeNumericType.LONG));
    }

    public void testMergeAggregatedValues() {
        aggregator.getInitialAggregatedValue(3.0);
        assertEquals(5.0, aggregator.mergeAggregatedValues(2.0, 3.0), 0.0);
    }

    public void testGetInitialAggregatedValue() {
        assertEquals(3.14, aggregator.getInitialAggregatedValue(3.14), 0.0);
    }

    public void testGetMaxAggregatedValueByteSize() {
        assertEquals(Double.BYTES, aggregator.getMaxAggregatedValueByteSize());
    }

    public void testToLongValue() {
        assertEquals(NumericUtils.doubleToSortableLong(3.14), aggregator.toLongValue(3.14), 0.0);
    }

    public void testToStarTreeNumericTypeValue() {
        assertEquals(NumericUtils.sortableLongToDouble(3L), aggregator.toStarTreeNumericTypeValue(3L, StarTreeNumericType.DOUBLE), 0.0);
    }
}
