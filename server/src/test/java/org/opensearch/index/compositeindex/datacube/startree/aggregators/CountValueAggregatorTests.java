/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.test.OpenSearchTestCase;

public class CountValueAggregatorTests extends OpenSearchTestCase {
    private final CountValueAggregator aggregator = new CountValueAggregator();

    public void testGetAggregationType() {
        assertEquals(MetricStat.COUNT.getTypeName(), aggregator.getAggregationType().getTypeName());
    }

    public void testGetAggregatedValueType() {
        assertEquals(CountValueAggregator.VALUE_AGGREGATOR_TYPE, aggregator.getAggregatedValueType());
    }

    public void testGetInitialAggregatedValueForSegmentDocValue() {
        assertEquals(1L, aggregator.getInitialAggregatedValueForSegmentDocValue(randomLong(), StarTreeNumericType.LONG), 0.0);
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        assertEquals(3L, aggregator.mergeAggregatedValueAndSegmentValue(2L, 3L, StarTreeNumericType.LONG), 0.0);
    }

    public void testMergeAggregatedValues() {
        assertEquals(5L, aggregator.mergeAggregatedValues(2L, 3L), 0.0);
    }

    public void testGetInitialAggregatedValue() {
        assertEquals(3L, aggregator.getInitialAggregatedValue(3L), 0.0);
    }

    public void testGetMaxAggregatedValueByteSize() {
        assertEquals(Long.BYTES, aggregator.getMaxAggregatedValueByteSize());
    }

    public void testToLongValue() {
        assertEquals(3L, aggregator.toLongValue(3L), 0.0);
    }

    public void testToStarTreeNumericTypeValue() {
        assertEquals(3L, aggregator.toStarTreeNumericTypeValue(3L, StarTreeNumericType.LONG), 0.0);
    }
}
