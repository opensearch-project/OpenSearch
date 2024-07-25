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

public class MinValueAggregatorTests extends OpenSearchTestCase {
    private final MinValueAggregator aggregator = new MinValueAggregator(StarTreeNumericType.LONG);

    public void testGetAggregationType() {
        assertEquals(MetricStat.MIN.getTypeName(), aggregator.getAggregationType().getTypeName());
    }

    public void testGetAggregatedValueType() {
        assertEquals(MinValueAggregator.VALUE_AGGREGATOR_TYPE, aggregator.getAggregatedValueType());
    }

    public void testGetInitialAggregatedValueForSegmentDocValue() {
        assertEquals(1.0, aggregator.getInitialAggregatedValueForSegmentDocValue(1L), 0.0);
        assertNull(aggregator.getInitialAggregatedValueForSegmentDocValue(null));
    }

    public void testMergeAggregatedValueAndSegmentValue() {
        assertEquals(2.0, aggregator.mergeAggregatedValueAndSegmentValue(2.0, 3L), 0.0);
    }

    public void testMergeAggregatedValues() {
        assertEquals(2.0, aggregator.mergeAggregatedValues(2.0, 3.0), 0.0);
    }

    public void testGetInitialAggregatedValue() {
        assertEquals(3.0, aggregator.getInitialAggregatedValue(3.0), 0.0);
    }

    public void testGetMaxAggregatedValueByteSize() {
        assertEquals(Double.BYTES, aggregator.getMaxAggregatedValueByteSize());
    }

    public void testToLongValue() {
        assertEquals(NumericUtils.doubleToSortableLong(3.0), aggregator.toLongValue(3.0), 0.0);
    }

    public void testToStarTreeNumericTypeValue() {
        MinValueAggregator aggregator = new MinValueAggregator(StarTreeNumericType.DOUBLE);
        assertEquals(NumericUtils.sortableLongToDouble(3L), aggregator.toStarTreeNumericTypeValue(3L), 0.0);
    }

    public void testIdentityMetricValue() {
        assertNull(aggregator.getIdentityMetricValue());
    }
}
