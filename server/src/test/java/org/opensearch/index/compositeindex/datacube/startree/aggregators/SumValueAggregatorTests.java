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

public class SumValueAggregatorTests extends OpenSearchTestCase {

    private final SumValueAggregator aggregator = new SumValueAggregator();

    public void testGetAggregationType() {
        assertEquals(MetricStat.SUM.getTypeName(), aggregator.getAggregationType().getTypeName());
    }

    public void testGetStarTreeNumericType() {
        assertEquals(SumValueAggregator.STAR_TREE_NUMERIC_TYPE, aggregator.getStarTreeNumericType());
    }

    public void testGetInitialAggregatedValue() {
        assertEquals(1.0, aggregator.getInitialAggregatedValue(1L, StarTreeNumericType.LONG), 0.0);
        assertThrows(NullPointerException.class, () -> aggregator.getInitialAggregatedValue(null, StarTreeNumericType.DOUBLE));
    }

    public void testApplySegmentRawValue() {
        assertEquals(5.0, aggregator.applySegmentRawValue(2.0, 3L, StarTreeNumericType.LONG), 0.0);
        assertThrows(NullPointerException.class, () -> aggregator.applySegmentRawValue(3.14, null, StarTreeNumericType.DOUBLE));
    }

    public void testApplyAggregatedValue() {
        assertEquals(5.0, aggregator.applyAggregatedValue(2.0, 3.0), 0.0);
    }

    public void testGetAggregatedValue() {
        assertEquals(3.14, aggregator.getAggregatedValue(3.14), 0.0);
    }

    public void testGetMaxAggregatedValueByteSize() {
        assertEquals(Double.BYTES, aggregator.getMaxAggregatedValueByteSize());
    }

    public void testToLongValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(NumericUtils.doubleToSortableLong(3.14), aggregator.toLongValue(3.14), 0.0);
    }

    public void testToStarTreeNumericTypeValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(NumericUtils.sortableLongToDouble(3L), aggregator.toStarTreeNumericTypeValue(3L, StarTreeNumericType.DOUBLE), 0.0);
    }
}
