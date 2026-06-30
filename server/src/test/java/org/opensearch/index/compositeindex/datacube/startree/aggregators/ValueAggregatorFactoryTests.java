/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

public class ValueAggregatorFactoryTests extends OpenSearchTestCase {

    public void testGetValueAggregatorForSumType() {
        ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(MetricStat.SUM, NumberFieldMapper.NumberType.LONG);
        assertNotNull(aggregator);
        assertEquals(SumValueAggregator.class, aggregator.getClass());
    }

    public void testGetValueAggregatorForMinType() {
        ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(MetricStat.MIN, NumberFieldMapper.NumberType.LONG);
        assertNotNull(aggregator);
        assertEquals(MinValueAggregator.class, aggregator.getClass());
    }

    public void testGetValueAggregatorForMaxType() {
        ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(MetricStat.MAX, NumberFieldMapper.NumberType.LONG);
        assertNotNull(aggregator);
        assertEquals(MaxValueAggregator.class, aggregator.getClass());
    }

    public void testGetValueAggregatorForCountType() {
        ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(MetricStat.VALUE_COUNT, NumberFieldMapper.NumberType.LONG);
        assertNotNull(aggregator);
        assertEquals(CountValueAggregator.class, aggregator.getClass());
    }

    public void testGetValueAggregatorForAvgType() {
        assertThrows(
            IllegalStateException.class,
            () -> ValueAggregatorFactory.getValueAggregator(MetricStat.AVG, NumberFieldMapper.NumberType.LONG)
        );
    }

}
