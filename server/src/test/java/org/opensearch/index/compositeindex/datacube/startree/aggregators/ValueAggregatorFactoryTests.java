/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.data.DataType;
import org.opensearch.test.OpenSearchTestCase;

public class ValueAggregatorFactoryTests extends OpenSearchTestCase {

    public void testGetValueAggregatorForSumType() {
        ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(MetricStat.SUM);
        assertNotNull(aggregator);
        assertEquals(SumValueAggregator.class, aggregator.getClass());
    }

    public void testGetValueAggregatorForUnsupportedType() {
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> ValueAggregatorFactory.getValueAggregator(MetricStat.UNSUPPORTED)
        );
        assertEquals("Unsupported aggregation type: UNSUPPORTED", exception.getMessage());
    }

    public void testGetAggregatedValueTypeForSumType() {
        DataType dataType = ValueAggregatorFactory.getAggregatedValueType(MetricStat.SUM);
        assertEquals(SumValueAggregator.AGGREGATED_VALUE_TYPE, dataType);
    }

    public void testGetAggregatedValueTypeForUnsupportedType() {
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> ValueAggregatorFactory.getAggregatedValueType(MetricStat.UNSUPPORTED)
        );
        assertEquals("Unsupported aggregation type: UNSUPPORTED", exception.getMessage());
    }
}
