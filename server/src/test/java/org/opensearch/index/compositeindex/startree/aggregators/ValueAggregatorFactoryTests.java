/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.startree.aggregators;

import org.opensearch.index.compositeindex.MetricType;
import org.opensearch.test.OpenSearchTestCase;

public class ValueAggregatorFactoryTests extends OpenSearchTestCase {

    public void testGetValueAggregatorForSumType() {
        ValueAggregator aggregator = ValueAggregatorFactory.getValueAggregator(MetricType.SUM);
        assertNotNull(aggregator);
        assertEquals(SumValueAggregator.class, aggregator.getClass());
    }

    public void testGetValueAggregatorForUnsupportedType() {
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> ValueAggregatorFactory.getValueAggregator(MetricType.UNSUPPORTED)
        );
        assertEquals("Unsupported aggregation type: UNSUPPORTED", exception.getMessage());
    }

    public void testGetAggregatedValueTypeForSumType() {
        DataType dataType = ValueAggregatorFactory.getAggregatedValueType(MetricType.SUM);
        assertEquals(SumValueAggregator.AGGREGATED_VALUE_TYPE, dataType);
    }

    public void testGetAggregatedValueTypeForUnsupportedType() {
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> ValueAggregatorFactory.getAggregatedValueType(MetricType.UNSUPPORTED)
        );
        assertEquals("Unsupported aggregation type: UNSUPPORTED", exception.getMessage());
    }
}
