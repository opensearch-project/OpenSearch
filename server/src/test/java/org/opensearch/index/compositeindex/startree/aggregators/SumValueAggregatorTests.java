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

public class SumValueAggregatorTests extends OpenSearchTestCase {

    public void testGetAggregationType() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(MetricType.SUM.getTypeName(), aggregator.getAggregationType().getTypeName());
    }

    public void testGetAggregatedValueType() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(SumValueAggregator.AGGREGATED_VALUE_TYPE, aggregator.getAggregatedValueType());
    }

    public void testGetInitialAggregatedValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(1.0, aggregator.getInitialAggregatedValue(1), 0.0);
        assertEquals(3.14, aggregator.getInitialAggregatedValue(3.14), 0.0);
    }

    public void testApplyRawValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(5.0, aggregator.applyRawValue(2.0, 3), 0.0);
        assertEquals(7.28, aggregator.applyRawValue(3.14, 4.14), 0.0000001);
    }

    public void testApplyAggregatedValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(5.0, aggregator.applyAggregatedValue(2.0, 3.0), 0.0);
        assertEquals(7.28, aggregator.applyAggregatedValue(3.14, 4.14), 0.0000001);
    }

    public void testCloneAggregatedValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(3.14, aggregator.cloneAggregatedValue(3.14), 0.0);
    }

    public void testGetMaxAggregatedValueByteSize() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertEquals(Double.BYTES, aggregator.getMaxAggregatedValueByteSize());
    }

    public void testSerializeAggregatedValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertThrows(UnsupportedOperationException.class, () -> aggregator.serializeAggregatedValue(3.14));
    }

    public void testDeserializeAggregatedValue() {
        SumValueAggregator aggregator = new SumValueAggregator();
        assertThrows(UnsupportedOperationException.class, () -> aggregator.deserializeAggregatedValue(new byte[0]));
    }
}
