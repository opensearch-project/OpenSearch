/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.test.OpenSearchTestCase;

public class StreamingCostMetricsTests extends OpenSearchTestCase {

    public void testStreamableMetrics() {
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100);
        assertTrue(metrics.streamable());
        assertEquals(100, metrics.topNSize());
    }

    public void testNonStreamable() {
        StreamingCostMetrics metrics = StreamingCostMetrics.nonStreamable();
        assertFalse(metrics.streamable());
        assertEquals(0, metrics.topNSize());
    }

    public void testNeutral() {
        StreamingCostMetrics metrics = StreamingCostMetrics.neutral();
        assertTrue(metrics.streamable());
        assertEquals(1, metrics.topNSize());
    }

    public void testCombineWithSubAggregation() {
        StreamingCostMetrics parent = new StreamingCostMetrics(true, 100);
        StreamingCostMetrics sub = new StreamingCostMetrics(true, 50);

        StreamingCostMetrics combined = parent.combineWithSubAggregation(sub);

        assertTrue(combined.streamable());
        assertEquals(5000, combined.topNSize()); // 100 * 50
    }

    public void testCombineWithSubAggregationOverflow() {
        StreamingCostMetrics parent = new StreamingCostMetrics(true, Integer.MAX_VALUE);
        StreamingCostMetrics sub = new StreamingCostMetrics(true, 2);

        StreamingCostMetrics combined = parent.combineWithSubAggregation(sub);

        assertFalse(combined.streamable()); // Overflow causes non-streamable
    }

    public void testCombineWithSibling() {
        StreamingCostMetrics sibling1 = new StreamingCostMetrics(true, 100);
        StreamingCostMetrics sibling2 = new StreamingCostMetrics(true, 200);

        StreamingCostMetrics combined = sibling1.combineWithSibling(sibling2);

        assertTrue(combined.streamable());
        assertEquals(300, combined.topNSize()); // 100 + 200
    }

    public void testCombineWithSiblingOverflow() {
        StreamingCostMetrics sibling1 = new StreamingCostMetrics(true, Integer.MAX_VALUE);
        StreamingCostMetrics sibling2 = new StreamingCostMetrics(true, 1);

        StreamingCostMetrics combined = sibling1.combineWithSibling(sibling2);

        assertFalse(combined.streamable()); // Overflow causes non-streamable
    }

    public void testCombineNonStreamableWithStreamable() {
        StreamingCostMetrics streamable = new StreamingCostMetrics(true, 100);
        StreamingCostMetrics nonStreamable = StreamingCostMetrics.nonStreamable();

        StreamingCostMetrics combined = streamable.combineWithSubAggregation(nonStreamable);

        assertFalse(combined.streamable());
    }
}
