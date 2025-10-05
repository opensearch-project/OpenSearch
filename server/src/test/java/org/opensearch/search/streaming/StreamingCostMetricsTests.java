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
        StreamingCostMetrics metrics = new StreamingCostMetrics(true, 100, 1000, 5, 10000);
        assertTrue(metrics.streamable());
        assertEquals(100, metrics.topNSize());
        assertEquals(1000, metrics.estimatedBucketCount());
        assertEquals(5, metrics.segmentCount());
        assertEquals(10000, metrics.estimatedDocCount());
    }

    public void testNonStreamable() {
        StreamingCostMetrics metrics = StreamingCostMetrics.nonStreamable();
        assertFalse(metrics.streamable());
        assertEquals(0, metrics.topNSize());
        assertEquals(0, metrics.estimatedBucketCount());
        assertEquals(0, metrics.segmentCount());
        assertEquals(0, metrics.estimatedDocCount());
    }

    public void testCombineWithSubAggregation() {
        StreamingCostMetrics parent = new StreamingCostMetrics(true, 100, 500, 3, 5000);
        StreamingCostMetrics sub = new StreamingCostMetrics(true, 50, 200, 2, 3000);

        StreamingCostMetrics combined = parent.combineWithSubAggregation(sub);

        assertTrue(combined.streamable());
        assertEquals(5000, combined.topNSize()); // 100 * 50
        assertEquals(100000, combined.estimatedBucketCount()); // 500 * 200
        assertEquals(3, combined.segmentCount()); // max(3, 2)
        assertEquals(5000, combined.estimatedDocCount()); // max(5000, 3000)
    }

    public void testCombineWithSibling() {
        StreamingCostMetrics sibling1 = new StreamingCostMetrics(true, 100, 500, 3, 5000);
        StreamingCostMetrics sibling2 = new StreamingCostMetrics(true, 200, 300, 4, 7000);

        StreamingCostMetrics combined = sibling1.combineWithSibling(sibling2);

        assertTrue(combined.streamable());
        assertEquals(300, combined.topNSize()); // 100 + 200
        assertEquals(800, combined.estimatedBucketCount()); // 500 + 300
        assertEquals(4, combined.segmentCount()); // max(3, 4)
        assertEquals(12000, combined.estimatedDocCount()); // 5000 + 7000
    }

    public void testCombineNonStreamableWithStreamable() {
        StreamingCostMetrics streamable = new StreamingCostMetrics(true, 100, 500, 3, 5000);
        StreamingCostMetrics nonStreamable = new StreamingCostMetrics(false, 0, 0, 0, 0);

        StreamingCostMetrics combined = streamable.combineWithSubAggregation(nonStreamable);

        assertFalse(combined.streamable());
    }
}
