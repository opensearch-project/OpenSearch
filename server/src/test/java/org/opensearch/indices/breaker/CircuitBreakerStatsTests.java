/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.breaker;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.indices.breaker.CircuitBreakerStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class CircuitBreakerStatsTests extends OpenSearchTestCase {

    public void testNegativeValuesClampedToZero() {
        CircuitBreakerStats stats = new CircuitBreakerStats("test", -100L, -50L, 1.0, 5L);
        assertEquals(0L, stats.getLimit());
        assertEquals(0L, stats.getEstimated());
        assertEquals(5L, stats.getTrippedCount());
    }

    public void testPositiveValuesUnchanged() {
        CircuitBreakerStats stats = new CircuitBreakerStats("test", 1000L, 500L, 1.0, 5L);
        assertEquals(1000L, stats.getLimit());
        assertEquals(500L, stats.getEstimated());
        assertEquals(5L, stats.getTrippedCount());
    }
}
