/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link TaskResourceUsageTrackerType} ensuring the new
 * {@code NATIVE_MEMORY_USAGE_TRACKER} entry round-trips through {@code fromName}.
 */
public class TaskResourceUsageTrackerTypeTests extends OpenSearchTestCase {

    public void testNativeMemoryUsageTrackerName() {
        assertEquals("native_memory_usage_tracker", TaskResourceUsageTrackerType.NATIVE_MEMORY_USAGE_TRACKER.getName());
    }

    public void testFromNameRoundTrip() {
        for (TaskResourceUsageTrackerType type : TaskResourceUsageTrackerType.values()) {
            assertSame(type, TaskResourceUsageTrackerType.fromName(type.getName()));
        }
    }

    public void testFromNameUnknownThrows() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> TaskResourceUsageTrackerType.fromName("not_a_real_tracker")
        );
        assertTrue(ex.getMessage().contains("not_a_real_tracker"));
    }

    public void testFromNameIsCaseSensitive() {
        // Mirrors the strictness of ResourceType.fromName — uppercase is not accepted.
        expectThrows(IllegalArgumentException.class, () -> TaskResourceUsageTrackerType.fromName("NATIVE_MEMORY_USAGE_TRACKER"));
    }
}
