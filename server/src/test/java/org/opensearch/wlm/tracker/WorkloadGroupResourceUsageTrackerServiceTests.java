/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.ResourceType;

/**
 * Unit tests for {@link WorkloadGroupResourceUsageTrackerService}'s {@code TRACKED_RESOURCES}
 * set. After NATIVE_MEMORY was added to {@link ResourceType} for search-backpressure duress
 * routing, this set MUST stay restricted to CPU + MEMORY so per-group accounting is unchanged.
 */
public class WorkloadGroupResourceUsageTrackerServiceTests extends OpenSearchTestCase {

    public void testTrackedResourcesContainsCpuAndMemory() {
        assertTrue(WorkloadGroupResourceUsageTrackerService.TRACKED_RESOURCES.contains(ResourceType.CPU));
        assertTrue(WorkloadGroupResourceUsageTrackerService.TRACKED_RESOURCES.contains(ResourceType.MEMORY));
    }

    public void testTrackedResourcesExcludesNativeMemory() {
        // Per the explicit comment on TRACKED_RESOURCES — WLM intentionally does not iterate
        // NATIVE_MEMORY because the calculator is a no-op and the duress signal is consumed
        // by SearchBackpressureService instead.
        assertFalse(WorkloadGroupResourceUsageTrackerService.TRACKED_RESOURCES.contains(ResourceType.NATIVE_MEMORY));
    }

    public void testTrackedResourcesHasExactlyTwoEntries() {
        assertEquals(2, WorkloadGroupResourceUsageTrackerService.TRACKED_RESOURCES.size());
    }
}
