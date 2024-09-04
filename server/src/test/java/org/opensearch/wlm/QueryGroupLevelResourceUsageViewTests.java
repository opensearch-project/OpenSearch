/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.tracker.QueryGroupResourceUsage;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerServiceTests;

import java.util.List;
import java.util.Map;

import static org.opensearch.wlm.cancellation.DefaultTaskCancellation.MIN_VALUE;
import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTests.createMockTaskWithResourceStats;
import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.HEAP_SIZE_BYTES;

public class QueryGroupLevelResourceUsageViewTests extends OpenSearchTestCase {
    Map<ResourceType, QueryGroupResourceUsage> resourceUsage;
    List<QueryGroupTask> activeTasks;
    QueryGroupResourceUsageTrackerServiceTests.TestClock clock;

    public void setUp() throws Exception {
        super.setUp();
        QueryGroupResourceUsage.QueryGroupCpuUsage cpuUsage = new QueryGroupResourceUsage.QueryGroupCpuUsage();
        QueryGroupResourceUsage.QueryGroupMemoryUsage memoryUsage = new QueryGroupResourceUsage.QueryGroupMemoryUsage();
        clock = new QueryGroupResourceUsageTrackerServiceTests.TestClock();
        activeTasks = List.of(createMockTaskWithResourceStats(QueryGroupTask.class, 100, 200, 0, 1));
        clock.fastForwardBy(300);

        cpuUsage.initialise(activeTasks, clock::getTime);
        memoryUsage.initialise(activeTasks, clock::getTime);

        resourceUsage = Map.of(ResourceType.MEMORY, memoryUsage, ResourceType.CPU, memoryUsage);
    }

    public void testGetResourceUsageData() {
        QueryGroupLevelResourceUsageView queryGroupLevelResourceUsageView = new QueryGroupLevelResourceUsageView(
            resourceUsage,
            activeTasks
        );
        Map<ResourceType, QueryGroupResourceUsage> resourceUsageData = queryGroupLevelResourceUsageView.getResourceUsageData();
        assertTrue(assertResourceUsageData(resourceUsageData));
    }

    public void testGetActiveTasks() {
        QueryGroupLevelResourceUsageView queryGroupLevelResourceUsageView = new QueryGroupLevelResourceUsageView(
            resourceUsage,
            activeTasks
        );
        List<QueryGroupTask> activeTasks = queryGroupLevelResourceUsageView.getActiveTasks();
        assertEquals(1, activeTasks.size());
        assertEquals(1, activeTasks.get(0).getId());
    }

    private boolean assertResourceUsageData(Map<ResourceType, QueryGroupResourceUsage> resourceUsageData) {
        return (resourceUsageData.get(ResourceType.MEMORY).getCurrentUsage() - 200.0 / HEAP_SIZE_BYTES) <= MIN_VALUE
            && (resourceUsageData.get(ResourceType.CPU).getCurrentUsage() - 100.0 / (300)) < MIN_VALUE;
    }
}
