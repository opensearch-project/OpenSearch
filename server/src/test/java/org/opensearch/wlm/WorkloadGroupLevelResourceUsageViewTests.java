/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.tracker.ResourceUsageCalculatorTrackerServiceTests;

import java.util.List;
import java.util.Map;

import static org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService.MIN_VALUE;
import static org.opensearch.wlm.tracker.CpuUsageCalculator.PROCESSOR_COUNT;
import static org.opensearch.wlm.tracker.MemoryUsageCalculator.HEAP_SIZE_BYTES;
import static org.opensearch.wlm.tracker.ResourceUsageCalculatorTests.createMockTaskWithResourceStats;
import static org.mockito.Mockito.mock;

public class WorkloadGroupLevelResourceUsageViewTests extends OpenSearchTestCase {
    Map<ResourceType, Double> resourceUsage;
    List<WorkloadGroupTask> activeTasks;
    ResourceUsageCalculatorTrackerServiceTests.TestClock clock;
    WorkloadManagementSettings settings;

    public void setUp() throws Exception {
        super.setUp();
        settings = mock(WorkloadManagementSettings.class);
        clock = new ResourceUsageCalculatorTrackerServiceTests.TestClock();
        activeTasks = List.of(createMockTaskWithResourceStats(WorkloadGroupTask.class, 100, 200, 0, 1));
        clock.fastForwardBy(300);
        double memoryUsage = 200.0 / HEAP_SIZE_BYTES;
        double cpuUsage = 100.0 / (PROCESSOR_COUNT * 300.0);

        resourceUsage = Map.of(ResourceType.MEMORY, memoryUsage, ResourceType.CPU, cpuUsage);
    }

    public void testGetResourceUsageData() {
        WorkloadGroupLevelResourceUsageView workloadGroupLevelResourceUsageView = new WorkloadGroupLevelResourceUsageView(
            resourceUsage,
            activeTasks
        );
        Map<ResourceType, Double> resourceUsageData = workloadGroupLevelResourceUsageView.getResourceUsageData();
        assertTrue(assertResourceUsageData(resourceUsageData));
    }

    public void testGetActiveTasks() {
        WorkloadGroupLevelResourceUsageView workloadGroupLevelResourceUsageView = new WorkloadGroupLevelResourceUsageView(
            resourceUsage,
            activeTasks
        );
        List<WorkloadGroupTask> activeTasks = workloadGroupLevelResourceUsageView.getActiveTasks();
        assertEquals(1, activeTasks.size());
        assertEquals(1, activeTasks.get(0).getId());
    }

    private boolean assertResourceUsageData(Map<ResourceType, Double> resourceUsageData) {
        return (resourceUsageData.get(ResourceType.MEMORY) - 200.0 / HEAP_SIZE_BYTES) <= MIN_VALUE
            && (resourceUsageData.get(ResourceType.CPU) - 100.0 / (300)) < MIN_VALUE;
    }
}
