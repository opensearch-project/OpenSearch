/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.tracker;

import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.MutableQueryGroupFragment;
import org.opensearch.wlm.MutableQueryGroupFragment.ResiliencyMode;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.tracker.ResourceUsageCalculatorTrackerServiceTests.TestClock;

import java.util.List;
import java.util.Map;

import static org.opensearch.wlm.cancellation.TaskCancellationService.MIN_VALUE;
import static org.opensearch.wlm.tracker.CpuUsageCalculator.PROCESSOR_COUNT;
import static org.opensearch.wlm.tracker.MemoryUsageCalculator.HEAP_SIZE_BYTES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceUsageCalculatorTests extends OpenSearchTestCase {

    public void testQueryGroupCpuUsage() {
        TestClock clock = new TestClock();
        long fastForwardTime = PROCESSOR_COUNT * 200L;
        clock.fastForwardBy(fastForwardTime);
        QueryGroup queryGroup = new QueryGroup(
            "testQG",
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.5 / PROCESSOR_COUNT))
        );
        double expectedQueryGroupCpuUsage = 1.0 / PROCESSOR_COUNT;

        QueryGroupTask mockTask = createMockTaskWithResourceStats(QueryGroupTask.class, fastForwardTime, 200, 0, 123);
        ResourceType.CPU.getResourceUsageCalculator().setNanoTimeSupplier(clock::getTime);
        double actualUsage = ResourceType.CPU.getResourceUsageCalculator().calculateResourceUsage(List.of(mockTask));
        assertEquals(expectedQueryGroupCpuUsage, actualUsage, MIN_VALUE);

        double taskResourceUsage = ResourceType.CPU.getResourceUsageCalculator().calculateTaskResourceUsage(mockTask);
        assertEquals(1.0, taskResourceUsage, MIN_VALUE);
    }

    public void testQueryGroupMemoryUsage() {
        QueryGroupTask mockTask = createMockTaskWithResourceStats(QueryGroupTask.class, 100, 200, 0, 123);
        double actualMemoryUsage = ResourceType.MEMORY.getResourceUsageCalculator().calculateResourceUsage(List.of(mockTask));
        double expectedMemoryUsage = 200.0 / HEAP_SIZE_BYTES;

        assertEquals(expectedMemoryUsage, actualMemoryUsage, MIN_VALUE);
        assertEquals(
            200.0 / HEAP_SIZE_BYTES,
            ResourceType.MEMORY.getResourceUsageCalculator().calculateTaskResourceUsage(mockTask),
            MIN_VALUE
        );
    }

    public static <T extends QueryGroupTask> T createMockTaskWithResourceStats(
        Class<T> type,
        long cpuUsage,
        long heapUsage,
        long startTimeNanos,
        long taskId
    ) {
        T task = mock(type);
        when(task.getTotalResourceUtilization(ResourceStats.CPU)).thenReturn(cpuUsage);
        when(task.getTotalResourceUtilization(ResourceStats.MEMORY)).thenReturn(heapUsage);
        when(task.getStartTimeNanos()).thenReturn(startTimeNanos);
        when(task.getId()).thenReturn(taskId);
        return task;
    }
}
