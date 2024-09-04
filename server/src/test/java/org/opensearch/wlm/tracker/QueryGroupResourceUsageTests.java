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
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.tracker.QueryGroupResourceUsage.QueryGroupCpuUsage;
import org.opensearch.wlm.tracker.QueryGroupResourceUsage.QueryGroupMemoryUsage;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerServiceTests.TestClock;

import java.util.List;
import java.util.Map;

import static org.opensearch.wlm.cancellation.DefaultTaskCancellation.MIN_VALUE;
import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.HEAP_SIZE_BYTES;
import static org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService.PROCESSOR_COUNT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryGroupResourceUsageTests extends OpenSearchTestCase {
    QueryGroupResourceUsage sut;
    WorkloadManagementSettings settings;

    public void testFactoryMethods() {
        assertTrue(QueryGroupResourceUsage.from(ResourceType.CPU) instanceof QueryGroupCpuUsage);
        assertTrue(QueryGroupResourceUsage.from(ResourceType.MEMORY) instanceof QueryGroupMemoryUsage);
        assertThrows(IllegalArgumentException.class, () -> QueryGroupResourceUsage.from(null));
    }

    public void testQueryGroupCpuUsage() {
        sut = new QueryGroupCpuUsage();
        TestClock clock = new TestClock();
        long fastForwardTime = PROCESSOR_COUNT * 200L;
        clock.fastForwardBy(fastForwardTime);
        QueryGroup queryGroup = new QueryGroup(
            "testQG",
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(ResourceType.CPU, 0.5 / PROCESSOR_COUNT)
        );

        sut.initialise(List.of(createMockTaskWithResourceStats(QueryGroupTask.class, fastForwardTime, 200, 0, 123)), clock::getTime);
        settings = mock(WorkloadManagementSettings.class);
        when(settings.getNodeLevelCpuCancellationThreshold()).thenReturn(0.90);

        double expectedNormalisedThreshold = 0.5 / PROCESSOR_COUNT * 0.9;
        double expectedQueryGroupCpuUsage = 1.0 / PROCESSOR_COUNT;
        double expectedReduceBy = expectedQueryGroupCpuUsage - expectedNormalisedThreshold;
        assertEquals(expectedNormalisedThreshold, sut.getNormalisedThresholdFor(queryGroup, settings), MIN_VALUE);
        assertEquals(expectedQueryGroupCpuUsage, sut.getCurrentUsage(), MIN_VALUE);
        assertTrue(sut.isBreachingThresholdFor(queryGroup, settings));
        assertEquals(expectedReduceBy, sut.getReduceByFor(queryGroup, settings), MIN_VALUE);
    }

    public void testQueryGroupMemoryUsage() {
        sut = new QueryGroupMemoryUsage();
        TestClock clock = new TestClock();
        QueryGroup queryGroup = new QueryGroup(
            "testQG",
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 500.0 / HEAP_SIZE_BYTES)
        );

        sut.initialise(List.of(createMockTaskWithResourceStats(QueryGroupTask.class, 100, 200, 0, 123)), clock::getTime);
        settings = mock(WorkloadManagementSettings.class);
        when(settings.getNodeLevelMemoryCancellationThreshold()).thenReturn(0.90);

        double expectedNormalisedThreshold = 500.0 / HEAP_SIZE_BYTES * 0.9;
        double expectedCurrentUsage = 200.0 / HEAP_SIZE_BYTES;
        assertEquals(expectedNormalisedThreshold, sut.getNormalisedThresholdFor(queryGroup, settings), MIN_VALUE);
        assertEquals(expectedCurrentUsage, sut.getCurrentUsage(), MIN_VALUE);
        assertFalse(sut.isBreachingThresholdFor(queryGroup, settings));
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
