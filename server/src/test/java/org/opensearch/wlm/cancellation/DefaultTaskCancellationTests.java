/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.QueryGroupLevelResourceUsageView;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.tracker.QueryGroupResourceUsage;
import org.opensearch.wlm.tracker.QueryGroupResourceUsage.QueryGroupCpuUsage;
import org.opensearch.wlm.tracker.QueryGroupResourceUsage.QueryGroupMemoryUsage;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerServiceTests.TestClock;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTaskCancellationTests extends OpenSearchTestCase {
    private static final String queryGroupId1 = "queryGroup1";
    private static final String queryGroupId2 = "queryGroup2";

    private TestClock clock;

    private static class TestTaskCancellationImpl extends DefaultTaskCancellation {

        public TestTaskCancellationImpl(
            WorkloadManagementSettings workloadManagementSettings,
            DefaultTaskSelectionStrategy defaultTaskSelectionStrategy,
            Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelViews,
            Set<QueryGroup> activeQueryGroups,
            Set<QueryGroup> deletedQueryGroups,
            BooleanSupplier isNodeInDuress
        ) {
            super(
                workloadManagementSettings,
                defaultTaskSelectionStrategy,
                queryGroupLevelViews,
                activeQueryGroups,
                deletedQueryGroups,
                isNodeInDuress
            );
        }
    }

    private Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelViews;
    private Set<QueryGroup> activeQueryGroups;
    private Set<QueryGroup> deletedQueryGroups;
    private DefaultTaskCancellation taskCancellation;
    private WorkloadManagementSettings workloadManagementSettings;

    @Before
    public void setup() {
        workloadManagementSettings = mock(WorkloadManagementSettings.class);
        queryGroupLevelViews = new HashMap<>();
        activeQueryGroups = new HashSet<>();
        deletedQueryGroups = new HashSet<>();
        clock = new TestClock();
        taskCancellation = new TestTaskCancellationImpl(
            workloadManagementSettings,
            new DefaultTaskSelectionStrategy(),
            queryGroupLevelViews,
            activeQueryGroups,
            deletedQueryGroups,
            () -> false
        );
    }

    public void testGetCancellableTasksFrom_setupAppropriateCancellationReasonAndScore() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage cpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage memoryUsage = mock(QueryGroupMemoryUsage.class);
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );
        clock.fastForwardBy(1000);
        when(memoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);
        when(cpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(true);
        when(cpuUsage.getReduceByFor(any(), any())).thenReturn(0.001);
        when(memoryUsage.getReduceByFor(any(), any())).thenReturn(0.0);

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(resourceType, cpuUsage, ResourceType.MEMORY, memoryUsage));
        queryGroupLevelViews.put(queryGroupId1, mockView);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(queryGroup1);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
        assertEquals(
            "[Workload Management] Cancelling Task ID : "
                + cancellableTasksFrom.get(0).getTask().getId()
                + " from QueryGroup ID : queryGroup1"
                + " breached the resource limit of : 10.0 for resource type : cpu",
            cancellableTasksFrom.get(0).getReasonString()
        );
        assertEquals(5, cancellableTasksFrom.get(0).getReasons().get(0).getCancellationScore());
    }

    public void testGetCancellableTasksFrom_returnsTasksWhenBreachingThreshold() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage cpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage memoryUsage = mock(QueryGroupMemoryUsage.class);
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );
        when(memoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);

        when(cpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(true);
        when(cpuUsage.getReduceByFor(any(), any())).thenReturn(0.15);

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(resourceType, cpuUsage, ResourceType.MEMORY, memoryUsage));
        queryGroupLevelViews.put(queryGroupId1, mockView);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(queryGroup1);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_returnsTasksWhenBreachingThresholdForMemory() {
        ResourceType resourceType = ResourceType.MEMORY;
        QueryGroupCpuUsage cpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage memoryUsage = mock(QueryGroupMemoryUsage.class);
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );
        when(memoryUsage.getCurrentUsage()).thenReturn(0.15);
        when(memoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(true);
        when(memoryUsage.getReduceByFor(any(), any())).thenReturn(0.005);
        when(cpuUsage.getCurrentUsage()).thenReturn(0.0);
        when(cpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, resourceType, memoryUsage));

        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_returnsNoTasksWhenNotBreachingThreshold() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage cpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage memoryUsage = mock(QueryGroupMemoryUsage.class);
        Double threshold = 0.9;
        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );
        when(memoryUsage.getCurrentUsage()).thenReturn(0.0);
        when(memoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);
        when(memoryUsage.getReduceByFor(any(), any())).thenReturn(0.005);
        when(cpuUsage.getCurrentUsage()).thenReturn(0.0);
        when(cpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, ResourceType.MEMORY, memoryUsage));
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        when(workloadManagementSettings.getNodeLevelCpuCancellationThreshold()).thenReturn(0.90);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(queryGroup1);
        assertTrue(cancellableTasksFrom.isEmpty());
    }

    public void testGetCancellableTasksFrom_filtersQueryGroupCorrectly() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage usage = mock(QueryGroupCpuUsage.class);
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);

        TestTaskCancellationImpl taskCancellation = new TestTaskCancellationImpl(
            workloadManagementSettings,
            new DefaultTaskSelectionStrategy(),
            queryGroupLevelViews,
            activeQueryGroups,
            deletedQueryGroups,
            () -> false
        );

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.SOFT);
        assertEquals(0, cancellableTasksFrom.size());
    }

    public void testCancelTasks_cancelsGivenTasks() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage cpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage memoryUsage = mock(QueryGroupMemoryUsage.class);

        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        when(memoryUsage.getCurrentUsage()).thenReturn(0.15);
        when(memoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);

        when(cpuUsage.getReduceByFor(any(), any())).thenReturn(0.005);
        when(cpuUsage.getCurrentUsage()).thenReturn(0.16);
        when(cpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(true);

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, ResourceType.MEMORY, memoryUsage));

        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);

        TestTaskCancellationImpl taskCancellation = new TestTaskCancellationImpl(
            workloadManagementSettings,
            new DefaultTaskSelectionStrategy(),
            queryGroupLevelViews,
            activeQueryGroups,
            deletedQueryGroups,
            () -> false
        );

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        taskCancellation.cancelTasks();
        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
    }

    public void testCancelTasks_cancelsTasksFromDeletedQueryGroups() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage activeQueryGroupCpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage activeQueryGroupMemoryUsage = mock(QueryGroupMemoryUsage.class);
        QueryGroupCpuUsage deletedQueryGroupCpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage deletedQueryGroupMemoryUsage = mock(QueryGroupMemoryUsage.class);
        Double threshold = 0.01;

        QueryGroup activeQueryGroup = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroup deletedQueryGroup = new QueryGroup(
            "testQueryGroup",
            queryGroupId2,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        when(activeQueryGroupCpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(true);
        when(deletedQueryGroupCpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(true);

        when(deletedQueryGroupMemoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);
        when(activeQueryGroupMemoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);

        when(deletedQueryGroupMemoryUsage.getReduceByFor(any(), any())).thenReturn(0.0);
        when(activeQueryGroupMemoryUsage.getReduceByFor(any(), any())).thenReturn(0.0);

        when(deletedQueryGroupCpuUsage.getReduceByFor(any(), any())).thenReturn(0.001);
        when(activeQueryGroupCpuUsage.getReduceByFor(any(), any())).thenReturn(0.001);

        QueryGroupLevelResourceUsageView mockView1 = createResourceUsageViewMock();
        QueryGroupLevelResourceUsageView mockView2 = createResourceUsageViewMock(
            resourceType,
            deletedQueryGroupCpuUsage,
            List.of(1000, 1001)
        );

        when(mockView1.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, activeQueryGroupCpuUsage, ResourceType.MEMORY, activeQueryGroupMemoryUsage)
        );
        when(mockView2.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, deletedQueryGroupCpuUsage, ResourceType.MEMORY, deletedQueryGroupMemoryUsage)
        );
        queryGroupLevelViews.put(queryGroupId1, mockView1);
        queryGroupLevelViews.put(queryGroupId2, mockView2);

        activeQueryGroups.add(activeQueryGroup);
        deletedQueryGroups.add(deletedQueryGroup);

        TestTaskCancellationImpl taskCancellation = new TestTaskCancellationImpl(
            workloadManagementSettings,
            new DefaultTaskSelectionStrategy(),
            queryGroupLevelViews,
            activeQueryGroups,
            deletedQueryGroups,
            () -> true
        );

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        List<TaskCancellation> cancellableTasksFromDeletedQueryGroups = taskCancellation.getTaskCancellationsForDeletedQueryGroup(
            deletedQueryGroup
        );
        assertEquals(2, cancellableTasksFromDeletedQueryGroups.size());
        assertEquals(1000, cancellableTasksFromDeletedQueryGroups.get(0).getTask().getId());
        assertEquals(1001, cancellableTasksFromDeletedQueryGroups.get(1).getTask().getId());

        taskCancellation.cancelTasks();

        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
        assertTrue(cancellableTasksFromDeletedQueryGroups.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFromDeletedQueryGroups.get(1).getTask().isCancelled());
    }

    public void testCancelTasks_does_not_cancelTasksFromDeletedQueryGroups_whenNodeNotInDuress() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage activeQueryGroupCpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage activeQueryGroupMemoryUsage = mock(QueryGroupMemoryUsage.class);
        QueryGroupCpuUsage deletedQueryGroupCpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage deletedQueryGroupMemoryUsage = mock(QueryGroupMemoryUsage.class);

        Double threshold = 0.01;

        QueryGroup activeQueryGroup = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroup deletedQueryGroup = new QueryGroup(
            "testQueryGroup",
            queryGroupId2,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        when(activeQueryGroupCpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(true);
        when(deletedQueryGroupCpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(true);
        when(activeQueryGroupMemoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);
        when(deletedQueryGroupMemoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);

        when(deletedQueryGroupCpuUsage.getReduceByFor(any(), any())).thenReturn(0.001);
        when(activeQueryGroupCpuUsage.getReduceByFor(any(), any())).thenReturn(0.001);
        when(deletedQueryGroupMemoryUsage.getReduceByFor(any(), any())).thenReturn(0.0);
        when(activeQueryGroupMemoryUsage.getReduceByFor(any(), any())).thenReturn(0.0);

        QueryGroupLevelResourceUsageView mockView1 = createResourceUsageViewMock();
        QueryGroupLevelResourceUsageView mockView2 = createResourceUsageViewMock(
            resourceType,
            deletedQueryGroupCpuUsage,
            List.of(1000, 1001)
        );

        when(mockView1.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, activeQueryGroupCpuUsage, ResourceType.MEMORY, activeQueryGroupMemoryUsage)
        );
        when(mockView2.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, deletedQueryGroupCpuUsage, ResourceType.MEMORY, deletedQueryGroupMemoryUsage)
        );

        queryGroupLevelViews.put(queryGroupId1, mockView1);
        queryGroupLevelViews.put(queryGroupId2, mockView2);
        activeQueryGroups.add(activeQueryGroup);
        deletedQueryGroups.add(deletedQueryGroup);

        TestTaskCancellationImpl taskCancellation = new TestTaskCancellationImpl(
            workloadManagementSettings,
            new DefaultTaskSelectionStrategy(),
            queryGroupLevelViews,
            activeQueryGroups,
            deletedQueryGroups,
            () -> false
        );

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        List<TaskCancellation> cancellableTasksFromDeletedQueryGroups = taskCancellation.getTaskCancellationsForDeletedQueryGroup(
            deletedQueryGroup
        );
        assertEquals(2, cancellableTasksFromDeletedQueryGroups.size());
        assertEquals(1000, cancellableTasksFromDeletedQueryGroups.get(0).getTask().getId());
        assertEquals(1001, cancellableTasksFromDeletedQueryGroups.get(1).getTask().getId());

        taskCancellation.cancelTasks();

        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
        assertFalse(cancellableTasksFromDeletedQueryGroups.get(0).getTask().isCancelled());
        assertFalse(cancellableTasksFromDeletedQueryGroups.get(1).getTask().isCancelled());
    }

    public void testCancelTasks_cancelsGivenTasks_WhenNodeInDuress() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage cpuUsage1 = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage memoryUsage1 = mock(QueryGroupMemoryUsage.class);
        QueryGroupCpuUsage cpuUsage2 = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage memoryUsage2 = mock(QueryGroupMemoryUsage.class);
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroup queryGroup2 = new QueryGroup(
            "testQueryGroup",
            queryGroupId2,
            QueryGroup.ResiliencyMode.SOFT,
            Map.of(resourceType, threshold),
            1L
        );

        when(cpuUsage1.isBreachingThresholdFor(any(), any())).thenReturn(true);
        when(cpuUsage2.isBreachingThresholdFor(any(), any())).thenReturn(true);
        when(memoryUsage2.isBreachingThresholdFor(any(), any())).thenReturn(false);
        when(memoryUsage1.isBreachingThresholdFor(any(), any())).thenReturn(false);

        when(cpuUsage1.getReduceByFor(any(), any())).thenReturn(0.001);
        when(cpuUsage2.getReduceByFor(any(), any())).thenReturn(0.001);
        when(memoryUsage2.getReduceByFor(any(), any())).thenReturn(0.0);
        when(memoryUsage1.getReduceByFor(any(), any())).thenReturn(0.0);

        QueryGroupLevelResourceUsageView mockView1 = createResourceUsageViewMock();
        when(mockView1.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage1, ResourceType.MEMORY, memoryUsage1));
        queryGroupLevelViews.put(queryGroupId1, mockView1);
        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(5678), getRandomSearchTask(8765)));
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage2, ResourceType.MEMORY, memoryUsage2));
        queryGroupLevelViews.put(queryGroupId2, mockView);
        Collections.addAll(activeQueryGroups, queryGroup1, queryGroup2);

        TestTaskCancellationImpl taskCancellation = new TestTaskCancellationImpl(
            workloadManagementSettings,
            new DefaultTaskSelectionStrategy(),
            queryGroupLevelViews,
            activeQueryGroups,
            deletedQueryGroups,
            () -> true
        );

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        List<TaskCancellation> cancellableTasksFrom1 = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.SOFT);
        assertEquals(2, cancellableTasksFrom1.size());
        assertEquals(5678, cancellableTasksFrom1.get(0).getTask().getId());
        assertEquals(8765, cancellableTasksFrom1.get(1).getTask().getId());

        taskCancellation.cancelTasks();
        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
        assertTrue(cancellableTasksFrom1.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom1.get(1).getTask().isCancelled());
    }

    public void testGetAllCancellableTasks_ReturnsNoTasksWhenNotBreachingThresholds() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage queryGroupCpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage queryGroupMemoryUsage = mock(QueryGroupMemoryUsage.class);
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );
        when(queryGroupCpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);
        when(queryGroupMemoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);
        when(queryGroupCpuUsage.getReduceByFor(any(), any())).thenReturn(0.0);
        when(queryGroupCpuUsage.getReduceByFor(any(), any())).thenReturn(0.0);
        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, queryGroupCpuUsage, ResourceType.MEMORY, queryGroupMemoryUsage)
        );
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertTrue(allCancellableTasks.isEmpty());
    }

    public void testGetAllCancellableTasks_ReturnsTasksWhenBreachingThresholds() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage queryGroupCpuUsage = mock(QueryGroupCpuUsage.class);
        QueryGroupMemoryUsage queryGroupMemoryUsage = mock(QueryGroupMemoryUsage.class);
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );
        when(queryGroupCpuUsage.isBreachingThresholdFor(any(), any())).thenReturn(true);
        when(queryGroupCpuUsage.getReduceByFor(any(), any())).thenReturn(0.005);
        when(queryGroupMemoryUsage.isBreachingThresholdFor(any(), any())).thenReturn(false);
        when(queryGroupMemoryUsage.getReduceByFor(any(), any())).thenReturn(0.0);

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, queryGroupCpuUsage, ResourceType.MEMORY, queryGroupMemoryUsage)
        );
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertEquals(2, allCancellableTasks.size());
        assertEquals(1234, allCancellableTasks.get(0).getTask().getId());
        assertEquals(4321, allCancellableTasks.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_doesNotReturnTasksWhenQueryGroupIdNotFound() {
        ResourceType resourceType = ResourceType.CPU;
        QueryGroupCpuUsage usage = mock(QueryGroupCpuUsage.class);
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup1",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );
        QueryGroup queryGroup2 = new QueryGroup(
            "testQueryGroup2",
            queryGroupId2,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        activeQueryGroups.add(queryGroup2);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(queryGroup2);
        assertEquals(0, cancellableTasksFrom.size());
    }

    private QueryGroupLevelResourceUsageView createResourceUsageViewMock() {
        QueryGroupLevelResourceUsageView mockView = mock(QueryGroupLevelResourceUsageView.class);
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(1234), getRandomSearchTask(4321)));
        return mockView;
    }

    private QueryGroupLevelResourceUsageView createResourceUsageViewMock(
        ResourceType resourceType,
        QueryGroupResourceUsage usage,
        Collection<Integer> ids
    ) {
        QueryGroupLevelResourceUsageView mockView = mock(QueryGroupLevelResourceUsageView.class);
        when(mockView.getResourceUsageData()).thenReturn(Collections.singletonMap(resourceType, usage));
        when(mockView.getActiveTasks()).thenReturn(ids.stream().map(this::getRandomSearchTask).collect(Collectors.toList()));
        return mockView;
    }

    private QueryGroupTask getRandomSearchTask(long id) {
        return new SearchTask(
            id,
            "transport",
            SearchAction.NAME,
            () -> "test description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap()
        );
    }
}
