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
import org.opensearch.wlm.ResourceType;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.QueryGroupLevelResourceUsageView;
import org.opensearch.wlm.WorkloadManagementSettings;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultTaskCancellationTests extends OpenSearchTestCase {
    private static final String queryGroupId1 = "queryGroup1";
    private static final String queryGroupId2 = "queryGroup2";

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
        long usage = 100_000_000L;
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );
        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);
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
        long usage = 100_000_000L;
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );
        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);
        queryGroupLevelViews.put(queryGroupId1, mockView);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(queryGroup1);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_returnsTasksWhenBreachingThresholdForMemory() {
        ResourceType resourceType = ResourceType.MEMORY;
        long usage = 900_000_000_000L;
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_returnsNoTasksWhenNotBreachingThreshold() {
        ResourceType resourceType = ResourceType.CPU;
        long usage = 500L;
        Double threshold = 0.9;
        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        when(workloadManagementSettings.getNodeLevelCpuCancellationThreshold()).thenReturn(0.90);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(queryGroup1);
        assertTrue(cancellableTasksFrom.isEmpty());
    }

    public void testGetCancellableTasksFrom_filtersQueryGroupCorrectly() {
        ResourceType resourceType = ResourceType.CPU;
        long usage = 150_000_000L;
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);
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
        long usage = 150_000_000_000L;
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);
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
        long usage = 150_000_000_000L;
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

        QueryGroupLevelResourceUsageView mockView1 = createResourceUsageViewMock(resourceType, usage);
        QueryGroupLevelResourceUsageView mockView2 = createResourceUsageViewMock(resourceType, usage, List.of(1000, 1001));
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
        long usage = 150_000_000_000L;
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

        QueryGroupLevelResourceUsageView mockView1 = createResourceUsageViewMock(resourceType, usage);
        QueryGroupLevelResourceUsageView mockView2 = createResourceUsageViewMock(resourceType, usage, List.of(1000, 1001));
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
        long usage = 150_000_000_000L;
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

        queryGroupLevelViews.put(queryGroupId1, createResourceUsageViewMock(resourceType, usage));
        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(5678), getRandomSearchTask(8765)));
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

    public void testGetAllCancellableTasks_ReturnsNoTasksFromWhenNotBreachingThresholds() {
        ResourceType resourceType = ResourceType.CPU;
        long usage = 1L;
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertTrue(allCancellableTasks.isEmpty());
    }

    public void testGetAllCancellableTasks_ReturnsTasksFromWhenBreachingThresholds() {
        ResourceType resourceType = ResourceType.CPU;
        long usage = 150_000_000_000L;
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks(QueryGroup.ResiliencyMode.ENFORCED);
        assertEquals(2, allCancellableTasks.size());
        assertEquals(1234, allCancellableTasks.get(0).getTask().getId());
        assertEquals(4321, allCancellableTasks.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_doesNotReturnTasksWhenQueryGroupIdNotFound() {
        ResourceType resourceType = ResourceType.CPU;
        long usage = 150_000_000_000L;
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
            QueryGroup.ResiliencyMode.ENFORCED,
            Map.of(resourceType, threshold),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock(resourceType, usage);

        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        activeQueryGroups.add(queryGroup2);

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getCancellableTasksFrom(queryGroup2);
        assertEquals(0, cancellableTasksFrom.size());
    }

    private QueryGroupLevelResourceUsageView createResourceUsageViewMock(ResourceType resourceType, Long usage) {
        QueryGroupLevelResourceUsageView mockView = mock(QueryGroupLevelResourceUsageView.class);
        when(mockView.getResourceUsageData()).thenReturn(Collections.singletonMap(resourceType, usage));
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(1234), getRandomSearchTask(4321)));
        return mockView;
    }

    private QueryGroupLevelResourceUsageView createResourceUsageViewMock(ResourceType resourceType, Long usage, Collection<Integer> ids) {
        QueryGroupLevelResourceUsageView mockView = mock(QueryGroupLevelResourceUsageView.class);
        when(mockView.getResourceUsageData()).thenReturn(Collections.singletonMap(resourceType, usage));
        when(mockView.getActiveTasks()).thenReturn(ids.stream().map(this::getRandomSearchTask).collect(Collectors.toList()));
        return mockView;
    }

    private Task getRandomSearchTask(long id) {
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
