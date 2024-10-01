/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.action.search.SearchAction;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.MutableQueryGroupFragment;
import org.opensearch.wlm.MutableQueryGroupFragment.ResiliencyMode;
import org.opensearch.wlm.QueryGroupLevelResourceUsageView;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService;
import org.opensearch.wlm.tracker.ResourceUsageCalculatorTrackerServiceTests.TestClock;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryGroupTaskCancellationServiceTests extends OpenSearchTestCase {
    private static final String queryGroupId1 = "queryGroup1";
    private static final String queryGroupId2 = "queryGroup2";

    private TestClock clock;

    private Map<String, QueryGroupLevelResourceUsageView> queryGroupLevelViews;
    private Set<QueryGroup> activeQueryGroups;
    private Set<QueryGroup> deletedQueryGroups;
    private QueryGroupTaskCancellationService taskCancellation;
    private WorkloadManagementSettings workloadManagementSettings;
    private QueryGroupResourceUsageTrackerService resourceUsageTrackerService;

    @Before
    public void setup() {
        workloadManagementSettings = mock(WorkloadManagementSettings.class);
        queryGroupLevelViews = new HashMap<>();
        activeQueryGroups = new HashSet<>();
        deletedQueryGroups = new HashSet<>();

        clock = new TestClock();
        when(workloadManagementSettings.getNodeLevelCpuCancellationThreshold()).thenReturn(0.9);
        when(workloadManagementSettings.getNodeLevelMemoryCancellationThreshold()).thenReturn(0.9);
        resourceUsageTrackerService = mock(QueryGroupResourceUsageTrackerService.class);
        taskCancellation = new QueryGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService
        );
        taskCancellation.setQueryGroupStateMapAccessor((x) -> new QueryGroupState());
    }

    public void testGetCancellableTasksFrom_setupAppropriateCancellationReasonAndScore() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage = 0.11;
        double memoryUsage = 0.0;
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );
        clock.fastForwardBy(1000);

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(resourceType, cpuUsage, ResourceType.MEMORY, memoryUsage));
        queryGroupLevelViews.put(queryGroupId1, mockView);
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(List.of(queryGroup1));
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
        assertEquals(1, cancellableTasksFrom.get(0).getReasons().get(0).getCancellationScore());
    }

    public void testGetCancellableTasksFrom_returnsTasksWhenBreachingThreshold() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage = 0.11;
        double memoryUsage = 0.0;
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(resourceType, cpuUsage, ResourceType.MEMORY, memoryUsage));
        queryGroupLevelViews.put(queryGroupId1, mockView);
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(List.of(queryGroup1));
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_returnsTasksWhenBreachingThresholdForMemory() {
        ResourceType resourceType = ResourceType.MEMORY;
        double cpuUsage = 0.0;
        double memoryUsage = 0.11;
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, resourceType, memoryUsage));

        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(ResiliencyMode.ENFORCED, activeQueryGroups);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_returnsNoTasksWhenNotBreachingThreshold() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage = 0.81;
        double memoryUsage = 0.0;
        Double threshold = 0.9;
        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, ResourceType.MEMORY, memoryUsage));
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(List.of(queryGroup1));
        assertTrue(cancellableTasksFrom.isEmpty());
    }

    public void testGetCancellableTasksFrom_filtersQueryGroupCorrectly() {
        ResourceType resourceType = ResourceType.CPU;
        double usage = 0.02;
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        QueryGroupTaskCancellationService taskCancellation = new QueryGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService
        );

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(ResiliencyMode.SOFT, activeQueryGroups);
        assertEquals(0, cancellableTasksFrom.size());
    }

    public void testCancelTasks_cancelsGivenTasks() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage = 0.011;
        double memoryUsage = 0.011;

        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold, ResourceType.MEMORY, threshold)),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, ResourceType.MEMORY, memoryUsage));

        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);

        QueryGroupTaskCancellationService taskCancellation = new QueryGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService
        );
        taskCancellation.setQueryGroupStateMapAccessor((x) -> new QueryGroupState());

        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(ResiliencyMode.ENFORCED, activeQueryGroups);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        when(resourceUsageTrackerService.constructQueryGroupLevelUsageViews()).thenReturn(queryGroupLevelViews);
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        taskCancellation.cancelTasks(() -> false, activeQueryGroups, deletedQueryGroups);
        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
    }

    public void testCancelTasks_cancelsTasksFromDeletedQueryGroups() {
        ResourceType resourceType = ResourceType.CPU;
        double activeQueryGroupCpuUsage = 0.01;
        double activeQueryGroupMemoryUsage = 0.0;
        double deletedQueryGroupCpuUsage = 0.01;
        double deletedQueryGroupMemoryUsage = 0.0;
        Double threshold = 0.01;

        QueryGroup activeQueryGroup = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroup deletedQueryGroup = new QueryGroup(
            "testQueryGroup",
            queryGroupId2,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

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

        QueryGroupTaskCancellationService taskCancellation = new QueryGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService
        );
        taskCancellation.setQueryGroupStateMapAccessor((x) -> new QueryGroupState());
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(ResiliencyMode.ENFORCED, activeQueryGroups);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        List<TaskCancellation> cancellableTasksFromDeletedQueryGroups = taskCancellation.getAllCancellableTasks(List.of(deletedQueryGroup));
        assertEquals(2, cancellableTasksFromDeletedQueryGroups.size());
        assertEquals(1000, cancellableTasksFromDeletedQueryGroups.get(0).getTask().getId());
        assertEquals(1001, cancellableTasksFromDeletedQueryGroups.get(1).getTask().getId());

        when(resourceUsageTrackerService.constructQueryGroupLevelUsageViews()).thenReturn(queryGroupLevelViews);
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        taskCancellation.cancelTasks(() -> true, activeQueryGroups, deletedQueryGroups);

        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
        assertTrue(cancellableTasksFromDeletedQueryGroups.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFromDeletedQueryGroups.get(1).getTask().isCancelled());
    }

    public void testCancelTasks_does_not_cancelTasksFromDeletedQueryGroups_whenNodeNotInDuress() {
        ResourceType resourceType = ResourceType.CPU;
        double activeQueryGroupCpuUsage = 0.11;
        double activeQueryGroupMemoryUsage = 0.0;
        double deletedQueryGroupCpuUsage = 0.11;
        double deletedQueryGroupMemoryUsage = 0.0;

        Double threshold = 0.01;

        QueryGroup activeQueryGroup = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroup deletedQueryGroup = new QueryGroup(
            "testQueryGroup",
            queryGroupId2,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

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

        QueryGroupTaskCancellationService taskCancellation = new QueryGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService
        );
        taskCancellation.setQueryGroupStateMapAccessor((x) -> new QueryGroupState());
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(ResiliencyMode.ENFORCED, activeQueryGroups);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        List<TaskCancellation> cancellableTasksFromDeletedQueryGroups = taskCancellation.getAllCancellableTasks(List.of(deletedQueryGroup));
        assertEquals(2, cancellableTasksFromDeletedQueryGroups.size());
        assertEquals(1000, cancellableTasksFromDeletedQueryGroups.get(0).getTask().getId());
        assertEquals(1001, cancellableTasksFromDeletedQueryGroups.get(1).getTask().getId());

        when(resourceUsageTrackerService.constructQueryGroupLevelUsageViews()).thenReturn(queryGroupLevelViews);
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        taskCancellation.cancelTasks(() -> false, activeQueryGroups, deletedQueryGroups);

        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
        assertFalse(cancellableTasksFromDeletedQueryGroups.get(0).getTask().isCancelled());
        assertFalse(cancellableTasksFromDeletedQueryGroups.get(1).getTask().isCancelled());
    }

    public void testCancelTasks_cancelsGivenTasks_WhenNodeInDuress() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage1 = 0.11;
        double memoryUsage1 = 0.0;
        double cpuUsage2 = 0.11;
        double memoryUsage2 = 0.0;
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroup queryGroup2 = new QueryGroup(
            "testQueryGroup",
            queryGroupId2,
            new MutableQueryGroupFragment(ResiliencyMode.SOFT, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroupLevelResourceUsageView mockView1 = createResourceUsageViewMock();
        when(mockView1.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage1, ResourceType.MEMORY, memoryUsage1));
        queryGroupLevelViews.put(queryGroupId1, mockView1);
        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(5678), getRandomSearchTask(8765)));
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage2, ResourceType.MEMORY, memoryUsage2));
        queryGroupLevelViews.put(queryGroupId2, mockView);
        Collections.addAll(activeQueryGroups, queryGroup1, queryGroup2);

        QueryGroupTaskCancellationService taskCancellation = new QueryGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService
        );
        taskCancellation.setQueryGroupStateMapAccessor((x) -> new QueryGroupState());

        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(ResiliencyMode.ENFORCED, activeQueryGroups);
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        List<TaskCancellation> cancellableTasksFrom1 = taskCancellation.getAllCancellableTasks(ResiliencyMode.SOFT, activeQueryGroups);
        assertEquals(2, cancellableTasksFrom1.size());
        assertEquals(5678, cancellableTasksFrom1.get(0).getTask().getId());
        assertEquals(8765, cancellableTasksFrom1.get(1).getTask().getId());

        when(resourceUsageTrackerService.constructQueryGroupLevelUsageViews()).thenReturn(queryGroupLevelViews);
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        taskCancellation.cancelTasks(() -> true, activeQueryGroups, deletedQueryGroups);
        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
        assertTrue(cancellableTasksFrom1.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom1.get(1).getTask().isCancelled());
    }

    public void testGetAllCancellableTasks_ReturnsNoTasksWhenNotBreachingThresholds() {
        ResourceType resourceType = ResourceType.CPU;
        double queryGroupCpuUsage = 0.09;
        double queryGroupMemoryUsage = 0.0;
        Double threshold = 0.1;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, queryGroupCpuUsage, ResourceType.MEMORY, queryGroupMemoryUsage)
        );
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks(ResiliencyMode.ENFORCED, activeQueryGroups);
        assertTrue(allCancellableTasks.isEmpty());
    }

    public void testGetAllCancellableTasks_ReturnsTasksWhenBreachingThresholds() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage = 0.11;
        double memoryUsage = 0.0;
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, ResourceType.MEMORY, memoryUsage));
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks(ResiliencyMode.ENFORCED, activeQueryGroups);
        assertEquals(2, allCancellableTasks.size());
        assertEquals(1234, allCancellableTasks.get(0).getTask().getId());
        assertEquals(4321, allCancellableTasks.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_doesNotReturnTasksWhenQueryGroupIdNotFound() {
        ResourceType resourceType = ResourceType.CPU;
        double usage = 0.11;
        Double threshold = 0.01;

        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup1",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );
        QueryGroup queryGroup2 = new QueryGroup(
            "testQueryGroup2",
            queryGroupId2,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        QueryGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        queryGroupLevelViews.put(queryGroupId1, mockView);
        activeQueryGroups.add(queryGroup1);
        activeQueryGroups.add(queryGroup2);
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(List.of(queryGroup2));
        assertEquals(0, cancellableTasksFrom.size());
    }

    public void testPruneDeletedQueryGroups() {
        QueryGroup queryGroup1 = new QueryGroup(
            "testQueryGroup1",
            queryGroupId1,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.2)),
            1L
        );
        QueryGroup queryGroup2 = new QueryGroup(
            "testQueryGroup2",
            queryGroupId2,
            new MutableQueryGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.1)),
            1L
        );
        List<QueryGroup> deletedQueryGroups = new ArrayList<>();
        deletedQueryGroups.add(queryGroup1);
        deletedQueryGroups.add(queryGroup2);
        QueryGroupLevelResourceUsageView resourceUsageView1 = createResourceUsageViewMock();

        List<QueryGroupTask> activeTasks = IntStream.range(0, 5).mapToObj(this::getRandomSearchTask).collect(Collectors.toList());
        when(resourceUsageView1.getActiveTasks()).thenReturn(activeTasks);

        QueryGroupLevelResourceUsageView resourceUsageView2 = createResourceUsageViewMock();
        when(resourceUsageView2.getActiveTasks()).thenReturn(new ArrayList<>());

        queryGroupLevelViews.put(queryGroupId1, resourceUsageView1);
        queryGroupLevelViews.put(queryGroupId2, resourceUsageView2);

        QueryGroupTaskCancellationService taskCancellation = new QueryGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService
        );
        taskCancellation.setQueryGroupStateMapAccessor((x) -> new QueryGroupState());
        taskCancellation.queryGroupLevelResourceUsageViews = queryGroupLevelViews;

        taskCancellation.pruneDeletedQueryGroups(deletedQueryGroups);

        assertEquals(1, deletedQueryGroups.size());
        assertEquals(queryGroupId1, deletedQueryGroups.get(0).get_id());

    }

    private QueryGroupLevelResourceUsageView createResourceUsageViewMock() {
        QueryGroupLevelResourceUsageView mockView = mock(QueryGroupLevelResourceUsageView.class);
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(1234), getRandomSearchTask(4321)));
        return mockView;
    }

    private QueryGroupLevelResourceUsageView createResourceUsageViewMock(ResourceType resourceType, double usage, Collection<Integer> ids) {
        QueryGroupLevelResourceUsageView mockView = mock(QueryGroupLevelResourceUsageView.class);
        when(mockView.getResourceUsageData()).thenReturn(Collections.singletonMap(resourceType, usage));
        when(mockView.getActiveTasks()).thenReturn(ids.stream().map(this::getRandomSearchTask).collect(Collectors.toList()));
        return mockView;
    }

    private QueryGroupTask getRandomSearchTask(long id) {
        return new QueryGroupTask(
            id,
            "transport",
            SearchAction.NAME,
            "test description",
            new TaskId(randomLong() + ":" + randomLong()),
            Collections.emptyMap(),
            null,
            clock::getTime
        );
    }
}
