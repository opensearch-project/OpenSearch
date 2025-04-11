/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.cancellation;

import org.opensearch.action.search.SearchAction;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.TaskCancellation;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WlmMode;
import org.opensearch.wlm.WorkloadGroupLevelResourceUsageView;
import org.opensearch.wlm.WorkloadGroupTask;
import org.opensearch.wlm.WorkloadGroupsStateAccessor;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.stats.WorkloadGroupState;
import org.opensearch.wlm.tracker.ResourceUsageCalculatorTrackerServiceTests.TestClock;
import org.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkloadGroupTaskCancellationServiceTests extends OpenSearchTestCase {
    private static final String workloadGroupId1 = "workloadGroup1";
    private static final String workloadGroupId2 = "workloadGroup2";

    private TestClock clock;

    private Map<String, WorkloadGroupLevelResourceUsageView> workloadGroupLevelViews;
    private Set<WorkloadGroup> activeWorkloadGroups;
    private Set<WorkloadGroup> deletedWorkloadGroups;
    private WorkloadGroupTaskCancellationService taskCancellation;
    private WorkloadManagementSettings workloadManagementSettings;
    private WorkloadGroupResourceUsageTrackerService resourceUsageTrackerService;
    private WorkloadGroupsStateAccessor stateAccessor;

    @Before
    public void setup() {
        workloadManagementSettings = mock(WorkloadManagementSettings.class);
        workloadGroupLevelViews = new HashMap<>();
        activeWorkloadGroups = new HashSet<>();
        deletedWorkloadGroups = new HashSet<>();

        clock = new TestClock();
        when(workloadManagementSettings.getNodeLevelCpuCancellationThreshold()).thenReturn(0.9);
        when(workloadManagementSettings.getNodeLevelMemoryCancellationThreshold()).thenReturn(0.9);
        resourceUsageTrackerService = mock(WorkloadGroupResourceUsageTrackerService.class);
        stateAccessor = mock(WorkloadGroupsStateAccessor.class);
        when(stateAccessor.getWorkloadGroupState(any())).thenReturn(new WorkloadGroupState());
        taskCancellation = new WorkloadGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService,
            stateAccessor
        );
    }

    public void testGetCancellableTasksFrom_setupAppropriateCancellationReasonAndScore() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage = 0.11;
        double memoryUsage = 0.0;
        Double threshold = 0.1;

        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );
        clock.fastForwardBy(1000);

        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(resourceType, cpuUsage, ResourceType.MEMORY, memoryUsage));
        workloadGroupLevelViews.put(workloadGroupId1, mockView);
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(List.of(workloadGroup1));
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

        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(resourceType, cpuUsage, ResourceType.MEMORY, memoryUsage));
        workloadGroupLevelViews.put(workloadGroupId1, mockView);
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(List.of(workloadGroup1));
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_returnsTasksWhenBreachingThresholdForMemory() {
        ResourceType resourceType = ResourceType.MEMORY;
        double cpuUsage = 0.0;
        double memoryUsage = 0.11;
        Double threshold = 0.1;

        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, resourceType, memoryUsage));

        workloadGroupLevelViews.put(workloadGroupId1, mockView);
        activeWorkloadGroups.add(workloadGroup1);
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(
            ResiliencyMode.ENFORCED,
            activeWorkloadGroups
        );
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_returnsNoTasksWhenNotBreachingThreshold() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage = 0.81;
        double memoryUsage = 0.0;
        Double threshold = 0.9;
        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, ResourceType.MEMORY, memoryUsage));
        workloadGroupLevelViews.put(workloadGroupId1, mockView);
        activeWorkloadGroups.add(workloadGroup1);
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(List.of(workloadGroup1));
        assertTrue(cancellableTasksFrom.isEmpty());
    }

    public void testGetCancellableTasksFrom_filtersWorkloadGroupCorrectly() {
        ResourceType resourceType = ResourceType.CPU;
        double usage = 0.02;
        Double threshold = 0.01;

        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        workloadGroupLevelViews.put(workloadGroupId1, mockView);
        activeWorkloadGroups.add(workloadGroup1);
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        WorkloadGroupTaskCancellationService taskCancellation = new WorkloadGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService,
            stateAccessor
        );

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(ResiliencyMode.SOFT, activeWorkloadGroups);
        assertEquals(0, cancellableTasksFrom.size());
    }

    public void testCancelTasks_cancelsGivenTasks() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage = 0.011;
        double memoryUsage = 0.011;

        Double threshold = 0.01;

        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold, ResourceType.MEMORY, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, ResourceType.MEMORY, memoryUsage));

        workloadGroupLevelViews.put(workloadGroupId1, mockView);
        activeWorkloadGroups.add(workloadGroup1);

        WorkloadGroupTaskCancellationService taskCancellation = new WorkloadGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService,
            stateAccessor
        );

        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(
            ResiliencyMode.ENFORCED,
            activeWorkloadGroups
        );
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        when(resourceUsageTrackerService.constructWorkloadGroupLevelUsageViews()).thenReturn(workloadGroupLevelViews);
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        taskCancellation.cancelTasks(() -> false, activeWorkloadGroups, deletedWorkloadGroups);
        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
    }

    public void testCancelTasks_cancelsTasksFromDeletedWorkloadGroups() {
        ResourceType resourceType = ResourceType.CPU;
        double activeWorkloadGroupCpuUsage = 0.01;
        double activeWorkloadGroupMemoryUsage = 0.0;
        double deletedWorkloadGroupCpuUsage = 0.01;
        double deletedWorkloadGroupMemoryUsage = 0.0;
        Double threshold = 0.01;

        WorkloadGroup activeWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroup deletedWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId2,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView1 = createResourceUsageViewMock();
        WorkloadGroupLevelResourceUsageView mockView2 = createResourceUsageViewMock(
            resourceType,
            deletedWorkloadGroupCpuUsage,
            List.of(1000, 1001)
        );

        when(mockView1.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, activeWorkloadGroupCpuUsage, ResourceType.MEMORY, activeWorkloadGroupMemoryUsage)
        );
        when(mockView2.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, deletedWorkloadGroupCpuUsage, ResourceType.MEMORY, deletedWorkloadGroupMemoryUsage)
        );
        workloadGroupLevelViews.put(workloadGroupId1, mockView1);
        workloadGroupLevelViews.put(workloadGroupId2, mockView2);

        activeWorkloadGroups.add(activeWorkloadGroup);
        deletedWorkloadGroups.add(deletedWorkloadGroup);

        WorkloadGroupTaskCancellationService taskCancellation = new WorkloadGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService,
            stateAccessor
        );
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(
            ResiliencyMode.ENFORCED,
            activeWorkloadGroups
        );
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        List<TaskCancellation> cancellableTasksFromDeletedWorkloadGroups = taskCancellation.getAllCancellableTasks(
            List.of(deletedWorkloadGroup)
        );
        assertEquals(2, cancellableTasksFromDeletedWorkloadGroups.size());
        assertEquals(1000, cancellableTasksFromDeletedWorkloadGroups.get(0).getTask().getId());
        assertEquals(1001, cancellableTasksFromDeletedWorkloadGroups.get(1).getTask().getId());

        when(resourceUsageTrackerService.constructWorkloadGroupLevelUsageViews()).thenReturn(workloadGroupLevelViews);
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        taskCancellation.cancelTasks(() -> true, activeWorkloadGroups, deletedWorkloadGroups);

        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
        assertTrue(cancellableTasksFromDeletedWorkloadGroups.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFromDeletedWorkloadGroups.get(1).getTask().isCancelled());
    }

    public void testCancelTasks_does_not_cancelTasksFromDeletedWorkloadGroups_whenNodeNotInDuress() {
        ResourceType resourceType = ResourceType.CPU;
        double activeWorkloadGroupCpuUsage = 0.11;
        double activeWorkloadGroupMemoryUsage = 0.0;
        double deletedWorkloadGroupCpuUsage = 0.11;
        double deletedWorkloadGroupMemoryUsage = 0.0;

        Double threshold = 0.01;

        WorkloadGroup activeWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroup deletedWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId2,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView1 = createResourceUsageViewMock();
        WorkloadGroupLevelResourceUsageView mockView2 = createResourceUsageViewMock(
            resourceType,
            deletedWorkloadGroupCpuUsage,
            List.of(1000, 1001)
        );

        when(mockView1.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, activeWorkloadGroupCpuUsage, ResourceType.MEMORY, activeWorkloadGroupMemoryUsage)
        );
        when(mockView2.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, deletedWorkloadGroupCpuUsage, ResourceType.MEMORY, deletedWorkloadGroupMemoryUsage)
        );

        workloadGroupLevelViews.put(workloadGroupId1, mockView1);
        workloadGroupLevelViews.put(workloadGroupId2, mockView2);
        activeWorkloadGroups.add(activeWorkloadGroup);
        deletedWorkloadGroups.add(deletedWorkloadGroup);

        WorkloadGroupTaskCancellationService taskCancellation = new WorkloadGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService,
            stateAccessor
        );
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(
            ResiliencyMode.ENFORCED,
            activeWorkloadGroups
        );
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        List<TaskCancellation> cancellableTasksFromDeletedWorkloadGroups = taskCancellation.getAllCancellableTasks(
            List.of(deletedWorkloadGroup)
        );
        assertEquals(2, cancellableTasksFromDeletedWorkloadGroups.size());
        assertEquals(1000, cancellableTasksFromDeletedWorkloadGroups.get(0).getTask().getId());
        assertEquals(1001, cancellableTasksFromDeletedWorkloadGroups.get(1).getTask().getId());

        when(resourceUsageTrackerService.constructWorkloadGroupLevelUsageViews()).thenReturn(workloadGroupLevelViews);
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        taskCancellation.cancelTasks(() -> false, activeWorkloadGroups, deletedWorkloadGroups);

        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
        assertFalse(cancellableTasksFromDeletedWorkloadGroups.get(0).getTask().isCancelled());
        assertFalse(cancellableTasksFromDeletedWorkloadGroups.get(1).getTask().isCancelled());
    }

    public void testCancelTasks_cancelsGivenTasks_WhenNodeInDuress() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage1 = 0.11;
        double memoryUsage1 = 0.0;
        double cpuUsage2 = 0.11;
        double memoryUsage2 = 0.0;
        Double threshold = 0.01;

        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroup workloadGroup2 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId2,
            new MutableWorkloadGroupFragment(ResiliencyMode.SOFT, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView1 = createResourceUsageViewMock();
        when(mockView1.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage1, ResourceType.MEMORY, memoryUsage1));
        workloadGroupLevelViews.put(workloadGroupId1, mockView1);
        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(5678), getRandomSearchTask(8765)));
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage2, ResourceType.MEMORY, memoryUsage2));
        workloadGroupLevelViews.put(workloadGroupId2, mockView);
        Collections.addAll(activeWorkloadGroups, workloadGroup1, workloadGroup2);

        WorkloadGroupTaskCancellationService taskCancellation = new WorkloadGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService,
            stateAccessor
        );

        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(
            ResiliencyMode.ENFORCED,
            activeWorkloadGroups
        );
        assertEquals(2, cancellableTasksFrom.size());
        assertEquals(1234, cancellableTasksFrom.get(0).getTask().getId());
        assertEquals(4321, cancellableTasksFrom.get(1).getTask().getId());

        List<TaskCancellation> cancellableTasksFrom1 = taskCancellation.getAllCancellableTasks(ResiliencyMode.SOFT, activeWorkloadGroups);
        assertEquals(2, cancellableTasksFrom1.size());
        assertEquals(5678, cancellableTasksFrom1.get(0).getTask().getId());
        assertEquals(8765, cancellableTasksFrom1.get(1).getTask().getId());

        when(resourceUsageTrackerService.constructWorkloadGroupLevelUsageViews()).thenReturn(workloadGroupLevelViews);
        when(workloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        taskCancellation.cancelTasks(() -> true, activeWorkloadGroups, deletedWorkloadGroups);
        assertTrue(cancellableTasksFrom.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom.get(1).getTask().isCancelled());
        assertTrue(cancellableTasksFrom1.get(0).getTask().isCancelled());
        assertTrue(cancellableTasksFrom1.get(1).getTask().isCancelled());
    }

    public void testGetAllCancellableTasks_ReturnsNoTasksWhenNotBreachingThresholds() {
        ResourceType resourceType = ResourceType.CPU;
        double workloadGroupCpuUsage = 0.09;
        double workloadGroupMemoryUsage = 0.0;
        Double threshold = 0.1;

        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(
            Map.of(ResourceType.CPU, workloadGroupCpuUsage, ResourceType.MEMORY, workloadGroupMemoryUsage)
        );
        workloadGroupLevelViews.put(workloadGroupId1, mockView);
        activeWorkloadGroups.add(workloadGroup1);
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks(ResiliencyMode.ENFORCED, activeWorkloadGroups);
        assertTrue(allCancellableTasks.isEmpty());
    }

    public void testGetAllCancellableTasks_ReturnsTasksWhenBreachingThresholds() {
        ResourceType resourceType = ResourceType.CPU;
        double cpuUsage = 0.11;
        double memoryUsage = 0.0;
        Double threshold = 0.01;

        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        when(mockView.getResourceUsageData()).thenReturn(Map.of(ResourceType.CPU, cpuUsage, ResourceType.MEMORY, memoryUsage));
        workloadGroupLevelViews.put(workloadGroupId1, mockView);
        activeWorkloadGroups.add(workloadGroup1);
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> allCancellableTasks = taskCancellation.getAllCancellableTasks(ResiliencyMode.ENFORCED, activeWorkloadGroups);
        assertEquals(2, allCancellableTasks.size());
        assertEquals(1234, allCancellableTasks.get(0).getTask().getId());
        assertEquals(4321, allCancellableTasks.get(1).getTask().getId());
    }

    public void testGetCancellableTasksFrom_doesNotReturnTasksWhenWorkloadGroupIdNotFound() {
        ResourceType resourceType = ResourceType.CPU;
        double usage = 0.11;
        Double threshold = 0.01;

        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup1",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );
        WorkloadGroup workloadGroup2 = new WorkloadGroup(
            "testWorkloadGroup2",
            workloadGroupId2,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(resourceType, threshold)),
            1L
        );

        WorkloadGroupLevelResourceUsageView mockView = createResourceUsageViewMock();
        workloadGroupLevelViews.put(workloadGroupId1, mockView);
        activeWorkloadGroups.add(workloadGroup1);
        activeWorkloadGroups.add(workloadGroup2);
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        List<TaskCancellation> cancellableTasksFrom = taskCancellation.getAllCancellableTasks(List.of(workloadGroup2));
        assertEquals(0, cancellableTasksFrom.size());
    }

    public void testPruneDeletedWorkloadGroups() {
        WorkloadGroup workloadGroup1 = new WorkloadGroup(
            "testWorkloadGroup1",
            workloadGroupId1,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.2)),
            1L
        );
        WorkloadGroup workloadGroup2 = new WorkloadGroup(
            "testWorkloadGroup2",
            workloadGroupId2,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.1)),
            1L
        );
        List<WorkloadGroup> deletedWorkloadGroups = new ArrayList<>();
        deletedWorkloadGroups.add(workloadGroup1);
        deletedWorkloadGroups.add(workloadGroup2);
        WorkloadGroupLevelResourceUsageView resourceUsageView1 = createResourceUsageViewMock();

        List<WorkloadGroupTask> activeTasks = IntStream.range(0, 5).mapToObj(this::getRandomSearchTask).collect(Collectors.toList());
        when(resourceUsageView1.getActiveTasks()).thenReturn(activeTasks);

        WorkloadGroupLevelResourceUsageView resourceUsageView2 = createResourceUsageViewMock();
        when(resourceUsageView2.getActiveTasks()).thenReturn(new ArrayList<>());

        workloadGroupLevelViews.put(workloadGroupId1, resourceUsageView1);
        workloadGroupLevelViews.put(workloadGroupId2, resourceUsageView2);

        WorkloadGroupTaskCancellationService taskCancellation = new WorkloadGroupTaskCancellationService(
            workloadManagementSettings,
            new MaximumResourceTaskSelectionStrategy(),
            resourceUsageTrackerService,
            stateAccessor
        );
        taskCancellation.workloadGroupLevelResourceUsageViews = workloadGroupLevelViews;

        taskCancellation.pruneDeletedWorkloadGroups(deletedWorkloadGroups);

        assertEquals(1, deletedWorkloadGroups.size());
        assertEquals(workloadGroupId1, deletedWorkloadGroups.get(0).get_id());

    }

    private WorkloadGroupLevelResourceUsageView createResourceUsageViewMock() {
        WorkloadGroupLevelResourceUsageView mockView = mock(WorkloadGroupLevelResourceUsageView.class);
        when(mockView.getActiveTasks()).thenReturn(List.of(getRandomSearchTask(1234), getRandomSearchTask(4321)));
        return mockView;
    }

    private WorkloadGroupLevelResourceUsageView createResourceUsageViewMock(
        ResourceType resourceType,
        double usage,
        Collection<Integer> ids
    ) {
        WorkloadGroupLevelResourceUsageView mockView = mock(WorkloadGroupLevelResourceUsageView.class);
        when(mockView.getResourceUsageData()).thenReturn(Collections.singletonMap(resourceType, usage));
        when(mockView.getActiveTasks()).thenReturn(ids.stream().map(this::getRandomSearchTask).collect(Collectors.toList()));
        return mockView;
    }

    private WorkloadGroupTask getRandomSearchTask(long id) {
        return new WorkloadGroupTask(
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
