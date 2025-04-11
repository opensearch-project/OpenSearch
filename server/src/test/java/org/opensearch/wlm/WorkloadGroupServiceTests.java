/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.search.SearchTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.opensearch.wlm.cancellation.TaskSelectionStrategy;
import org.opensearch.wlm.stats.WorkloadGroupState;
import org.opensearch.wlm.tracker.WorkloadGroupResourceUsageTrackerService;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.opensearch.wlm.tracker.ResourceUsageCalculatorTests.createMockTaskWithResourceStats;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WorkloadGroupServiceTests extends OpenSearchTestCase {
    public static final String QUERY_GROUP_ID = "queryGroupId1";
    private WorkloadGroupService queryGroupService;
    private WorkloadGroupTaskCancellationService mockCancellationService;
    private ClusterService mockClusterService;
    private ThreadPool mockThreadPool;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    private Scheduler.Cancellable mockScheduledFuture;
    private Map<String, WorkloadGroupState> mockWorkloadGroupStateMap;
    NodeDuressTrackers mockNodeDuressTrackers;
    WorkloadGroupsStateAccessor mockWorkloadGroupsStateAccessor;

    public void setUp() throws Exception {
        super.setUp();
        mockClusterService = Mockito.mock(ClusterService.class);
        mockThreadPool = Mockito.mock(ThreadPool.class);
        mockScheduledFuture = Mockito.mock(Scheduler.Cancellable.class);
        mockWorkloadManagementSettings = Mockito.mock(WorkloadManagementSettings.class);
        mockWorkloadGroupStateMap = new HashMap<>();
        mockNodeDuressTrackers = Mockito.mock(NodeDuressTrackers.class);
        mockCancellationService = Mockito.mock(TestWorkloadGroupCancellationService.class);
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor();
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(false);

        queryGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            new HashSet<>(),
            new HashSet<>()
        );
    }

    public void tearDown() throws Exception {
        super.tearDown();
        mockThreadPool.shutdown();
    }

    public void testClusterChanged() {
        ClusterChangedEvent mockClusterChangedEvent = Mockito.mock(ClusterChangedEvent.class);
        ClusterState mockPreviousClusterState = Mockito.mock(ClusterState.class);
        ClusterState mockClusterState = Mockito.mock(ClusterState.class);
        Metadata mockPreviousMetadata = Mockito.mock(Metadata.class);
        Metadata mockMetadata = Mockito.mock(Metadata.class);
        WorkloadGroup addedWorkloadGroup = new WorkloadGroup(
            "addedWorkloadGroup",
            "4242",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5)),
            1L
        );
        WorkloadGroup deletedWorkloadGroup = new WorkloadGroup(
            "deletedWorkloadGroup",
            "4241",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5)),
            1L
        );
        Map<String, WorkloadGroup> previousWorkloadGroups = new HashMap<>();
        previousWorkloadGroups.put("4242", addedWorkloadGroup);
        Map<String, WorkloadGroup> currentWorkloadGroups = new HashMap<>();
        currentWorkloadGroups.put("4241", deletedWorkloadGroup);

        when(mockClusterChangedEvent.previousState()).thenReturn(mockPreviousClusterState);
        when(mockClusterChangedEvent.state()).thenReturn(mockClusterState);
        when(mockPreviousClusterState.metadata()).thenReturn(mockPreviousMetadata);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockPreviousMetadata.queryGroups()).thenReturn(previousWorkloadGroups);
        when(mockMetadata.queryGroups()).thenReturn(currentWorkloadGroups);
        queryGroupService.clusterChanged(mockClusterChangedEvent);

        Set<WorkloadGroup> currentWorkloadGroupsExpected = Set.of(currentWorkloadGroups.get("4241"));
        Set<WorkloadGroup> previousWorkloadGroupsExpected = Set.of(previousWorkloadGroups.get("4242"));

        assertEquals(currentWorkloadGroupsExpected, queryGroupService.getActiveWorkloadGroups());
        assertEquals(previousWorkloadGroupsExpected, queryGroupService.getDeletedWorkloadGroups());
    }

    public void testDoStart_SchedulesTask() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockWorkloadManagementSettings.getWorkloadGroupServiceRunInterval()).thenReturn(TimeValue.timeValueSeconds(1));
        queryGroupService.doStart();
        Mockito.verify(mockThreadPool).scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class), eq(ThreadPool.Names.GENERIC));
    }

    public void testDoStop_CancelsScheduledTask() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockThreadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(mockScheduledFuture);
        queryGroupService.doStart();
        queryGroupService.doStop();
        Mockito.verify(mockScheduledFuture).cancel();
    }

    public void testDoRun_WhenModeEnabled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(true);
        // Call the method
        queryGroupService.doRun();

        // Verify that refreshWorkloadGroups was called

        // Verify that cancelTasks was called with a BooleanSupplier
        ArgumentCaptor<BooleanSupplier> booleanSupplierCaptor = ArgumentCaptor.forClass(BooleanSupplier.class);
        Mockito.verify(mockCancellationService).cancelTasks(booleanSupplierCaptor.capture(), any(), any());

        // Assert the behavior of the BooleanSupplier
        BooleanSupplier capturedSupplier = booleanSupplierCaptor.getValue();
        assertTrue(capturedSupplier.getAsBoolean());

    }

    public void testDoRun_WhenModeDisabled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(false);
        queryGroupService.doRun();
        // Verify that refreshWorkloadGroups was called

        Mockito.verify(mockCancellationService, never()).cancelTasks(any(), any(), any());

    }

    public void testRejectIfNeeded_whenWorkloadGroupIdIsNullOrDefaultOne() {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            "queryGroupId1",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.10)),
            1L
        );
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(testWorkloadGroup);
            }
        };
        mockWorkloadGroupStateMap = new HashMap<>();
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);
        mockWorkloadGroupStateMap.put("queryGroupId1", new WorkloadGroupState());

        Map<String, WorkloadGroupState> spyMap = spy(mockWorkloadGroupStateMap);

        queryGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        queryGroupService.rejectIfNeeded(null);

        verify(spyMap, never()).get(any());

        queryGroupService.rejectIfNeeded(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());
        verify(spyMap, never()).get(any());
    }

    public void testRejectIfNeeded_whenSoftModeWorkloadGroupIsContendedAndNodeInDuress() {
        Set<WorkloadGroup> activeWorkloadGroups = getActiveWorkloadGroups(
            "testWorkloadGroup",
            QUERY_GROUP_ID,
            MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockWorkloadGroupStateMap = new HashMap<>();
        mockWorkloadGroupStateMap.put("queryGroupId1", new WorkloadGroupState());
        WorkloadGroupState state = new WorkloadGroupState();
        WorkloadGroupState.ResourceTypeState cpuResourceState = new WorkloadGroupState.ResourceTypeState(ResourceType.CPU);
        cpuResourceState.setLastRecordedUsage(0.10);
        state.getResourceState().put(ResourceType.CPU, cpuResourceState);
        WorkloadGroupState spyState = spy(state);
        mockWorkloadGroupStateMap.put(QUERY_GROUP_ID, spyState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        queryGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(true);
        assertThrows(OpenSearchRejectedExecutionException.class, () -> queryGroupService.rejectIfNeeded("queryGroupId1"));
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsSoftMode() {
        Set<WorkloadGroup> activeWorkloadGroups = getActiveWorkloadGroups(
            "testWorkloadGroup",
            QUERY_GROUP_ID,
            MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockWorkloadGroupStateMap = new HashMap<>();
        WorkloadGroupState spyState = spy(new WorkloadGroupState());
        mockWorkloadGroupStateMap.put("queryGroupId1", spyState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        queryGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        queryGroupService.rejectIfNeeded("queryGroupId1");

        verify(spyState, never()).getResourceState();
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsEnforcedMode_andNotBreaching() {
        WorkloadGroup testWorkloadGroup = getWorkloadGroup(
            "testWorkloadGroup",
            "queryGroupId1",
            MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
            Map.of(ResourceType.CPU, 0.10)
        );
        WorkloadGroup spuWorkloadGroup = spy(testWorkloadGroup);
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(spuWorkloadGroup);
            }
        };
        mockWorkloadGroupStateMap = new HashMap<>();
        WorkloadGroupState queryGroupState = new WorkloadGroupState();
        queryGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.05);

        mockWorkloadGroupStateMap.put("queryGroupId1", queryGroupState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        queryGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockWorkloadManagementSettings.getNodeLevelCpuRejectionThreshold()).thenReturn(0.8);
        queryGroupService.rejectIfNeeded("queryGroupId1");

        // verify the check to compare the current usage and limit
        // this should happen 3 times => 2 to check whether the resource limit has the TRACKED resource type and 1 to get the value
        verify(spuWorkloadGroup, times(3)).getResourceLimits();
        assertEquals(0, queryGroupState.getResourceState().get(ResourceType.CPU).rejections.count());
        assertEquals(0, queryGroupState.totalRejections.count());
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsEnforcedMode_andBreaching() {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            "queryGroupId1",
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.10, ResourceType.MEMORY, 0.10)
            ),
            1L
        );
        WorkloadGroup spuWorkloadGroup = spy(testWorkloadGroup);
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(spuWorkloadGroup);
            }
        };
        mockWorkloadGroupStateMap = new HashMap<>();
        WorkloadGroupState queryGroupState = new WorkloadGroupState();
        queryGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.18);
        queryGroupState.getResourceState().get(ResourceType.MEMORY).setLastRecordedUsage(0.18);
        WorkloadGroupState spyState = spy(queryGroupState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        mockWorkloadGroupStateMap.put("queryGroupId1", spyState);

        queryGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        assertThrows(OpenSearchRejectedExecutionException.class, () -> queryGroupService.rejectIfNeeded("queryGroupId1"));

        // verify the check to compare the current usage and limit
        // this should happen 3 times => 1 to check whether the resource limit has the TRACKED resource type and 1 to get the value
        // because it will break out of the loop since the limits are breached
        verify(spuWorkloadGroup, times(2)).getResourceLimits();
        assertEquals(
            1,
            queryGroupState.getResourceState().get(ResourceType.CPU).rejections.count() + queryGroupState.getResourceState()
                .get(ResourceType.MEMORY).rejections.count()
        );
        assertEquals(1, queryGroupState.totalRejections.count());
    }

    public void testRejectIfNeeded_whenFeatureIsNotEnabled() {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            "queryGroupId1",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.10)),
            1L
        );
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(testWorkloadGroup);
            }
        };
        mockWorkloadGroupStateMap = new HashMap<>();
        mockWorkloadGroupStateMap.put("queryGroupId1", new WorkloadGroupState());

        Map<String, WorkloadGroupState> spyMap = spy(mockWorkloadGroupStateMap);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        queryGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);

        queryGroupService.rejectIfNeeded(testWorkloadGroup.get_id());
        verify(spyMap, never()).get(any());
    }

    public void testOnTaskCompleted() {
        Task task = new SearchTask(12, "", "", () -> "", null, null);
        mockThreadPool = new TestThreadPool("queryGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testId");
        WorkloadGroupState queryGroupState = new WorkloadGroupState();
        mockWorkloadGroupStateMap.put("testId", queryGroupState);
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);
        queryGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            new HashSet<>() {
                {
                    add(
                        new WorkloadGroup(
                            "testWorkloadGroup",
                            "testId",
                            new MutableWorkloadGroupFragment(
                                MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                                Map.of(ResourceType.CPU, 0.10, ResourceType.MEMORY, 0.10)
                            ),
                            1L
                        )
                    );
                }
            },
            new HashSet<>()
        );

        ((WorkloadGroupTask) task).setWorkloadGroupId(mockThreadPool.getThreadContext());
        queryGroupService.onTaskCompleted(task);

        assertEquals(1, queryGroupState.totalCompletions.count());

        // test non WorkloadGroupTask
        task = new Task(1, "simple", "test", "mock task", null, null);
        queryGroupService.onTaskCompleted(task);

        // It should still be 1
        assertEquals(1, queryGroupState.totalCompletions.count());

        mockThreadPool.shutdown();
    }

    public void testShouldSBPHandle() {
        SearchTask task = createMockTaskWithResourceStats(SearchTask.class, 100, 200, 0, 12);
        WorkloadGroupState queryGroupState = new WorkloadGroupState();
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>();
        mockWorkloadGroupStateMap.put("testId", queryGroupState);
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);
        queryGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            Collections.emptySet()
        );

        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);

        // Default queryGroupId
        mockThreadPool = new TestThreadPool("queryGroupServiceTests");
        mockThreadPool.getThreadContext()
            .putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());
        // we haven't set the queryGroupId yet SBP should still track the task for cancellation
        assertTrue(queryGroupService.shouldSBPHandle(task));
        task.setWorkloadGroupId(mockThreadPool.getThreadContext());
        assertTrue(queryGroupService.shouldSBPHandle(task));

        mockThreadPool.shutdownNow();

        // invalid queryGroup task
        mockThreadPool = new TestThreadPool("queryGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testId");
        task.setWorkloadGroupId(mockThreadPool.getThreadContext());
        assertTrue(queryGroupService.shouldSBPHandle(task));

        // Valid query group task but wlm not enabled
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        activeWorkloadGroups.add(
            new WorkloadGroup(
                "testWorkloadGroup",
                "testId",
                new MutableWorkloadGroupFragment(
                    MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.CPU, 0.10, ResourceType.MEMORY, 0.10)
                ),
                1L
            )
        );
        assertTrue(queryGroupService.shouldSBPHandle(task));

        mockThreadPool.shutdownNow();

        // test the case when SBP should not track the task
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        task = new SearchTask(1, "", "test", () -> "", null, null);
        mockThreadPool = new TestThreadPool("queryGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testId");
        task.setWorkloadGroupId(mockThreadPool.getThreadContext());
        assertFalse(queryGroupService.shouldSBPHandle(task));
    }

    private static Set<WorkloadGroup> getActiveWorkloadGroups(
        String name,
        String id,
        MutableWorkloadGroupFragment.ResiliencyMode mode,
        Map<ResourceType, Double> resourceLimits
    ) {
        WorkloadGroup testWorkloadGroup = getWorkloadGroup(name, id, mode, resourceLimits);
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(testWorkloadGroup);
            }
        };
        return activeWorkloadGroups;
    }

    private static WorkloadGroup getWorkloadGroup(
        String name,
        String id,
        MutableWorkloadGroupFragment.ResiliencyMode mode,
        Map<ResourceType, Double> resourceLimits
    ) {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(name, id, new MutableWorkloadGroupFragment(mode, resourceLimits), 1L);
        return testWorkloadGroup;
    }

    // This is needed to test the behavior of WorkloadGroupService#doRun method
    static class TestWorkloadGroupCancellationService extends WorkloadGroupTaskCancellationService {
        public TestWorkloadGroupCancellationService(
            WorkloadManagementSettings workloadManagementSettings,
            TaskSelectionStrategy taskSelectionStrategy,
            WorkloadGroupResourceUsageTrackerService resourceUsageTrackerService,
            WorkloadGroupsStateAccessor queryGroupsStateAccessor,
            Collection<WorkloadGroup> activeWorkloadGroups,
            Collection<WorkloadGroup> deletedWorkloadGroups
        ) {
            super(workloadManagementSettings, taskSelectionStrategy, resourceUsageTrackerService, queryGroupsStateAccessor);
        }

        @Override
        public void cancelTasks(
            BooleanSupplier isNodeInDuress,
            Collection<WorkloadGroup> activeWorkloadGroups,
            Collection<WorkloadGroup> deletedWorkloadGroups
        ) {

        }
    }
}
