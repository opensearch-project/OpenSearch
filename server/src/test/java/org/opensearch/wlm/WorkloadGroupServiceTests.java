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
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.TaskSelectionStrategy;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
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
    public static final String WORKLOAD_GROUP_ID = "workloadGroupId1";
    private WorkloadGroupService workloadGroupService;
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

        workloadGroupService = new WorkloadGroupService(
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
        when(mockPreviousMetadata.workloadGroups()).thenReturn(previousWorkloadGroups);
        when(mockMetadata.workloadGroups()).thenReturn(currentWorkloadGroups);
        workloadGroupService.clusterChanged(mockClusterChangedEvent);

        Set<WorkloadGroup> currentWorkloadGroupsExpected = Set.of(currentWorkloadGroups.get("4241"));
        Set<WorkloadGroup> previousWorkloadGroupsExpected = Set.of(previousWorkloadGroups.get("4242"));

        assertEquals(currentWorkloadGroupsExpected, workloadGroupService.getActiveWorkloadGroups());
        assertEquals(previousWorkloadGroupsExpected, workloadGroupService.getDeletedWorkloadGroups());
    }

    public void testDoStart_SchedulesTask() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockWorkloadManagementSettings.getWorkloadGroupServiceRunInterval()).thenReturn(TimeValue.timeValueSeconds(1));
        workloadGroupService.doStart();
        Mockito.verify(mockThreadPool).scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class), eq(ThreadPool.Names.GENERIC));
    }

    public void testDoStop_CancelsScheduledTask() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockThreadPool.scheduleWithFixedDelay(any(), any(), any())).thenReturn(mockScheduledFuture);
        workloadGroupService.doStart();
        workloadGroupService.doStop();
        Mockito.verify(mockScheduledFuture).cancel();
    }

    public void testDoRun_WhenModeEnabled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(true);
        // Call the method
        workloadGroupService.doRun();

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
        workloadGroupService.doRun();
        // Verify that refreshWorkloadGroups was called

        Mockito.verify(mockCancellationService, never()).cancelTasks(any(), any(), any());

    }

    public void testRejectIfNeeded_whenWorkloadGroupIdIsNullOrDefaultOne() {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            "workloadGroupId1",
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
        mockWorkloadGroupStateMap.put("workloadGroupId1", new WorkloadGroupState());

        Map<String, WorkloadGroupState> spyMap = spy(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        workloadGroupService.rejectIfNeeded(null);

        verify(spyMap, never()).get(any());

        workloadGroupService.rejectIfNeeded(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());
        verify(spyMap, never()).get(any());
    }

    public void testRejectIfNeeded_whenSoftModeWorkloadGroupIsContendedAndNodeInDuress() {
        Set<WorkloadGroup> activeWorkloadGroups = getActiveWorkloadGroups(
            "testWorkloadGroup",
            WORKLOAD_GROUP_ID,
            MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockWorkloadGroupStateMap = new HashMap<>();
        mockWorkloadGroupStateMap.put("workloadGroupId1", new WorkloadGroupState());
        WorkloadGroupState state = new WorkloadGroupState();
        WorkloadGroupState.ResourceTypeState cpuResourceState = new WorkloadGroupState.ResourceTypeState(ResourceType.CPU);
        cpuResourceState.setLastRecordedUsage(0.10);
        state.getResourceState().put(ResourceType.CPU, cpuResourceState);
        WorkloadGroupState spyState = spy(state);
        mockWorkloadGroupStateMap.put(WORKLOAD_GROUP_ID, spyState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
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
        assertThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.rejectIfNeeded("workloadGroupId1"));
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsSoftMode() {
        Set<WorkloadGroup> activeWorkloadGroups = getActiveWorkloadGroups(
            "testWorkloadGroup",
            WORKLOAD_GROUP_ID,
            MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockWorkloadGroupStateMap = new HashMap<>();
        WorkloadGroupState spyState = spy(new WorkloadGroupState());
        mockWorkloadGroupStateMap.put("workloadGroupId1", spyState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockWorkloadGroupsStateAccessor,
            activeWorkloadGroups,
            new HashSet<>()
        );
        workloadGroupService.rejectIfNeeded("workloadGroupId1");

        verify(spyState, never()).getResourceState();
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsEnforcedMode_andNotBreaching() {
        WorkloadGroup testWorkloadGroup = getWorkloadGroup(
            "testWorkloadGroup",
            "workloadGroupId1",
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
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        workloadGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.05);

        mockWorkloadGroupStateMap.put("workloadGroupId1", workloadGroupState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
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
        workloadGroupService.rejectIfNeeded("workloadGroupId1");

        // verify the check to compare the current usage and limit
        // this should happen 3 times => 2 to check whether the resource limit has the TRACKED resource type and 1 to get the value
        verify(spuWorkloadGroup, times(3)).getResourceLimits();
        assertEquals(0, workloadGroupState.getResourceState().get(ResourceType.CPU).rejections.count());
        assertEquals(0, workloadGroupState.totalRejections.count());
    }

    public void testRejectIfNeeded_whenWorkloadGroupIsEnforcedMode_andBreaching() {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            "workloadGroupId1",
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
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        workloadGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.18);
        workloadGroupState.getResourceState().get(ResourceType.MEMORY).setLastRecordedUsage(0.18);
        WorkloadGroupState spyState = spy(workloadGroupState);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        mockWorkloadGroupStateMap.put("workloadGroupId1", spyState);

        workloadGroupService = new WorkloadGroupService(
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
        assertThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.rejectIfNeeded("workloadGroupId1"));

        // verify the check to compare the current usage and limit
        // this should happen 3 times => 1 to check whether the resource limit has the TRACKED resource type and 1 to get the value
        // because it will break out of the loop since the limits are breached
        verify(spuWorkloadGroup, times(2)).getResourceLimits();
        assertEquals(
            1,
            workloadGroupState.getResourceState().get(ResourceType.CPU).rejections.count() + workloadGroupState.getResourceState()
                .get(ResourceType.MEMORY).rejections.count()
        );
        assertEquals(1, workloadGroupState.totalRejections.count());
    }

    public void testRejectIfNeeded_whenFeatureIsNotEnabled() {
        WorkloadGroup testWorkloadGroup = new WorkloadGroup(
            "testWorkloadGroup",
            "workloadGroupId1",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.10)),
            1L
        );
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>() {
            {
                add(testWorkloadGroup);
            }
        };
        mockWorkloadGroupStateMap = new HashMap<>();
        mockWorkloadGroupStateMap.put("workloadGroupId1", new WorkloadGroupState());

        Map<String, WorkloadGroupState> spyMap = spy(mockWorkloadGroupStateMap);

        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);

        workloadGroupService = new WorkloadGroupService(
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

        workloadGroupService.rejectIfNeeded(testWorkloadGroup.get_id());
        verify(spyMap, never()).get(any());
    }

    public void testOnTaskCompleted() {
        Task task = new SearchTask(12, "", "", () -> "", null, null);
        mockThreadPool = new TestThreadPool("workloadGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testId");
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        mockWorkloadGroupStateMap.put("testId", workloadGroupState);
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);
        workloadGroupService = new WorkloadGroupService(
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
        workloadGroupService.onTaskCompleted(task);

        assertEquals(1, workloadGroupState.totalCompletions.count());

        // test non WorkloadGroupTask
        task = new Task(1, "simple", "test", "mock task", null, null);
        workloadGroupService.onTaskCompleted(task);

        // It should still be 1
        assertEquals(1, workloadGroupState.totalCompletions.count());

        mockThreadPool.shutdown();
    }

    public void testGetCurrentWorkloadGroupReturnsNullWhenHeaderMissing() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        assertNull(workloadGroupService.getCurrentWorkloadGroup());
    }

    public void testGetCurrentWorkloadGroupReturnsGroupWhenPresent() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "wg-1");
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        WorkloadGroup wg = new WorkloadGroup(
            "wg-1-name",
            "wg-1",
            new MutableWorkloadGroupFragment(MutableWorkloadGroupFragment.ResiliencyMode.SOFT, Map.of(ResourceType.MEMORY, 0.5)),
            1L
        );
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Map.of("wg-1", wg));
        assertSame(wg, workloadGroupService.getCurrentWorkloadGroup());
    }

    public void testGetCurrentWorkloadGroupReturnsNullWhenGroupMissing() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "missing-id");
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Collections.emptyMap());
        assertNull(workloadGroupService.getCurrentWorkloadGroup());
    }

    private void stubClusterStateWithGroup(WorkloadGroup wg) {
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Map.of(wg.get_id(), wg));
    }

    private WorkloadGroup throttledGroup(String id, Settings throttling) {
        return throttledGroup(id, throttling, MutableWorkloadGroupFragment.ResiliencyMode.ENFORCED);
    }

    private WorkloadGroup throttledGroup(String id, Settings throttling, MutableWorkloadGroupFragment.ResiliencyMode mode) {
        return new WorkloadGroup(
            id + "-name",
            id,
            new MutableWorkloadGroupFragment(mode, Map.of(ResourceType.MEMORY, 0.5), Settings.EMPTY, throttling),
            1L
        );
    }

    public void testAcquireThrottleAdmitsNestedRequestWithoutASecondPermit() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // The outer request takes the group's only permit and is marked as counted.
        WorkloadGroupTask outer = throttleTask("wg-1");
        Releasable outerPermit = workloadGroupService.acquireThrottleOrReject(outer, false);
        assertNotNull(outerPermit);
        assertTrue("a successful acquire must mark the task as counted", outer.isThrottleCounted());

        // A nested coordinator search (a terms lookup with a subquery runs one during the rewrite phase) has a counted
        // parent. At node_limit=1 charging it again would 429 the request that spawned it, so it is admitted with no
        // permit of its own -- null means "nothing to release", not "throttled".
        WorkloadGroupTask nested = throttleTask("wg-1");
        assertNull(workloadGroupService.acquireThrottleOrReject(nested, true));
        // The exempt request must still be marked as counted, even though it holds no permit. The accounting has to be
        // transitive: this task may itself issue a nested search (a terms lookup whose subquery is another terms lookup),
        // and that grandchild reads only its own parent. If this task recorded nothing, the grandchild would be charged a
        // fresh permit for a family that already paid -- a 429 at node_limit=1 for a single legitimate request.
        assertTrue("an exempt request must be marked as counted, so the accounting propagates transitively", nested.isThrottleCounted());
        assertEquals(
            "an exemption is not a throttle",
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled()
        );

        // The exemption applies only to a request whose parent was counted: an independent request still hits the limit,
        // so this cannot silently disable throttling for the group.
        WorkloadGroupTask independent = throttleTask("wg-1");
        expectThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.acquireThrottleOrReject(independent, false));

        outerPermit.close();
    }

    public void testAcquireThrottleChargesASearchWhoseParentWasNotCounted() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // An _msearch sub-search has a parent task, but that parent is the multi-search task -- a plain CancellableTask
        // that never went through admission and so is never counted. Sibling sub-searches are independent units of client
        // work and must each be charged, so the first takes the group's only permit and the second is rejected.
        WorkloadGroupTask firstSubSearch = throttleTask("wg-1");
        Releasable firstPermit = workloadGroupService.acquireThrottleOrReject(firstSubSearch, false);
        assertNotNull("the first sub-search of an _msearch must take its own permit", firstPermit);
        assertTrue(firstSubSearch.isThrottleCounted());

        WorkloadGroupTask secondSubSearch = throttleTask("wg-1");
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> workloadGroupService.acquireThrottleOrReject(secondSubSearch, false)
        );
        assertFalse("a rejected request must not be marked as counted", secondSubSearch.isThrottleCounted());
        assertEquals(
            "an _msearch sub-search rejected by the limit is a real throttle",
            1,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled()
        );

        firstPermit.close();
    }

    public void testAcquireThrottleExemptionIsTransitiveAcrossTwoLevelsOfNesting() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // A terms lookup whose subquery is itself a terms lookup issues two levels of nested coordinator search. Each level
        // reads only its own parent, so the exemption is only correct if it survives a hop through a task that holds no
        // permit of its own. Root A pays; B and C must both ride on that one permit.
        WorkloadGroupTask rootA = throttleTask("wg-1");
        Releasable rootPermit = workloadGroupService.acquireThrottleOrReject(rootA, false);
        assertNotNull(rootPermit);
        assertTrue(rootA.isThrottleCounted());

        WorkloadGroupTask nestedB = throttleTask("wg-1");
        assertNull(workloadGroupService.acquireThrottleOrReject(nestedB, rootA.isThrottleCounted()));

        // C sees only what B advertises. If the exempt middle task recorded nothing, C would be charged a fresh permit and
        // self-reject at node_limit=1 -- one legitimate request 429ing itself.
        WorkloadGroupTask grandchildC = throttleTask("wg-1");
        assertNull(
            "a second level of nesting must inherit the accounting through the exempt middle task",
            workloadGroupService.acquireThrottleOrReject(grandchildC, nestedB.isThrottleCounted())
        );
        assertEquals(
            "no level of a single request's own nesting may be counted as a throttle",
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled()
        );

        // The accounting must not leak into unrelated requests: the group is still at its limit for anyone else.
        WorkloadGroupTask independent = throttleTask("wg-1");
        expectThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.acquireThrottleOrReject(independent, false));

        rootPermit.close();
    }

    private WorkloadGroupTask throttleTask(String workloadGroupId) {
        WorkloadGroupTask task = new WorkloadGroupTask(1L, "transport", "Search", "test task", TaskId.EMPTY_TASK_ID, Map.of());
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, workloadGroupId);
        task.setWorkloadGroupId(threadContext);
        return task;
    }

    public void testAcquireThrottleReturnsNullWhenNodeLimitUnset() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        stubClusterStateWithGroup(throttledGroup("wg-1", Settings.EMPTY)); // throttling not configured
        assertNull(workloadGroupService.acquireThrottleOrReject("wg-1", null));
    }

    public void testAcquireThrottleReturnsNullWhenWlmDisabled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        assertNull(workloadGroupService.acquireThrottleOrReject("wg-1", null));
    }

    public void testAcquireThrottleRejectsAtLimitAndIncrementsStat() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        Releasable permit = workloadGroupService.acquireThrottleOrReject("wg-1", null); // first admit succeeds
        assertNotNull(permit);
        // second admit hits node_limit of 1 -> 429 + total_throttled incremented
        expectThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.acquireThrottleOrReject("wg-1", null));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        // releasing the first permit frees the slot so a subsequent acquire succeeds
        permit.close();
        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", null));
    }

    public void testAcquireThrottleMonitorModeObservesWithoutRejecting() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.MONITOR));

        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", null)); // first admit takes the only slot
        // A MONITOR group observes only: an over-limit request is admitted (null permit, nothing to release) rather
        // than rejected, and the would-be rejection is not counted.
        assertNull(workloadGroupService.acquireThrottleOrReject("wg-1", null));
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleSoftModeStillRejects() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling, MutableWorkloadGroupFragment.ResiliencyMode.SOFT));

        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", null));
        // Only MONITOR is observe-only; SOFT enforces the throttle like ENFORCED does.
        expectThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.acquireThrottleOrReject("wg-1", null));
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleUsernameKeepsPerUserBuckets() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // alice takes her single slot; a second alice request is rejected.
        Releasable alice = workloadGroupService.acquireThrottleOrReject("wg-1", "username|alice");
        assertNotNull(alice);
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> workloadGroupService.acquireThrottleOrReject("wg-1", "username|alice")
        );
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());

        // bob is a different bucket, so he is admitted even while alice is at her limit.
        Releasable bob = workloadGroupService.acquireThrottleOrReject("wg-1", "username|bob");
        assertNotNull(bob);

        // releasing alice frees her bucket
        alice.close();
        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", "username|alice"));
    }

    public void testAcquireThrottleUsernameWithCommaDoesNotCollide() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        // principal for user "a,b" with a role token appended
        String userAB = "username|a,b" + delim + "role|admin";
        // user "a" is a genuinely different principal
        String userA = "username|a";

        Releasable ab = workloadGroupService.acquireThrottleOrReject("wg-1", userAB); // fills "a,b" bucket
        assertNotNull(ab);
        // user "a" must NOT be treated as the same bucket as "a,b" -> still admitted
        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", userA));
        // a second "a,b" request hits the "a,b" bucket limit -> rejected
        expectThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.acquireThrottleOrReject("wg-1", userAB));
    }

    public void testAcquireThrottleRolePicksMatchingSubfieldFromMultiTokenPrincipal() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "role").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // A principal header may carry both subfields; the role bucket must key off the role token only.
        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", "username|alice" + delim + "role|admin"));
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> workloadGroupService.acquireThrottleOrReject("wg-1", "username|bob" + delim + "role|admin")
        );
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testAcquireThrottleRoleBucketIsStableAcrossTokenOrder() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "role").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // A user in several roles must land in one deterministic bucket. If the resolver took whichever role token came
        // first, the same user would draw a fresh allowance whenever the extractor changed its ordering.
        String delim = WorkloadGroupTask.WORKLOAD_GROUP_PRINCIPAL_VALUE_DELIMITER;
        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", "role|admin" + delim + "role|analyst"));
        expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> workloadGroupService.acquireThrottleOrReject("wg-1", "role|analyst" + delim + "role|admin")
        );
        assertEquals(1, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    public void testThrottleRejectionNamesGroupAndAttribute() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", "username|alice"));
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> workloadGroupService.acquireThrottleOrReject("wg-1", "username|alice")
        );
        // The operator (and the caller) must be able to tell which group and which principal was throttled.
        assertTrue(e.getMessage(), e.getMessage().contains("workload group [wg-1-name]"));
        assertTrue(e.getMessage(), e.getMessage().contains("username [alice]"));
        assertTrue(e.getMessage(), e.getMessage().contains("per-node limit of 1"));
    }

    public void testThrottleRejectionForWholeGroupOmitsAttributeClause() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", null));
        OpenSearchRejectedExecutionException e = expectThrows(
            OpenSearchRejectedExecutionException.class,
            () -> workloadGroupService.acquireThrottleOrReject("wg-1", null)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("workload group [wg-1-name]"));
        assertFalse(e.getMessage(), e.getMessage().contains(" for group "));
    }

    public void testIncrementFailuresForUntaggedRequestDoesNotThrow() {
        // The search failure listener reads the workload group header unconditionally, so it legitimately passes null
        // for an untagged request. The state map is a ConcurrentHashMap, which rejects a null key.
        workloadGroupService.incrementFailuresFor(null);
        assertEquals(
            1,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get()).getFailures()
        );
    }

    public void testAcquireThrottleFailsOpenWhenPrincipalMissingForUsername() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup("wg-1");
        Settings throttling = Settings.builder().put("attribute", "username").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // No principal (e.g. security plugin not installed) or no matching subfield -> not throttled (fail open).
        assertNull(workloadGroupService.acquireThrottleOrReject("wg-1", null));
        assertNull(workloadGroupService.acquireThrottleOrReject("wg-1", ""));
        assertNull(workloadGroupService.acquireThrottleOrReject("wg-1", "role|admin")); // no username token
        assertEquals(0, mockWorkloadGroupsStateAccessor.getWorkloadGroupState("wg-1").getTotalThrottled());
    }

    /**
     * A failure while recording the total_throttled stat must NOT swallow the rejection and admit the over-limit
     * request. Whether the state map lookup returns null (group not yet registered / just deleted) or throws, the
     * 429 must still propagate.
     */
    public void testAcquireThrottleStillRejectsWhenStatUpdateFails() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // state map with no entry for wg-1 (as during the state-registration lag) -> raw get(id) returns null
        WorkloadGroupsStateAccessor emptyMapAccessor = Mockito.mock(WorkloadGroupsStateAccessor.class);
        when(emptyMapAccessor.getWorkloadGroupStateMap()).thenReturn(new HashMap<>());
        WorkloadGroupService serviceWithNullState = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            emptyMapAccessor,
            new HashSet<>(),
            new HashSet<>()
        );

        assertNotNull(serviceWithNullState.acquireThrottleOrReject("wg-1", null)); // first admit fills the single slot
        // second acquire is over the limit; a null state must not let the stat update swallow the 429
        expectThrows(OpenSearchRejectedExecutionException.class, () -> serviceWithNullState.acquireThrottleOrReject("wg-1", null));

        // accessor whose state-map lookup throws must also still propagate the 429
        WorkloadGroupsStateAccessor throwingStateAccessor = Mockito.mock(WorkloadGroupsStateAccessor.class);
        when(throwingStateAccessor.getWorkloadGroupStateMap()).thenThrow(new RuntimeException("state map race"));
        WorkloadGroupService serviceWithThrowingState = new WorkloadGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            throwingStateAccessor,
            new HashSet<>(),
            new HashSet<>()
        );

        assertNotNull(serviceWithThrowingState.acquireThrottleOrReject("wg-1", null)); // fills the single slot
        expectThrows(OpenSearchRejectedExecutionException.class, () -> serviceWithThrowingState.acquireThrottleOrReject("wg-1", null));
    }

    /**
     * During the state-registration lag a node can enforce a new group's limit before its clusterChanged() registers
     * the state. The rejection stat must not be misattributed to the DEFAULT group in that window.
     */
    public void testAcquireThrottleDoesNotMisattributeToDefaultDuringRegistrationLag() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        Settings throttling = Settings.builder().put("attribute", "group").put("node_limit", 1).build();
        stubClusterStateWithGroup(throttledGroup("wg-1", throttling));

        // DEFAULT group state exists, but wg-1 is NOT yet registered (registration lag).
        mockWorkloadGroupsStateAccessor.addNewWorkloadGroup(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());

        assertNotNull(workloadGroupService.acquireThrottleOrReject("wg-1", null)); // fills the single slot
        expectThrows(OpenSearchRejectedExecutionException.class, () -> workloadGroupService.acquireThrottleOrReject("wg-1", null));

        // the rejection must NOT have landed on the DEFAULT group
        assertEquals(
            0,
            mockWorkloadGroupsStateAccessor.getWorkloadGroupState(WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get())
                .getTotalThrottled()
        );
    }

    public void testShouldSBPHandle() {
        SearchTask task = createMockTaskWithResourceStats(SearchTask.class, 100, 200, 0, 12);
        WorkloadGroupState workloadGroupState = new WorkloadGroupState();
        Set<WorkloadGroup> activeWorkloadGroups = new HashSet<>();
        mockWorkloadGroupStateMap.put("testId", workloadGroupState);
        mockWorkloadGroupsStateAccessor = new WorkloadGroupsStateAccessor(mockWorkloadGroupStateMap);
        workloadGroupService = new WorkloadGroupService(
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

        // Default workloadGroupId
        mockThreadPool = new TestThreadPool("workloadGroupServiceTests");
        mockThreadPool.getThreadContext()
            .putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get());
        // we haven't set the workloadGroupId yet SBP should still track the task for cancellation
        assertTrue(workloadGroupService.shouldSBPHandle(task));
        task.setWorkloadGroupId(mockThreadPool.getThreadContext());
        assertTrue(workloadGroupService.shouldSBPHandle(task));

        mockThreadPool.shutdownNow();

        // invalid workloadGroup task
        mockThreadPool = new TestThreadPool("workloadGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testId");
        task.setWorkloadGroupId(mockThreadPool.getThreadContext());
        assertTrue(workloadGroupService.shouldSBPHandle(task));

        // Valid workload group task but wlm not enabled
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
        assertTrue(workloadGroupService.shouldSBPHandle(task));

        mockThreadPool.shutdownNow();

        // test the case when SBP should not track the task
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        task = new SearchTask(1, "", "test", () -> "", null, null);
        mockThreadPool = new TestThreadPool("workloadGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "testId");
        task.setWorkloadGroupId(mockThreadPool.getThreadContext());
        assertFalse(workloadGroupService.shouldSBPHandle(task));
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
            WorkloadGroupsStateAccessor workloadGroupsStateAccessor,
            Collection<WorkloadGroup> activeWorkloadGroups,
            Collection<WorkloadGroup> deletedWorkloadGroups
        ) {
            super(workloadManagementSettings, taskSelectionStrategy, resourceUsageTrackerService, workloadGroupsStateAccessor);
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
