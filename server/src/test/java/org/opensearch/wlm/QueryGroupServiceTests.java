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
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.QueryGroupTaskCancellationService;
import org.opensearch.wlm.cancellation.TaskSelectionStrategy;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService;

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

public class QueryGroupServiceTests extends OpenSearchTestCase {
    public static final String QUERY_GROUP_ID = "queryGroupId1";
    private QueryGroupService queryGroupService;
    private QueryGroupTaskCancellationService mockCancellationService;
    private ClusterService mockClusterService;
    private ThreadPool mockThreadPool;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    private Scheduler.Cancellable mockScheduledFuture;
    private Map<String, QueryGroupState> mockQueryGroupStateMap;
    NodeDuressTrackers mockNodeDuressTrackers;
    QueryGroupsStateAccessor mockQueryGroupsStateAccessor;

    public void setUp() throws Exception {
        super.setUp();
        mockClusterService = Mockito.mock(ClusterService.class);
        mockThreadPool = Mockito.mock(ThreadPool.class);
        mockScheduledFuture = Mockito.mock(Scheduler.Cancellable.class);
        mockWorkloadManagementSettings = Mockito.mock(WorkloadManagementSettings.class);
        mockQueryGroupStateMap = new HashMap<>();
        mockNodeDuressTrackers = Mockito.mock(NodeDuressTrackers.class);
        mockCancellationService = Mockito.mock(TestQueryGroupCancellationService.class);
        mockQueryGroupsStateAccessor = new QueryGroupsStateAccessor();
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(false);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupsStateAccessor,
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
        QueryGroup addedQueryGroup = new QueryGroup(
            "addedQueryGroup",
            "4242",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5)),
            1L
        );
        QueryGroup deletedQueryGroup = new QueryGroup(
            "deletedQueryGroup",
            "4241",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5)),
            1L
        );
        Map<String, QueryGroup> previousQueryGroups = new HashMap<>();
        previousQueryGroups.put("4242", addedQueryGroup);
        Map<String, QueryGroup> currentQueryGroups = new HashMap<>();
        currentQueryGroups.put("4241", deletedQueryGroup);

        when(mockClusterChangedEvent.previousState()).thenReturn(mockPreviousClusterState);
        when(mockClusterChangedEvent.state()).thenReturn(mockClusterState);
        when(mockPreviousClusterState.metadata()).thenReturn(mockPreviousMetadata);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockPreviousMetadata.queryGroups()).thenReturn(previousQueryGroups);
        when(mockMetadata.queryGroups()).thenReturn(currentQueryGroups);
        queryGroupService.clusterChanged(mockClusterChangedEvent);

        Set<QueryGroup> currentQueryGroupsExpected = Set.of(currentQueryGroups.get("4241"));
        Set<QueryGroup> previousQueryGroupsExpected = Set.of(previousQueryGroups.get("4242"));

        assertEquals(currentQueryGroupsExpected, queryGroupService.getActiveQueryGroups());
        assertEquals(previousQueryGroupsExpected, queryGroupService.getDeletedQueryGroups());
    }

    public void testDoStart_SchedulesTask() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockWorkloadManagementSettings.getQueryGroupServiceRunInterval()).thenReturn(TimeValue.timeValueSeconds(1));
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

        // Verify that refreshQueryGroups was called

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
        // Verify that refreshQueryGroups was called

        Mockito.verify(mockCancellationService, never()).cancelTasks(any(), any(), any());

    }

    public void testRejectIfNeeded_whenQueryGroupIdIsNullOrDefaultOne() {
        QueryGroup testQueryGroup = new QueryGroup(
            "testQueryGroup",
            "queryGroupId1",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.10)),
            1L
        );
        Set<QueryGroup> activeQueryGroups = new HashSet<>() {
            {
                add(testQueryGroup);
            }
        };
        mockQueryGroupStateMap = new HashMap<>();
        mockQueryGroupsStateAccessor = new QueryGroupsStateAccessor(mockQueryGroupStateMap);
        mockQueryGroupStateMap.put("queryGroupId1", new QueryGroupState());

        Map<String, QueryGroupState> spyMap = spy(mockQueryGroupStateMap);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupsStateAccessor,
            activeQueryGroups,
            new HashSet<>()
        );
        queryGroupService.rejectIfNeeded(null);

        verify(spyMap, never()).get(any());

        queryGroupService.rejectIfNeeded(QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get());
        verify(spyMap, never()).get(any());
    }

    public void testRejectIfNeeded_whenSoftModeQueryGroupIsContendedAndNodeInDuress() {
        Set<QueryGroup> activeQueryGroups = getActiveQueryGroups(
            "testQueryGroup",
            QUERY_GROUP_ID,
            MutableQueryGroupFragment.ResiliencyMode.SOFT,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockQueryGroupStateMap = new HashMap<>();
        mockQueryGroupStateMap.put("queryGroupId1", new QueryGroupState());
        QueryGroupState state = new QueryGroupState();
        QueryGroupState.ResourceTypeState cpuResourceState = new QueryGroupState.ResourceTypeState(ResourceType.CPU);
        cpuResourceState.setLastRecordedUsage(0.10);
        state.getResourceState().put(ResourceType.CPU, cpuResourceState);
        QueryGroupState spyState = spy(state);
        mockQueryGroupStateMap.put(QUERY_GROUP_ID, spyState);

        mockQueryGroupsStateAccessor = new QueryGroupsStateAccessor(mockQueryGroupStateMap);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupsStateAccessor,
            activeQueryGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(true);
        assertThrows(OpenSearchRejectedExecutionException.class, () -> queryGroupService.rejectIfNeeded("queryGroupId1"));
    }

    public void testRejectIfNeeded_whenQueryGroupIsSoftMode() {
        Set<QueryGroup> activeQueryGroups = getActiveQueryGroups(
            "testQueryGroup",
            QUERY_GROUP_ID,
            MutableQueryGroupFragment.ResiliencyMode.SOFT,
            Map.of(ResourceType.CPU, 0.10)
        );
        mockQueryGroupStateMap = new HashMap<>();
        QueryGroupState spyState = spy(new QueryGroupState());
        mockQueryGroupStateMap.put("queryGroupId1", spyState);

        mockQueryGroupsStateAccessor = new QueryGroupsStateAccessor(mockQueryGroupStateMap);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupsStateAccessor,
            activeQueryGroups,
            new HashSet<>()
        );
        queryGroupService.rejectIfNeeded("queryGroupId1");

        verify(spyState, never()).getResourceState();
    }

    public void testRejectIfNeeded_whenQueryGroupIsEnforcedMode_andNotBreaching() {
        QueryGroup testQueryGroup = getQueryGroup(
            "testQueryGroup",
            "queryGroupId1",
            MutableQueryGroupFragment.ResiliencyMode.ENFORCED,
            Map.of(ResourceType.CPU, 0.10)
        );
        QueryGroup spuQueryGroup = spy(testQueryGroup);
        Set<QueryGroup> activeQueryGroups = new HashSet<>() {
            {
                add(spuQueryGroup);
            }
        };
        mockQueryGroupStateMap = new HashMap<>();
        QueryGroupState queryGroupState = new QueryGroupState();
        queryGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.05);

        mockQueryGroupStateMap.put("queryGroupId1", queryGroupState);

        mockQueryGroupsStateAccessor = new QueryGroupsStateAccessor(mockQueryGroupStateMap);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupsStateAccessor,
            activeQueryGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        when(mockWorkloadManagementSettings.getNodeLevelCpuRejectionThreshold()).thenReturn(0.8);
        queryGroupService.rejectIfNeeded("queryGroupId1");

        // verify the check to compare the current usage and limit
        // this should happen 3 times => 2 to check whether the resource limit has the TRACKED resource type and 1 to get the value
        verify(spuQueryGroup, times(3)).getResourceLimits();
        assertEquals(0, queryGroupState.getResourceState().get(ResourceType.CPU).rejections.count());
        assertEquals(0, queryGroupState.totalRejections.count());
    }

    public void testRejectIfNeeded_whenQueryGroupIsEnforcedMode_andBreaching() {
        QueryGroup testQueryGroup = new QueryGroup(
            "testQueryGroup",
            "queryGroupId1",
            new MutableQueryGroupFragment(
                MutableQueryGroupFragment.ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.10, ResourceType.MEMORY, 0.10)
            ),
            1L
        );
        QueryGroup spuQueryGroup = spy(testQueryGroup);
        Set<QueryGroup> activeQueryGroups = new HashSet<>() {
            {
                add(spuQueryGroup);
            }
        };
        mockQueryGroupStateMap = new HashMap<>();
        QueryGroupState queryGroupState = new QueryGroupState();
        queryGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.18);
        queryGroupState.getResourceState().get(ResourceType.MEMORY).setLastRecordedUsage(0.18);
        QueryGroupState spyState = spy(queryGroupState);

        mockQueryGroupsStateAccessor = new QueryGroupsStateAccessor(mockQueryGroupStateMap);

        mockQueryGroupStateMap.put("queryGroupId1", spyState);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupsStateAccessor,
            activeQueryGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);
        assertThrows(OpenSearchRejectedExecutionException.class, () -> queryGroupService.rejectIfNeeded("queryGroupId1"));

        // verify the check to compare the current usage and limit
        // this should happen 3 times => 1 to check whether the resource limit has the TRACKED resource type and 1 to get the value
        // because it will break out of the loop since the limits are breached
        verify(spuQueryGroup, times(2)).getResourceLimits();
        assertEquals(
            1,
            queryGroupState.getResourceState().get(ResourceType.CPU).rejections.count() + queryGroupState.getResourceState()
                .get(ResourceType.MEMORY).rejections.count()
        );
        assertEquals(1, queryGroupState.totalRejections.count());
    }

    public void testRejectIfNeeded_whenFeatureIsNotEnabled() {
        QueryGroup testQueryGroup = new QueryGroup(
            "testQueryGroup",
            "queryGroupId1",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.10)),
            1L
        );
        Set<QueryGroup> activeQueryGroups = new HashSet<>() {
            {
                add(testQueryGroup);
            }
        };
        mockQueryGroupStateMap = new HashMap<>();
        mockQueryGroupStateMap.put("queryGroupId1", new QueryGroupState());

        Map<String, QueryGroupState> spyMap = spy(mockQueryGroupStateMap);

        mockQueryGroupsStateAccessor = new QueryGroupsStateAccessor(mockQueryGroupStateMap);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupsStateAccessor,
            activeQueryGroups,
            new HashSet<>()
        );
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);

        queryGroupService.rejectIfNeeded(testQueryGroup.get_id());
        verify(spyMap, never()).get(any());
    }

    public void testOnTaskCompleted() {
        Task task = createMockTaskWithResourceStats(SearchTask.class, 100, 200, 0, 12);
        mockThreadPool = new TestThreadPool("queryGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, "testId");
        QueryGroupState queryGroupState = new QueryGroupState();
        mockQueryGroupStateMap.put("testId", queryGroupState);
        mockQueryGroupsStateAccessor = new QueryGroupsStateAccessor(mockQueryGroupStateMap);
        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupsStateAccessor,
            new HashSet<>() {
                {
                    add(
                        new QueryGroup(
                            "testQueryGroup",
                            "testId",
                            new MutableQueryGroupFragment(
                                MutableQueryGroupFragment.ResiliencyMode.ENFORCED,
                                Map.of(ResourceType.CPU, 0.10, ResourceType.MEMORY, 0.10)
                            ),
                            1L
                        )
                    );
                }
            },
            new HashSet<>()
        );

        ((QueryGroupTask) task).setQueryGroupId(mockThreadPool.getThreadContext());
        queryGroupService.onTaskCompleted(task);

        assertEquals(1, queryGroupState.totalCompletions.count());

        // test non QueryGroupTask
        task = new Task(1, "simple", "test", "mock task", null, null);
        queryGroupService.onTaskCompleted(task);

        // It should still be 1
        assertEquals(1, queryGroupState.totalCompletions.count());

        mockThreadPool.shutdown();
    }

    public void testShouldSBPHandle() {
        QueryGroupTask task = createMockTaskWithResourceStats(SearchTask.class, 100, 200, 0, 12);
        QueryGroupState queryGroupState = new QueryGroupState();
        Set<QueryGroup> activeQueryGroups = new HashSet<>();
        mockQueryGroupStateMap.put("testId", queryGroupState);
        mockQueryGroupsStateAccessor = new QueryGroupsStateAccessor(mockQueryGroupStateMap);
        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupsStateAccessor,
            activeQueryGroups,
            Collections.emptySet()
        );

        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.ENABLED);

        // Default queryGroupId
        mockThreadPool = new TestThreadPool("queryGroupServiceTests");
        mockThreadPool.getThreadContext()
            .putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get());
        task.setQueryGroupId(mockThreadPool.getThreadContext());
        assertTrue(queryGroupService.shouldSBPHandle(task));

        mockThreadPool.shutdownNow();

        // invalid queryGroup task
        mockThreadPool = new TestThreadPool("queryGroupServiceTests");
        mockThreadPool.getThreadContext().putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, "testId");
        task.setQueryGroupId(mockThreadPool.getThreadContext());
        assertTrue(queryGroupService.shouldSBPHandle(task));

        // Valid query group task but wlm not enabled
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        activeQueryGroups.add(
            new QueryGroup(
                "testQueryGroup",
                "testId",
                new MutableQueryGroupFragment(
                    MutableQueryGroupFragment.ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.CPU, 0.10, ResourceType.MEMORY, 0.10)
                ),
                1L
            )
        );
        assertTrue(queryGroupService.shouldSBPHandle(task));

    }

    private static Set<QueryGroup> getActiveQueryGroups(
        String name,
        String id,
        MutableQueryGroupFragment.ResiliencyMode mode,
        Map<ResourceType, Double> resourceLimits
    ) {
        QueryGroup testQueryGroup = getQueryGroup(name, id, mode, resourceLimits);
        Set<QueryGroup> activeQueryGroups = new HashSet<>() {
            {
                add(testQueryGroup);
            }
        };
        return activeQueryGroups;
    }

    private static QueryGroup getQueryGroup(
        String name,
        String id,
        MutableQueryGroupFragment.ResiliencyMode mode,
        Map<ResourceType, Double> resourceLimits
    ) {
        QueryGroup testQueryGroup = new QueryGroup(name, id, new MutableQueryGroupFragment(mode, resourceLimits), 1L);
        return testQueryGroup;
    }

    // This is needed to test the behavior of QueryGroupService#doRun method
    static class TestQueryGroupCancellationService extends QueryGroupTaskCancellationService {
        public TestQueryGroupCancellationService(
            WorkloadManagementSettings workloadManagementSettings,
            TaskSelectionStrategy taskSelectionStrategy,
            QueryGroupResourceUsageTrackerService resourceUsageTrackerService,
            QueryGroupsStateAccessor queryGroupsStateAccessor,
            Collection<QueryGroup> activeQueryGroups,
            Collection<QueryGroup> deletedQueryGroups
        ) {
            super(workloadManagementSettings, taskSelectionStrategy, resourceUsageTrackerService, queryGroupsStateAccessor);
        }

        @Override
        public void cancelTasks(
            BooleanSupplier isNodeInDuress,
            Collection<QueryGroup> activeQueryGroups,
            Collection<QueryGroup> deletedQueryGroups
        ) {

        }
    }
}
