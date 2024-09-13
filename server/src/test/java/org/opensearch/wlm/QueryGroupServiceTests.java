/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.search.backpressure.trackers.NodeDuressTrackers;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.cancellation.QueryGroupTaskCancellationService;
import org.opensearch.wlm.cancellation.TaskSelectionStrategy;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueryGroupServiceTests extends OpenSearchTestCase {
    private QueryGroupService queryGroupService;
    private QueryGroupTaskCancellationService mockCancellationService;
    private ClusterService mockClusterService;
    private ThreadPool mockThreadPool;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    private Scheduler.Cancellable mockScheduledFuture;
    private Map<String, QueryGroupState> mockQueryGroupStateMap;
    NodeDuressTrackers mockNodeDuressTrackers;

    @Before
    public void setup() {
        mockClusterService = Mockito.mock(ClusterService.class);
        mockThreadPool = Mockito.mock(ThreadPool.class);
        mockScheduledFuture = Mockito.mock(Scheduler.Cancellable.class);
        mockWorkloadManagementSettings = Mockito.mock(WorkloadManagementSettings.class);
        mockQueryGroupStateMap = new HashMap<>();
        mockNodeDuressTrackers = Mockito.mock(NodeDuressTrackers.class);
        mockCancellationService = Mockito.mock(TestQueryGroupCancellationService.class);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupStateMap,
            new HashSet<>(),
            new HashSet<>()
        );
    }

    public void testApplyClusterState() {
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
        queryGroupService.applyClusterState(mockClusterChangedEvent);

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
        doNothing().when(mockCancellationService).refreshQueryGroups(any(), any());
        // Call the method
        queryGroupService.doRun();

        // Verify that refreshQueryGroups was called
        Mockito.verify(mockCancellationService).refreshQueryGroups(any(), any());

        // Verify that cancelTasks was called with a BooleanSupplier
        ArgumentCaptor<BooleanSupplier> booleanSupplierCaptor = ArgumentCaptor.forClass(BooleanSupplier.class);
        Mockito.verify(mockCancellationService).cancelTasks(booleanSupplierCaptor.capture());

        // Assert the behavior of the BooleanSupplier
        BooleanSupplier capturedSupplier = booleanSupplierCaptor.getValue();
        assertTrue(capturedSupplier.getAsBoolean());

    }

    public void testDoRun_WhenModeDisabled() {
        when(mockWorkloadManagementSettings.getWlmMode()).thenReturn(WlmMode.DISABLED);
        when(mockNodeDuressTrackers.isNodeInDuress()).thenReturn(false);
        queryGroupService.doRun();
        // Verify that refreshQueryGroups was called
        Mockito.verify(mockCancellationService, never()).refreshQueryGroups(any(), any());

        Mockito.verify(mockCancellationService, never()).cancelTasks(any());

    }

    public void testRejectIfNeeded_whenQueryGroupIdIsNull() {
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

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            spyMap,
            activeQueryGroups,
            new HashSet<>()
        );
        queryGroupService.rejectIfNeeded(null);

        verify(spyMap, never()).get(any());
    }

    public void testRejectIfNeeded_whenQueryGroupIsSoftMode() {
        QueryGroup testQueryGroup = new QueryGroup(
            "testQueryGroup",
            "queryGroupId1",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.SOFT, Map.of(ResourceType.CPU, 0.10)),
            1L
        );
        Set<QueryGroup> activeQueryGroups = new HashSet<>() {
            {
                add(testQueryGroup);
            }
        };
        mockQueryGroupStateMap = new HashMap<>();
        QueryGroupState spyState = spy(new QueryGroupState());
        mockQueryGroupStateMap.put("queryGroupId1", spyState);

        Map<String, QueryGroupState> spyMap = spy(mockQueryGroupStateMap);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            spyMap,
            activeQueryGroups,
            new HashSet<>()
        );
        queryGroupService.rejectIfNeeded("queryGroupId1");

        verify(spyState, never()).getResourceState();
    }

    public void testRejectIfNeeded_whenQueryGroupIsEnforcedMode_andNotBreaching() {
        QueryGroup testQueryGroup = new QueryGroup(
            "testQueryGroup",
            "queryGroupId1",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.10)),
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
        queryGroupState.getResourceState().get(ResourceType.CPU).setLastRecordedUsage(0.08);

        mockQueryGroupStateMap.put("queryGroupId1", queryGroupState);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupStateMap,
            activeQueryGroups,
            new HashSet<>()
        );
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

        mockQueryGroupStateMap.put("queryGroupId1", spyState);

        queryGroupService = new QueryGroupService(
            mockCancellationService,
            mockClusterService,
            mockThreadPool,
            mockWorkloadManagementSettings,
            mockNodeDuressTrackers,
            mockQueryGroupStateMap,
            activeQueryGroups,
            new HashSet<>()
        );
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

    // This is needed to test the behavior of QueryGroupService#doRun method
    static class TestQueryGroupCancellationService extends QueryGroupTaskCancellationService {
        public TestQueryGroupCancellationService(
            WorkloadManagementSettings workloadManagementSettings,
            TaskSelectionStrategy taskSelectionStrategy,
            QueryGroupResourceUsageTrackerService resourceUsageTrackerService,
            Collection<QueryGroup> activeQueryGroups,
            Collection<QueryGroup> deletedQueryGroups
        ) {
            super(workloadManagementSettings, taskSelectionStrategy, resourceUsageTrackerService, activeQueryGroups, deletedQueryGroups);
        }

        @Override
        public void cancelTasks(BooleanSupplier isNodeInDuress) {

        }
    }
}
