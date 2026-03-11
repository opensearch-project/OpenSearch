/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WorkloadGroup;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupService;
import org.opensearch.wlm.WorkloadGroupTask;
import org.opensearch.wlm.WorkloadGroupsStateAccessor;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.opensearch.wlm.stats.WorkloadGroupState;
import org.opensearch.wlm.stats.WorkloadGroupStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkloadGroupRequestOperationListenerTests extends OpenSearchTestCase {
    public static final int ITERATIONS = 20;
    ThreadPool testThreadPool;
    WorkloadGroupService workloadGroupService;
    private WorkloadGroupTaskCancellationService taskCancellationService;
    private ClusterService mockClusterService;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    Map<String, WorkloadGroupState> workloadGroupStateMap;
    String testWorkloadGroupId;
    WorkloadGroupRequestOperationListener sut;
    SearchRequestContext mockSearchRequestContext;
    SearchRequest mockSearchRequest;

    public void setUp() throws Exception {
        super.setUp();
        taskCancellationService = mock(WorkloadGroupTaskCancellationService.class);
        mockClusterService = mock(ClusterService.class);
        mockWorkloadManagementSettings = mock(WorkloadManagementSettings.class);
        workloadGroupStateMap = new HashMap<>();
        testWorkloadGroupId = "safjgagnakg-3r3fads";
        testThreadPool = new TestThreadPool("RejectionTestThreadPool");
        ClusterState mockClusterState = mock(ClusterState.class);
        when(mockClusterService.state()).thenReturn(mockClusterState);
        Metadata mockMetaData = mock(Metadata.class);
        when(mockClusterState.metadata()).thenReturn(mockMetaData);
        workloadGroupService = mock(WorkloadGroupService.class);
        sut = new WorkloadGroupRequestOperationListener(workloadGroupService, testThreadPool);
        mockSearchRequest = new SearchRequest();
        mockSearchRequestContext = mock(SearchRequestContext.class);
        when(mockSearchRequestContext.getRequest()).thenReturn(mockSearchRequest);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        testThreadPool.shutdown();
    }

    public void testRejectionCase() {
        final String testWorkloadGroupId = "asdgasgkajgkw3141_3rt4t";
        testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, testWorkloadGroupId);
        doThrow(OpenSearchRejectedExecutionException.class).when(workloadGroupService).rejectIfNeeded(testWorkloadGroupId);
        assertThrows(OpenSearchRejectedExecutionException.class, () -> sut.onRequestStart(mockSearchRequestContext));
    }

    public void testNonRejectionCase() {
        final String testWorkloadGroupId = "asdgasgkajgkw3141_3rt4t";
        testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, testWorkloadGroupId);
        doNothing().when(workloadGroupService).rejectIfNeeded(testWorkloadGroupId);

        sut.onRequestStart(mockSearchRequestContext);
    }

    public void testValidWorkloadGroupRequestFailure() throws IOException {

        WorkloadGroupStats expectedStats = new WorkloadGroupStats(
            Map.of(
                testWorkloadGroupId,
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    0,
                    0,
                    1,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0)
                    )
                ),
                WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get(),
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    0,
                    0,
                    0,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0)
                    )
                )
            )
        );

        assertSuccess(testWorkloadGroupId, workloadGroupStateMap, expectedStats, testWorkloadGroupId);
    }

    public void testMultiThreadedValidWorkloadGroupRequestFailures() {

        workloadGroupStateMap.put(testWorkloadGroupId, new WorkloadGroupState());
        WorkloadGroupsStateAccessor accessor = new WorkloadGroupsStateAccessor(workloadGroupStateMap);
        setupMockedWorkloadGroupsFromClusterState();
        workloadGroupService = new WorkloadGroupService(
            taskCancellationService,
            mockClusterService,
            testThreadPool,
            mockWorkloadManagementSettings,
            null,
            accessor,
            Collections.emptySet(),
            Collections.emptySet()
        );

        sut = new WorkloadGroupRequestOperationListener(workloadGroupService, testThreadPool);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            threads.add(new Thread(() -> {
                try (ThreadContext.StoredContext currentContext = testThreadPool.getThreadContext().stashContext()) {
                    testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, testWorkloadGroupId);
                    sut.onRequestFailure(null, null);
                }
            }));
        }

        threads.forEach(Thread::start);
        threads.forEach(th -> {
            try {
                th.join();
            } catch (InterruptedException ignored) {

            }
        });

        HashSet<String> set = new HashSet<>();
        set.add("_all");
        WorkloadGroupStats actualStats = workloadGroupService.nodeStats(set, null);

        WorkloadGroupStats expectedStats = new WorkloadGroupStats(
            Map.of(
                testWorkloadGroupId,
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    0,
                    0,
                    ITERATIONS,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0)
                    )
                ),
                WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get(),
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    0,
                    0,
                    0,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0)
                    )
                )
            )
        );

        assertEquals(expectedStats, actualStats);
    }

    public void testInvalidWorkloadGroupFailure() throws IOException {
        WorkloadGroupStats expectedStats = new WorkloadGroupStats(
            Map.of(
                testWorkloadGroupId,
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    0,
                    0,
                    0,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0)
                    )
                ),
                WorkloadGroupTask.DEFAULT_WORKLOAD_GROUP_ID_SUPPLIER.get(),
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    0,
                    0,
                    1,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new WorkloadGroupStats.ResourceStats(0, 0, 0)
                    )
                )
            )
        );

        assertSuccess(testWorkloadGroupId, workloadGroupStateMap, expectedStats, "dummy-invalid-qg-id");

    }

    private void assertSuccess(
        String testWorkloadGroupId,
        Map<String, WorkloadGroupState> workloadGroupStateMap,
        WorkloadGroupStats expectedStats,
        String threadContextQG_Id
    ) {
        WorkloadGroupsStateAccessor stateAccessor = new WorkloadGroupsStateAccessor(workloadGroupStateMap);
        try (ThreadContext.StoredContext currentContext = testThreadPool.getThreadContext().stashContext()) {
            testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, threadContextQG_Id);
            workloadGroupStateMap.put(testWorkloadGroupId, new WorkloadGroupState());

            setupMockedWorkloadGroupsFromClusterState();

            workloadGroupService = new WorkloadGroupService(
                taskCancellationService,
                mockClusterService,
                testThreadPool,
                mockWorkloadManagementSettings,
                null,
                stateAccessor,
                Collections.emptySet(),
                Collections.emptySet()
            );
            sut = new WorkloadGroupRequestOperationListener(workloadGroupService, testThreadPool);
            sut.onRequestFailure(null, null);

            HashSet<String> set = new HashSet<>();
            set.add("_all");
            WorkloadGroupStats actualStats = workloadGroupService.nodeStats(set, null);
            assertEquals(expectedStats, actualStats);
        }

    }

    private void setupMockedWorkloadGroupsFromClusterState() {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Collections.emptyMap());
    }

    // Tests for applyWorkloadGroupSearchSettings

    public void testApplySearchSettings_NullWorkloadGroupId() {
        // No workload group ID in thread context
        sut.onRequestStart(mockSearchRequestContext);

        // Request should remain unchanged - source is null, no timeout set
        assertNull(mockSearchRequest.source());
    }

    public void testApplySearchSettings_WorkloadGroupNotFound() {
        testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, "non-existent-id");
        when(workloadGroupService.getWorkloadGroupById("non-existent-id")).thenReturn(null);

        sut.onRequestStart(mockSearchRequestContext);

        assertNull(mockSearchRequest.source());
    }

    public void testApplySearchSettings_EmptySearchSettings() {
        mockSearchRequest.source(new SearchSourceBuilder());

        String wgId = "test-wg";
        WorkloadGroup wg = createWorkloadGroup(wgId, Map.of());
        when(workloadGroupService.getWorkloadGroupById(wgId)).thenReturn(wg);
        testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, wgId);

        sut.onRequestStart(mockSearchRequestContext);

        assertNull(mockSearchRequest.source().timeout()); // No settings applied
    }

    public void testApplySearchSettings_Timeout_WlmAppliedWhenNull() {
        mockSearchRequest.source(new SearchSourceBuilder());
        assertNull(mockSearchRequest.source().timeout());

        String wgId = "test-wg";
        WorkloadGroup wg = createWorkloadGroup(wgId, Map.of("timeout", "1m"));
        when(workloadGroupService.getWorkloadGroupById(wgId)).thenReturn(wg);
        testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, wgId);

        sut.onRequestStart(mockSearchRequestContext);

        assertEquals(TimeValue.timeValueMinutes(1), mockSearchRequest.source().timeout());
    }

    public void testApplySearchSettings_Timeout_RequestAlreadySet() {
        mockSearchRequest.source(new SearchSourceBuilder().timeout(TimeValue.timeValueSeconds(30)));

        String wgId = "test-wg";
        WorkloadGroup wg = createWorkloadGroup(wgId, Map.of("timeout", "10s"));
        when(workloadGroupService.getWorkloadGroupById(wgId)).thenReturn(wg);
        testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, wgId);

        sut.onRequestStart(mockSearchRequestContext);

        assertEquals(TimeValue.timeValueSeconds(30), mockSearchRequest.source().timeout()); // Request value preserved
    }

    public void testApplySearchSettings_Timeout_NullSource() {
        assertNull(mockSearchRequest.source());

        String wgId = "test-wg";
        WorkloadGroup wg = createWorkloadGroup(wgId, Map.of("timeout", "30s"));
        when(workloadGroupService.getWorkloadGroupById(wgId)).thenReturn(wg);
        testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, wgId);

        sut.onRequestStart(mockSearchRequestContext);

        assertNull(mockSearchRequest.source()); // Should not throw, source remains null
    }

    private WorkloadGroup createWorkloadGroup(String id, Map<String, String> searchSettings) {
        return new WorkloadGroup(
            "test-name",
            id,
            new MutableWorkloadGroupFragment(
                MutableWorkloadGroupFragment.ResiliencyMode.SOFT,
                Map.of(ResourceType.MEMORY, 0.5),
                searchSettings
            ),
            System.currentTimeMillis()
        );
    }
}
