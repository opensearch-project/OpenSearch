/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.WorkloadGroupService;
import org.opensearch.wlm.WorkloadGroupTask;
import org.opensearch.wlm.WorkloadGroupsStateAccessor;
import org.opensearch.wlm.ResourceType;
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
    WorkloadGroupService queryGroupService;
    private WorkloadGroupTaskCancellationService taskCancellationService;
    private ClusterService mockClusterService;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    Map<String, WorkloadGroupState> queryGroupStateMap;
    String testWorkloadGroupId;
    WorkloadGroupRequestOperationListener sut;

    public void setUp() throws Exception {
        super.setUp();
        taskCancellationService = mock(WorkloadGroupTaskCancellationService.class);
        mockClusterService = mock(ClusterService.class);
        mockWorkloadManagementSettings = mock(WorkloadManagementSettings.class);
        queryGroupStateMap = new HashMap<>();
        testWorkloadGroupId = "safjgagnakg-3r3fads";
        testThreadPool = new TestThreadPool("RejectionTestThreadPool");
        ClusterState mockClusterState = mock(ClusterState.class);
        when(mockClusterService.state()).thenReturn(mockClusterState);
        Metadata mockMetaData = mock(Metadata.class);
        when(mockClusterState.metadata()).thenReturn(mockMetaData);
        queryGroupService = mock(WorkloadGroupService.class);
        sut = new WorkloadGroupRequestOperationListener(queryGroupService, testThreadPool);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        testThreadPool.shutdown();
    }

    public void testRejectionCase() {
        final String testWorkloadGroupId = "asdgasgkajgkw3141_3rt4t";
        testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, testWorkloadGroupId);
        doThrow(OpenSearchRejectedExecutionException.class).when(queryGroupService).rejectIfNeeded(testWorkloadGroupId);
        assertThrows(OpenSearchRejectedExecutionException.class, () -> sut.onRequestStart(null));
    }

    public void testNonRejectionCase() {
        final String testWorkloadGroupId = "asdgasgkajgkw3141_3rt4t";
        testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, testWorkloadGroupId);
        doNothing().when(queryGroupService).rejectIfNeeded(testWorkloadGroupId);

        sut.onRequestStart(null);
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

        assertSuccess(testWorkloadGroupId, queryGroupStateMap, expectedStats, testWorkloadGroupId);
    }

    public void testMultiThreadedValidWorkloadGroupRequestFailures() {

        queryGroupStateMap.put(testWorkloadGroupId, new WorkloadGroupState());
        WorkloadGroupsStateAccessor accessor = new WorkloadGroupsStateAccessor(queryGroupStateMap);
        setupMockedWorkloadGroupsFromClusterState();
        queryGroupService = new WorkloadGroupService(
            taskCancellationService,
            mockClusterService,
            testThreadPool,
            mockWorkloadManagementSettings,
            null,
            accessor,
            Collections.emptySet(),
            Collections.emptySet()
        );

        sut = new WorkloadGroupRequestOperationListener(queryGroupService, testThreadPool);

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
        WorkloadGroupStats actualStats = queryGroupService.nodeStats(set, null);

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

        assertSuccess(testWorkloadGroupId, queryGroupStateMap, expectedStats, "dummy-invalid-qg-id");

    }

    private void assertSuccess(
        String testWorkloadGroupId,
        Map<String, WorkloadGroupState> queryGroupStateMap,
        WorkloadGroupStats expectedStats,
        String threadContextQG_Id
    ) {
        WorkloadGroupsStateAccessor stateAccessor = new WorkloadGroupsStateAccessor(queryGroupStateMap);
        try (ThreadContext.StoredContext currentContext = testThreadPool.getThreadContext().stashContext()) {
            testThreadPool.getThreadContext().putHeader(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, threadContextQG_Id);
            queryGroupStateMap.put(testWorkloadGroupId, new WorkloadGroupState());

            setupMockedWorkloadGroupsFromClusterState();

            queryGroupService = new WorkloadGroupService(
                taskCancellationService,
                mockClusterService,
                testThreadPool,
                mockWorkloadManagementSettings,
                null,
                stateAccessor,
                Collections.emptySet(),
                Collections.emptySet()
            );
            sut = new WorkloadGroupRequestOperationListener(queryGroupService, testThreadPool);
            sut.onRequestFailure(null, null);

            HashSet<String> set = new HashSet<>();
            set.add("_all");
            WorkloadGroupStats actualStats = queryGroupService.nodeStats(set, null);
            assertEquals(expectedStats, actualStats);
        }

    }

    private void setupMockedWorkloadGroupsFromClusterState() {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);
        when(metadata.queryGroups()).thenReturn(Collections.emptyMap());
    }
}
