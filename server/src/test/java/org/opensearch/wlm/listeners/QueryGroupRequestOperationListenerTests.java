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
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.QueryGroupService;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.cancellation.TaskCancellationService;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.stats.QueryGroupStats;
import org.opensearch.wlm.tracker.QueryGroupResourceUsageTrackerService;

import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class QueryGroupRequestOperationListenerTests extends OpenSearchTestCase {
    public static final int ITERATIONS = 20;
    ThreadPool testThreadPool;
    QueryGroupService queryGroupService;
    private TaskCancellationService taskCancellationService;
    private ClusterService mockClusterService;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    Map<String, QueryGroupState> queryGroupStateMap;
    String testQueryGroupId;
    QueryGroupRequestOperationListener sut;

    public void setUp() throws Exception {
        super.setUp();
        taskCancellationService = mock(TaskCancellationService.class);
        mockClusterService = mock(ClusterService.class);
        mockWorkloadManagementSettings = mock(WorkloadManagementSettings.class);
        queryGroupStateMap = new HashMap<>();
        testQueryGroupId = "safjgagnakg-3r3fads";
        testThreadPool = new TestThreadPool("RejectionTestThreadPool");
        queryGroupService = mock(QueryGroupService.class);
        sut = new QueryGroupRequestOperationListener(queryGroupService, testThreadPool);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        testThreadPool.shutdown();
    }

    public void testRejectionCase() {
        final String testQueryGroupId = "asdgasgkajgkw3141_3rt4t";
        testThreadPool.getThreadContext().putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, testQueryGroupId);
        doThrow(OpenSearchRejectedExecutionException.class).when(queryGroupService).rejectIfNeeded(testQueryGroupId);
        assertThrows(OpenSearchRejectedExecutionException.class, () -> sut.onRequestStart(null));
    }

    public void testNonRejectionCase() {
        final String testQueryGroupId = "asdgasgkajgkw3141_3rt4t";
        testThreadPool.getThreadContext().putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, testQueryGroupId);
        doNothing().when(queryGroupService).rejectIfNeeded(testQueryGroupId);

        sut.onRequestStart(null);
    }

    public void testValidQueryGroupRequestFailure() throws IOException {

        QueryGroupStats expectedStats = new QueryGroupStats(
            Map.of(
                testQueryGroupId,
                new QueryGroupStats.QueryGroupStatsHolder(
                    0,
                    0,
                    1,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new QueryGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new QueryGroupStats.ResourceStats(0, 0, 0)
                    )
                )
            )
        );

        assertSuccess(testQueryGroupId, queryGroupStateMap, expectedStats, testQueryGroupId);
    }

    public void testMultiThreadedValidQueryGroupRequestFailures() {

        queryGroupStateMap.put(testQueryGroupId, new QueryGroupState());
        setupMockedQueryGroupsFromClusterState();
        queryGroupService = new QueryGroupService(
            taskCancellationService,
            mockClusterService,
            testThreadPool,
            mockWorkloadManagementSettings,
            null,
            queryGroupStateMap
        );

        sut = new QueryGroupRequestOperationListener(queryGroupService, testThreadPool);

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            threads.add(new Thread(() -> {
                try (ThreadContext.StoredContext currentContext = testThreadPool.getThreadContext().stashContext()) {
                    testThreadPool.getThreadContext().putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, testQueryGroupId);
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

        QueryGroupStats actualStats = queryGroupService.nodeStats();

        QueryGroupStats expectedStats = new QueryGroupStats(
            Map.of(
                testQueryGroupId,
                new QueryGroupStats.QueryGroupStatsHolder(
                    0,
                    0,
                    ITERATIONS,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new QueryGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new QueryGroupStats.ResourceStats(0, 0, 0)
                    )
                )
            )
        );

        assertEquals(expectedStats, actualStats);
    }

    public void testInvalidQueryGroupFailure() throws IOException {
        QueryGroupStats expectedStats = new QueryGroupStats(
            Map.of(
                testQueryGroupId,
                new QueryGroupStats.QueryGroupStatsHolder(
                    0,
                    0,
                    0,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new QueryGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new QueryGroupStats.ResourceStats(0, 0, 0)
                    )
                )
            )
        );

        assertSuccess(testQueryGroupId, queryGroupStateMap, expectedStats, "dummy-invalid-qg-id");

    }

    private void assertSuccess(
        String testQueryGroupId,
        Map<String, QueryGroupState> queryGroupStateMap,
        QueryGroupStats expectedStats,
        String threadContextQG_Id
    ) {

        try (ThreadContext.StoredContext currentContext = testThreadPool.getThreadContext().stashContext()) {
            testThreadPool.getThreadContext().putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, threadContextQG_Id);
            queryGroupStateMap.put(testQueryGroupId, new QueryGroupState());

            setupMockedQueryGroupsFromClusterState();

            queryGroupService = new QueryGroupService(
                taskCancellationService,
                mockClusterService,
                testThreadPool,
                mockWorkloadManagementSettings,
                null,
                queryGroupStateMap
            );

            sut = new QueryGroupRequestOperationListener(queryGroupService, testThreadPool);
            sut.onRequestFailure(null, null);

            QueryGroupStats actualStats = queryGroupService.nodeStats();
            assertEquals(expectedStats, actualStats);
        }

    }

    private void setupMockedQueryGroupsFromClusterState() {
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);
        when(metadata.queryGroups()).thenReturn(Collections.emptyMap());
    }
}
