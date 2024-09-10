/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

<<<<<<< HEAD
<<<<<<< HEAD
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.metadata.QueryGroup;
=======
>>>>>>> b5cbfa4de9e (changelog)
=======
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.QueryGroup;
>>>>>>> 1a471881c21 (add tests)
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
<<<<<<< HEAD
<<<<<<< HEAD
import org.opensearch.wlm.MutableQueryGroupFragment;
=======
>>>>>>> b5cbfa4de9e (changelog)
=======
import org.opensearch.wlm.MutableQueryGroupFragment;
>>>>>>> 1a471881c21 (add tests)
import org.opensearch.wlm.QueryGroupService;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.QueryGroupsStateAccessor;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadManagementSettings;
import org.opensearch.wlm.cancellation.QueryGroupTaskCancellationService;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.stats.QueryGroupStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryGroupRequestOperationListenerTests extends OpenSearchTestCase {
    public static final int ITERATIONS = 20;
    ThreadPool testThreadPool;
    QueryGroupService queryGroupService;
    private QueryGroupTaskCancellationService taskCancellationService;
    private ClusterService mockClusterService;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    Map<String, QueryGroupState> queryGroupStateMap;
    String testQueryGroupId;
    QueryGroupRequestOperationListener sut;

    public void setUp() throws Exception {
        super.setUp();
        taskCancellationService = mock(QueryGroupTaskCancellationService.class);
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
            mock(DiscoveryNode.class),
            Map.of(
                testQueryGroupId,
                new QueryGroupStats.QueryGroupStatsHolder(
                    0,
                    0,
                    1,
                    0,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new QueryGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new QueryGroupStats.ResourceStats(0, 0, 0)
                    )
                ),
                QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get(),
                new QueryGroupStats.QueryGroupStatsHolder(
                    0,
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

        assertSuccess(testQueryGroupId, queryGroupStateMap, expectedStats, testQueryGroupId);
    }

    public void testResourceLimitBreached() throws IOException {
        QueryGroup queryGroup = new QueryGroup(
            "testgroup",
            new MutableQueryGroupFragment(MutableQueryGroupFragment.ResiliencyMode.MONITOR, Map.of(ResourceType.MEMORY, 0.5))
        );
        queryGroupStateMap.put(testQueryGroupId, new QueryGroupState());
        TransportService mockTransportService = mock(TransportService.class);
        DiscoveryNode mockDiscoveryNode = mock(DiscoveryNode.class);
        when(mockTransportService.getLocalNode()).thenReturn(mockDiscoveryNode);
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        Metadata metadata = mock(Metadata.class);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.queryGroups()).thenReturn(Map.of(testQueryGroupId, queryGroup));
        queryGroupService = new QueryGroupService(mockTransportService.getLocalNode(), clusterService, queryGroupStateMap);
        assertFalse(queryGroupService.resourceLimitBreached(testQueryGroupId, new QueryGroupState()));
    }

    public void testMultiThreadedValidQueryGroupRequestFailures() {

        queryGroupStateMap.put(testQueryGroupId, new QueryGroupState());
        TransportService mockTransportService = mock(TransportService.class);
        DiscoveryNode mockDiscoveryNode = mock(DiscoveryNode.class);
        when(mockTransportService.getLocalNode()).thenReturn(mockDiscoveryNode);
<<<<<<< HEAD
<<<<<<< HEAD
        QueryGroupsStateAccessor accessor = new QueryGroupsStateAccessor(queryGroupStateMap);
        setupMockedQueryGroupsFromClusterState();
        queryGroupService = new QueryGroupService(
            taskCancellationService,
            mockTransportService,
            mockClusterService,
            testThreadPool,
            mockWorkloadManagementSettings,
            null,
            accessor,
            Collections.emptySet(),
            Collections.emptySet()
        );
=======
        queryGroupService = new QueryGroupService(mockTransportService, queryGroupStateMap);
=======
        queryGroupService = new QueryGroupService(mockTransportService.getLocalNode(), mock(ClusterService.class), queryGroupStateMap);
>>>>>>> ffe0d7fa2cd (address comments)

>>>>>>> b5cbfa4de9e (changelog)
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

        Set<String> set = new HashSet<>();
        set.add("_all");
        QueryGroupStats actualStats = queryGroupService.nodeStats(set, null);

        QueryGroupStats expectedStats = new QueryGroupStats(
            mock(DiscoveryNode.class),
            Map.of(
                testQueryGroupId,
                new QueryGroupStats.QueryGroupStatsHolder(
                    0,
                    0,
                    ITERATIONS,
                    0,
                    0,
                    Map.of(
                        ResourceType.CPU,
                        new QueryGroupStats.ResourceStats(0, 0, 0),
                        ResourceType.MEMORY,
                        new QueryGroupStats.ResourceStats(0, 0, 0)
                    )
                ),
                QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get(),
                new QueryGroupStats.QueryGroupStatsHolder(
                    0,
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

        assertEquals(expectedStats, actualStats);
    }

    public void testInvalidQueryGroupFailure() throws IOException {
        QueryGroupStats expectedStats = new QueryGroupStats(
            mock(DiscoveryNode.class),
            Map.of(
                testQueryGroupId,
                new QueryGroupStats.QueryGroupStatsHolder(
                    0,
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
                ),
                QueryGroupTask.DEFAULT_QUERY_GROUP_ID_SUPPLIER.get(),
                new QueryGroupStats.QueryGroupStatsHolder(
                    0,
                    0,
                    1,
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
        QueryGroupsStateAccessor stateAccessor = new QueryGroupsStateAccessor(queryGroupStateMap);
        TransportService mockTransportService = mock(TransportService.class);
        DiscoveryNode mockDiscoveryNode = mock(DiscoveryNode.class);
        when(mockTransportService.getLocalNode()).thenReturn(mockDiscoveryNode);
        try (ThreadContext.StoredContext currentContext = testThreadPool.getThreadContext().stashContext()) {
            testThreadPool.getThreadContext().putHeader(QueryGroupTask.QUERY_GROUP_ID_HEADER, threadContextQG_Id);
            queryGroupStateMap.put(testQueryGroupId, new QueryGroupState());
<<<<<<< HEAD
            setupMockedQueryGroupsFromClusterState();

            queryGroupService = new QueryGroupService(
                taskCancellationService,
                mockTransportService,
                mockClusterService,
                testThreadPool,
                mockWorkloadManagementSettings,
                null,
                stateAccessor,
                Collections.emptySet(),
                Collections.emptySet()
            );
=======
            TransportService mockTransportService = mock(TransportService.class);
            DiscoveryNode mockDiscoveryNode = mock(DiscoveryNode.class);
            when(mockTransportService.getLocalNode()).thenReturn(mockDiscoveryNode);
<<<<<<< HEAD
            queryGroupService = new QueryGroupService(mockTransportService, queryGroupStateMap);
>>>>>>> b5cbfa4de9e (changelog)
=======
            queryGroupService = new QueryGroupService(mockTransportService.getLocalNode(), mock(ClusterService.class), queryGroupStateMap);
>>>>>>> ffe0d7fa2cd (address comments)

            sut = new QueryGroupRequestOperationListener(queryGroupService, testThreadPool);
            sut.onRequestFailure(null, null);

<<<<<<< HEAD
            QueryGroupStats actualStats = queryGroupService.nodeStats();
=======
            Set<String> set = new HashSet<>();
            set.add("_all");
            QueryGroupStats actualStats = queryGroupService.nodeStats(set, null);
>>>>>>> ffe0d7fa2cd (address comments)

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
