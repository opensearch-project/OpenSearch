/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.listeners;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.wlm.QueryGroupService;
import org.opensearch.wlm.QueryGroupTask;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.stats.QueryGroupState;
import org.opensearch.wlm.stats.QueryGroupStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class QueryGroupRequestOperationListenerTests extends OpenSearchTestCase {
    public static final int ITERATIONS = 20;
    ThreadPool testThreadPool;
    QueryGroupService queryGroupService;

    Map<String, QueryGroupState> queryGroupStateMap;
    String testQueryGroupId;
    QueryGroupRequestOperationListener sut;

    public void setUp() throws Exception {
        super.setUp();
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

        queryGroupService = new QueryGroupService(queryGroupStateMap);

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

            queryGroupService = new QueryGroupService(queryGroupStateMap);

            sut = new QueryGroupRequestOperationListener(queryGroupService, testThreadPool);
            sut.onRequestFailure(null, null);

            QueryGroupStats actualStats = queryGroupService.nodeStats();
            assertEquals(expectedStats, actualStats);
        }

    }
}
