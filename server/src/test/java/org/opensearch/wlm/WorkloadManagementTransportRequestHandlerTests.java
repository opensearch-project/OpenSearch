/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import java.util.Collections;
import java.util.Optional;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class WorkloadManagementTransportRequestHandlerTests extends OpenSearchTestCase {
    private WorkloadManagementTransportInterceptor.RequestHandler<TransportRequest> sut;
    private ThreadPool threadPool;
    private QueryGroupService queryGroupService;

    private TestTransportRequestHandler<TransportRequest> actualHandler;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        actualHandler = new TestTransportRequestHandler<>();
        queryGroupService = mock(QueryGroupService.class);

        sut = new WorkloadManagementTransportInterceptor.RequestHandler<>(threadPool, actualHandler, queryGroupService);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testMessageReceivedForSearchWorkload_nonRejectionCase() throws Exception {
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        QueryGroupTask spyTask = getSpyTask();
        when(queryGroupService.shouldRejectFor(anyString())).thenReturn(Optional.empty());

        sut.messageReceived(request, mock(TransportChannel.class), spyTask);
        assertTrue(sut.isSearchWorkloadRequest(spyTask));
    }

    public void testMessageReceivedForSearchWorkload_RejectionCase() throws Exception {
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        QueryGroupTask spyTask = getSpyTask();
        when(queryGroupService.shouldRejectFor(anyString())).thenReturn(Optional.of("QueryGroup is contended."));

        assertThrows(OpenSearchRejectedExecutionException.class, () -> sut.messageReceived(request, mock(TransportChannel.class), spyTask));
    }

    public void testMessageReceivedForNonSearchWorkload() throws Exception {
        IndexRequest indexRequest = mock(IndexRequest.class);
        Task task = mock(Task.class);
        sut.messageReceived(indexRequest, mock(TransportChannel.class), task);
        assertFalse(sut.isSearchWorkloadRequest(task));
        assertEquals(1, actualHandler.invokeCount);
    }

    private static QueryGroupTask getSpyTask() {
        final QueryGroupTask task = new QueryGroupTask(123, "transport", "Search", "test task", null, Collections.emptyMap());

        return spy(task);
    }

    private static class TestTransportRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {
        int invokeCount = 0;

        @Override
        public void messageReceived(TransportRequest request, TransportChannel channel, Task task) throws Exception {
            invokeCount += 1;
        }
    };
}
