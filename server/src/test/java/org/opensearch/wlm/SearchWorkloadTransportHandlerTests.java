/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import java.util.Collections;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SearchWorkloadTransportHandlerTests extends OpenSearchTestCase {
    private SearchWorkloadTransportHandler<TransportRequest> sut;
    private ThreadPool threadPool;

    private TransportRequestHandler<TransportRequest> actualHandler;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        actualHandler = new TestTransportRequestHandler<>();

        sut = new SearchWorkloadTransportHandler<>(threadPool, actualHandler);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testMessageReceivedForSearchWorkload() throws Exception {
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        Task spyTask = getSpyTask();

        sut.messageReceived(request, mock(TransportChannel.class), spyTask);

        verify(spyTask, times(1)).addHeader(
            QueryGroupConstants.QUERY_GROUP_ID_HEADER,
            threadPool.getThreadContext(),
            QueryGroupConstants.DEFAULT_QUERY_GROUP_ID_SUPPLIER
        );
    }

    public void testMessageReceivedForNonSearchWorkload() throws Exception {
        IndexRequest indexRequest = mock(IndexRequest.class);
        Task spyTask = getSpyTask();
        sut.messageReceived(indexRequest, mock(TransportChannel.class), spyTask);

        verify(spyTask, times(0)).addHeader(any(), any(), any());
    }

    private static Task getSpyTask() {
        final Task task = new Task(123, "transport", "Search", "test task", null, Collections.emptyMap());

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
