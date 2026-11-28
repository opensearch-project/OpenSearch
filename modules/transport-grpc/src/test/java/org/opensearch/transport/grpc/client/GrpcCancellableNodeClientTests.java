/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.client;

import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;
import org.junit.Before;

import io.grpc.stub.ServerCallStreamObserver;
import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcCancellableNodeClientTests extends OpenSearchTestCase {

    private static final String LOCAL_NODE_ID = "local-node";
    private static final long TASK_ID = 42L;

    private NodeClient nodeClient;
    private ServerCallStreamObserver<SearchResponse> serverCallObserver;
    private Task task;
    private SearchRequest searchRequest;
    private ActionListener<SearchResponse> listener;
    private GrpcCancellableNodeClient cancellableClient;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        nodeClient = mock(NodeClient.class);
        serverCallObserver = mock(ServerCallStreamObserver.class);
        task = mock(Task.class);
        when(task.getId()).thenReturn(TASK_ID);
        when(nodeClient.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
        when(nodeClient.executeLocally(eq(SearchAction.INSTANCE), any(SearchRequest.class), any(ActionListener.class))).thenReturn(task);

        searchRequest = new SearchRequest();
        listener = mock(ActionListener.class);

        // Spy so the actual cancellation dispatch (which requires a real thread context) can be stubbed
        // while still exercising the real cancellation wiring in doExecute.
        cancellableClient = spy(new GrpcCancellableNodeClient(nodeClient, serverCallObserver));
        doNothing().when(cancellableClient).cancelTask(any(TaskId.class));
    }

    public void testExecuteLocallyIsUsedToCaptureTask() {
        cancellableClient.doExecute(SearchAction.INSTANCE, searchRequest, listener);

        // The task must be obtained via executeLocally (the plain Client.search discards it),
        // and the caller's listener must be passed through untouched.
        verify(nodeClient).executeLocally(eq(SearchAction.INSTANCE), eq(searchRequest), eq(listener));
    }

    public void testCancelHandlerCancelsTaskWhenCallIsCancelled() {
        cancellableClient.doExecute(SearchAction.INSTANCE, searchRequest, listener);

        // A cancel handler must be registered on the gRPC call, and firing it must cancel the captured task.
        ArgumentCaptor<Runnable> handlerCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(serverCallObserver).setOnCancelHandler(handlerCaptor.capture());

        // Not cancelled yet, so nothing should have been cancelled during setup.
        verify(cancellableClient, never()).cancelTask(any(TaskId.class));

        handlerCaptor.getValue().run();

        verify(cancellableClient).cancelTask(new TaskId(LOCAL_NODE_ID, TASK_ID));
    }

    public void testTaskCancelledImmediatelyWhenCallAlreadyCancelled() {
        // Simulate the race where the client cancels before the handler is registered.
        when(serverCallObserver.isCancelled()).thenReturn(true);

        cancellableClient.doExecute(SearchAction.INSTANCE, searchRequest, listener);

        verify(cancellableClient).cancelTask(new TaskId(LOCAL_NODE_ID, TASK_ID));
    }

    public void testTaskNotCancelledWhenCallCompletesNormally() {
        cancellableClient.doExecute(SearchAction.INSTANCE, searchRequest, listener);

        // Handler registered but never fired, call not cancelled: the task must be left running.
        verify(serverCallObserver).setOnCancelHandler(any(Runnable.class));
        verify(cancellableClient, never()).cancelTask(any(TaskId.class));
    }
}
