/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.ProtobufWriteable;
import org.opensearch.tasks.ProtobufCancellableTask;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.tasks.TaskManager;

import java.io.IOException;

/**
 * Registry for OpenSearch RequestHandlers
*
* @opensearch.internal
*/
public final class ProtobufRequestHandlerRegistry<Request extends TransportRequest> {

    private final String action;
    private final ProtobufTransportRequestHandler<Request> handler;
    private final boolean forceExecution;
    private final boolean canTripCircuitBreaker;
    private final String executor;
    private final TaskManager taskManager;
    private final ProtobufWriteable.Reader<Request> requestReader;

    public ProtobufRequestHandlerRegistry(
        String action,
        ProtobufWriteable.Reader<Request> requestReader,
        TaskManager taskManager,
        ProtobufTransportRequestHandler<Request> handler,
        String executor,
        boolean forceExecution,
        boolean canTripCircuitBreaker
    ) {
        this.action = action;
        this.requestReader = requestReader;
        this.handler = handler;
        this.forceExecution = forceExecution;
        this.canTripCircuitBreaker = canTripCircuitBreaker;
        this.executor = executor;
        this.taskManager = taskManager;
    }

    public String getAction() {
        return action;
    }

    public Request newRequest(byte[] in) throws IOException {
        return requestReader.read(in);
    }

    public void processMessageReceived(Request request, TransportChannel channel) throws Exception {
        final ProtobufTask task = taskManager.registerProtobuf(channel.getChannelType(), action, request);
        ThreadContext.StoredContext contextToRestore = taskManager.protobufTaskExecutionStarted(task);

        Releasable unregisterTask = () -> taskManager.unregisterProtobufTask(task);
        try {
            if (channel instanceof TcpTransportChannel && task instanceof ProtobufCancellableTask) {
                final TcpChannel tcpChannel = ((TcpTransportChannel) channel).getChannel();
                final Releasable stopTracking = taskManager.startProtobufTrackingCancellableChannelTask(
                    tcpChannel,
                    (ProtobufCancellableTask) task
                );
                unregisterTask = Releasables.wrap(unregisterTask, stopTracking);
            }
            final TaskTransportChannel taskTransportChannel = new TaskTransportChannel(channel, unregisterTask);
            handler.messageReceived(request, taskTransportChannel, task);
            unregisterTask = null;
        } finally {
            Releasables.close(unregisterTask);
            contextToRestore.restore();
        }
    }

    public boolean isForceExecution() {
        return forceExecution;
    }

    public boolean canTripCircuitBreaker() {
        return canTripCircuitBreaker;
    }

    public String getExecutor() {
        return executor;
    }

    public ProtobufTransportRequestHandler<Request> getHandler() {
        return handler;
    }

    @Override
    public String toString() {
        return handler.toString();
    }

    public static <R extends TransportRequest> ProtobufRequestHandlerRegistry<R> replaceHandler(
        ProtobufRequestHandlerRegistry<R> registry,
        ProtobufTransportRequestHandler<R> handler
    ) {
        return new ProtobufRequestHandlerRegistry<>(
            registry.action,
            registry.requestReader,
            registry.taskManager,
            handler,
            registry.executor,
            registry.forceExecution,
            registry.canTripCircuitBreaker
        );
    }
}
