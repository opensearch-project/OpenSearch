/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.tasks.ProtobufCancellableTask;
import org.opensearch.tasks.ProtobufTask;
import org.opensearch.tasks.ProtobufTaskManager;

import java.io.IOException;

/**
 * Registry for OpenSearch RequestHandlers
*
* @opensearch.internal
*/
public final class ProtobufRequestHandlerRegistry<Request extends ProtobufTransportRequest> {

    private final String action;
    private final ProtobufTransportRequestHandler<Request> handler;
    private final boolean forceExecution;
    private final boolean canTripCircuitBreaker;
    private final String executor;
    private final ProtobufTaskManager taskManager;
    private final ProtobufWriteable.Reader<Request> requestReader;

    public ProtobufRequestHandlerRegistry(
        String action,
        ProtobufWriteable.Reader<Request> requestReader,
        ProtobufTaskManager taskManager,
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

    public Request newRequest(StreamInput in) throws IOException {
        return requestReader.read(in);
    }

    public void processMessageReceived(Request request, TransportChannel channel) throws Exception {
        final ProtobufTask task = taskManager.register(channel.getChannelType(), action, request);
        ThreadContext.StoredContext contextToRestore = taskManager.taskExecutionStarted(task);

        Releasable unregisterTask = () -> taskManager.unregister(task);
        try {
            if (channel instanceof TcpTransportChannel && task instanceof ProtobufCancellableTask) {
                // if (request instanceof ShardSearchRequest) {
                //     // on receiving request, update the inbound network time to reflect time spent in transit over the network
                //     ((ShardSearchRequest) request).setInboundNetworkTime(
                //         Math.max(0, System.currentTimeMillis() - ((ShardSearchRequest) request).getInboundNetworkTime())
                //     );
                // }
                final TcpChannel tcpChannel = ((TcpTransportChannel) channel).getChannel();
                final Releasable stopTracking = taskManager.startTrackingCancellableChannelTask(tcpChannel, (ProtobufCancellableTask) task);
                unregisterTask = Releasables.wrap(unregisterTask, stopTracking);
            }
            final ProtobufTaskTransportChannel taskTransportChannel = new ProtobufTaskTransportChannel(channel, unregisterTask);
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

    public static <R extends ProtobufTransportRequest> RequestHandlerRegistry<R> replaceHandler(
        RequestHandlerRegistry<R> registry,
        ProtobufTransportRequestHandler<R> handler
    ) {
        return new RequestHandlerRegistry<>(
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
