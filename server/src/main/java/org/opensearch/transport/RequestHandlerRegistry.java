/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;

import java.io.IOException;

/**
 * Registry for OpenSearch RequestHandlers
 *
 * @opensearch.internal
 */
public final class RequestHandlerRegistry<Request extends TransportRequest> {

    private final String action;
    private final TransportRequestHandler<Request> handler;
    private final boolean forceExecution;
    private final boolean canTripCircuitBreaker;
    private final String executor;
    private final TaskManager taskManager;
    private final Writeable.Reader<Request> requestReader;

    public RequestHandlerRegistry(
        String action,
        Writeable.Reader<Request> requestReader,
        TaskManager taskManager,
        TransportRequestHandler<Request> handler,
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
        final Task task = taskManager.register(channel.getChannelType(), action, request);
        ThreadContext.StoredContext contextToRestore = taskManager.taskExecutionStarted(task);

        Releasable unregisterTask = () -> taskManager.unregister(task);
        try {
            if (channel instanceof TcpTransportChannel && task instanceof CancellableTask) {
                if (request instanceof ShardSearchRequest) {
                    // on receiving request, update the inbound network time to reflect time spent in transit over the network
                    ((ShardSearchRequest) request).setInboundNetworkTime(
                        Math.max(0, System.currentTimeMillis() - ((ShardSearchRequest) request).getInboundNetworkTime())
                    );
                }
                final TcpChannel tcpChannel = ((TcpTransportChannel) channel).getChannel();
                final Releasable stopTracking = taskManager.startTrackingCancellableChannelTask(tcpChannel, (CancellableTask) task);
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

    public TransportRequestHandler<Request> getHandler() {
        return handler;
    }

    @Override
    public String toString() {
        return handler.toString();
    }

    public static <R extends TransportRequest> RequestHandlerRegistry<R> replaceHandler(
        RequestHandlerRegistry<R> registry,
        TransportRequestHandler<R> handler
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
