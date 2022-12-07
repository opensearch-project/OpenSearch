/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

public class SecurityRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

    private final String action;
    private final TransportRequestHandler<T> actualHandler;
    private final ThreadPool threadPool;
    private final ClusterService cs;

    SecurityRequestHandler(String action,
                           final TransportRequestHandler<T> actualHandler,
                           final ThreadPool threadPool,
                           final ClusterService cs) {
        this.action = action;
        this.actualHandler = actualHandler;
        this.threadPool = threadPool;
        this.cs = cs;
    }

    @Override
    public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
        actualHandler.messageReceived(request, channel, task);
    }
}
