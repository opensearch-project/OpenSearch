/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

/**
 * This class is used to intercept search traffic requests and populate the queryGroupId header in task headers
 * TODO: We still need to add this interceptor in {@link org.opensearch.node.Node} class to enable,
 * leaving it until the feature is tested and done.
 */
public class SearchWorkloadTransportInterceptor implements TransportInterceptor {
    private final ThreadPool threadPool;

    public SearchWorkloadTransportInterceptor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler
    ) {
        return new SearchWorkloadTransportHandler<T>(threadPool, actualHandler);
    }
}
