/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport.Connection;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor.AsyncSender;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.Map;

public class SecurityInterceptor {

    protected final Logger log = LogManager.getLogger(getClass());
    private final ThreadPool threadPool;
    private final ClusterService cs;
    private final Settings settings;

    public SecurityInterceptor(final Settings settings, final ThreadPool threadPool, final ClusterService cs) {
        this.threadPool = threadPool;
        this.cs = cs;
        this.settings = settings;
    }

    public <T extends TransportRequest> SecurityRequestHandler<T> getHandler(String action, TransportRequestHandler<T> actualHandler) {
        return new SecurityRequestHandler<T>(action, actualHandler, threadPool, cs);
    }

    public <T extends TransportResponse> void sendRequestDecorate(
        AsyncSender sender,
        Connection connection,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<T> handler
    ) {

        final Map<String, String> origHeaders0 = getThreadContext().getHeaders();

        try (ThreadContext.StoredContext stashedContext = getThreadContext().stashContext()) {
            final TransportResponseHandler<T> restoringHandler = new RestoringTransportResponseHandler<T>(handler, stashedContext);
            sender.sendRequest(connection, action, request, options, restoringHandler);
        }
    }

    private ThreadContext getThreadContext() {
        return threadPool.getThreadContext();
    }

    // TODO This is used for tests, but should not have public access. Figure out how to re-factor.
    public Map<String, String> getHeaders() {
        return threadPool.getThreadContext().getHeaders();
    }

    // based on
    // org.opensearch.transport.TransportService.ContextRestoreResponseHandler<T>
    // which is private scoped
    private class RestoringTransportResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final ThreadContext.StoredContext contextToRestore;
        private final TransportResponseHandler<T> innerHandler;

        private RestoringTransportResponseHandler(TransportResponseHandler<T> innerHandler, ThreadContext.StoredContext contextToRestore) {
            this.contextToRestore = contextToRestore;
            this.innerHandler = innerHandler;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return innerHandler.read(in);
        }

        @Override
        public void handleResponse(T response) {
            contextToRestore.restore();
            innerHandler.handleResponse(response);
        }

        @Override
        public void handleException(TransportException e) {
            contextToRestore.restore();
            innerHandler.handleException(e);
        }

        @Override
        public String executor() {
            return innerHandler.executor();
        }
    }
}
