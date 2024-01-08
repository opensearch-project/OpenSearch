/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.*;

import java.io.IOException;

public class AdmissionControlInterceptSender {

    ThreadPool threadPool;
     public AdmissionControlInterceptSender(ThreadPool threadPool) {
         this.threadPool = threadPool;
     }
    private static final Logger logger = LogManager.getLogger(AdmissionControlInterceptSender.class);
    public <T extends TransportResponse> void sendRequestDecorate(
        TransportInterceptor.AsyncSender sender,
        Transport.Connection connection,
        String action,
        TransportRequest request,
        TransportRequestOptions options,
        TransportResponseHandler<T> handler
    ) {
        try (ThreadContext.StoredContext stashedContext = this.getThreadContext().stashContext()) {
            if(isActionIndexingOrSearch(action)){
                logger.info("AdmissionControlInterceptSender is Triggered Action: {}", action);
            }
            RestoringTransportResponseHandler restoringTransportResponseHandler = new RestoringTransportResponseHandler(handler, stashedContext, action);
            sender.sendRequest(connection, action, request, options, restoringTransportResponseHandler);
        }
    }

    private boolean isActionIndexingOrSearch(String action) {
        return action.startsWith("indices:data/read/search") || action.startsWith("indices:data/write/bulk");
    }

    private ThreadContext getThreadContext() {
        return threadPool.getThreadContext();
    }

    private static class RestoringTransportResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

        private final ThreadContext.StoredContext contextToRestore;
        private final TransportResponseHandler<T> innerHandler;

        private final String action;

        private RestoringTransportResponseHandler(TransportResponseHandler<T> innerHandler, ThreadContext.StoredContext contextToRestore, String action) {
            this.contextToRestore = contextToRestore;
            this.innerHandler = innerHandler;
            this.action = action;
        }

        @Override
        public T read(StreamInput in) throws IOException {
            return innerHandler.read(in);
        }

        @Override
        public void handleResponse(T response) {
            if (this.isActionIndexingOrSearch(this.action)){
                logger.info("Handle Response Triggered in: RestoringTransportResponseHandler");
            }
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

        private boolean isActionIndexingOrSearch(String action) {
            return action.startsWith("indices:data/read/search") || action.startsWith("indices:data/write/bulk");
        }
    }
}
