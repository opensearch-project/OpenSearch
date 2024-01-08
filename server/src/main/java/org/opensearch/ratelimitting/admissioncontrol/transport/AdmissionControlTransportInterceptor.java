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
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.*;

/**
 * This class allows throttling by intercepting requests on both the sender and the receiver side.
 */
public class AdmissionControlTransportInterceptor implements TransportInterceptor {

    AdmissionControlService admissionControlService;
    AdmissionControlInterceptSender admissionControlInterceptSender;

    private static final Logger logger = LogManager.getLogger(AdmissionControlTransportInterceptor.class);

    public AdmissionControlTransportInterceptor(AdmissionControlService admissionControlService, ThreadPool threadPool) {
        this.admissionControlService = admissionControlService;
        admissionControlInterceptSender = new AdmissionControlInterceptSender(threadPool);
    }

    /**
     *
     * @return admissionController handler to intercept transport requests
     */
    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler,
        AdmissionControlActionType admissionControlActionType
    ) {
        return new AdmissionControlTransportHandler<>(
            action,
            actualHandler,
            this.admissionControlService,
            forceExecution,
            admissionControlActionType
        );
    }

    @Override
    public AsyncSender interceptSender(AsyncSender sender) {
        logger.info("AdmissionControl Intercept Sender Initialised");
        return new AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                admissionControlInterceptSender.sendRequestDecorate(sender, connection, action, request, options, handler);
            }
        };
    }
}
