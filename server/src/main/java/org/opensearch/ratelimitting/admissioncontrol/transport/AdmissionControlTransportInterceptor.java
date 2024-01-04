/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.transport;

import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

/**
 * This class allows throttling by intercepting requests on both the sender and the receiver side.
 */
public class AdmissionControlTransportInterceptor implements TransportInterceptor {

    AdmissionControlService admissionControlService;

    public AdmissionControlTransportInterceptor(AdmissionControlService admissionControlService) {
        this.admissionControlService = admissionControlService;
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
}
