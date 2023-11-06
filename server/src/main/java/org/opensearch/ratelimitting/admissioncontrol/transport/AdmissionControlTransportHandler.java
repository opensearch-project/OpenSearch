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
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

/**
 * AdmissionControl Handler to intercept Transport Requests.
 * @param <T> Transport Request
 */
public class AdmissionControlTransportHandler<T extends TransportRequest> implements TransportRequestHandler<T> {

    private final String action;
    private final TransportRequestHandler<T> actualHandler;
    protected final Logger log = LogManager.getLogger(this.getClass());
    AdmissionControlService admissionControlService;
    boolean forceExecution;
    AdmissionControlActionType admissionControlActionType;

    public AdmissionControlTransportHandler(
        String action,
        TransportRequestHandler<T> actualHandler,
        AdmissionControlService admissionControlService,
        boolean forceExecution,
        AdmissionControlActionType admissionControlActionType
    ) {
        super();
        this.action = action;
        this.actualHandler = actualHandler;
        this.admissionControlService = admissionControlService;
        this.forceExecution = forceExecution;
        this.admissionControlActionType = admissionControlActionType;
    }

    /**
     * @param request Transport Request that landed on the node
     * @param channel Transport channel allows to send a response to a request
     * @param task Current task that is executing
     * @throws Exception when admission control rejected the requests
     */
    @Override
    public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
        // skip admission control if force execution is true
        if (!this.forceExecution) {
            // intercept the transport requests here and apply admission control
            try {
                this.admissionControlService.applyTransportAdmissionControl(this.action, this.admissionControlActionType);
            } catch (final OpenSearchRejectedExecutionException openSearchRejectedExecutionException) {
                log.warn(openSearchRejectedExecutionException.getMessage());
                channel.sendResponse(openSearchRejectedExecutionException);
                return;
            }
        }
        actualHandler.messageReceived(request, channel, task);
    }
}
