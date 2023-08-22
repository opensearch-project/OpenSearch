/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.throttling.admissioncontrol.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.tasks.Task;
import org.opensearch.throttling.admissioncontrol.AdmissionControlService;
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
    boolean canTripAdmissionControl;

    public AdmissionControlTransportHandler(
        String action,
        TransportRequestHandler<T> actualHandler,
        AdmissionControlService admissionControlService,
        boolean forceExecution,
        boolean canTripAdmissionControl
    ) {
        super();
        this.action = action;
        this.actualHandler = actualHandler;
        this.admissionControlService = admissionControlService;
        this.forceExecution = forceExecution;
        this.canTripAdmissionControl = canTripAdmissionControl;
    }

    public AdmissionControlTransportHandler(
        String action,
        TransportRequestHandler<T> actualHandler,
        AdmissionControlService admissionControlService,
        boolean forceExecution
    ) {
        this(action, actualHandler, admissionControlService, forceExecution, true);
    }

    /**
     * @param request Transport Request that landed on the node
     * @param channel Transport channel allows to send a response to a request
     * @param task Current task that is executing
     * @throws Exception when admission control rejected the requests
     */
    @Override
    public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
        // intercept all the transport requests here and apply admission control
        try {
            // Need to evaluate if we need to apply admission control or not if force Execution is true
//            if(this.action.startsWith("indices:data")){
            this.admissionControlService.applyTransportAdmissionControl(this.action);
//            }
        } catch (final OpenSearchRejectedExecutionException openSearchRejectedExecutionException) {
            channel.sendResponse(openSearchRejectedExecutionException);
            throw openSearchRejectedExecutionException;
        } catch (final Exception e) {
            throw e;
        }
        this.messageReceivedDecorate(request, actualHandler, channel, task);
    }

    /**
     *
     * @param request Transport Request that landed on the node
     * @param actualHandler is the next handler to intercept the request
     * @param transportChannel Transport channel allows to send a response to a request
     * @param task Current task that is executing
     * @throws Exception from the next handler
     */
    protected void messageReceivedDecorate(
        final T request,
        final TransportRequestHandler<T> actualHandler,
        final TransportChannel transportChannel,
        Task task
    ) throws Exception {
        actualHandler.messageReceived(request, transportChannel, task);
    }
}
