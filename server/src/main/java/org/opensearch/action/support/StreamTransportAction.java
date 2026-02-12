/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.action.ActionRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Base class for streaming transport actions.
 * <p>
 * This class provides a framework for actions that need to send multiple responses
 * for a single request (streaming). Subclasses implement {@link #executeStream}
 * which receives a {@link TransportChannel} for sending batched responses.
 * <p>
 * The standard {@link #doExecute} method is implemented to extract the channel
 * from the listener and delegate to {@link #executeStream}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class StreamTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends TransportAction<
    Request,
    Response> {

    protected final TransportService transportService;
    protected final StreamTransportService streamTransportService;

    protected StreamTransportAction(
        String actionName,
        ActionFilters actionFilters,
        TaskManager taskManager,
        TransportService transportService,
        StreamTransportService streamTransportService
    ) {
        super(actionName, actionFilters, taskManager);
        this.transportService = transportService;
        this.streamTransportService = streamTransportService;
    }

    @Override
    protected final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        // Extract the channel from the streaming listener
        if (!(listener instanceof HandledStreamTransportAction.StreamingActionListener)) {
            listener.onFailure(new IllegalStateException("This action only supports streaming execution via HandledStreamTransportAction"));
            return;
        }

        @SuppressWarnings("unchecked")
        HandledStreamTransportAction.StreamingActionListener<Response, Request> streamingListener =
            (HandledStreamTransportAction.StreamingActionListener<Response, Request>) listener;
        TransportChannel channel = streamingListener.getChannel();

        try {
            executeStream(task, request, channel);
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }

    /**
     * Execute the streaming request by sending it to the local node via StreamTransportService.
     * This method is called by NodeClient.executeStream() to initiate the streaming flow.
     *
     * Uses transportService.getLocalNode() to get the local node with all attributes including
     * the streaming port, then sends the request via streamTransportService.
     *
     * @param request the request
     * @param handler the streaming response handler
     */
    public void executeStreamRequest(Request request, StreamTransportResponseHandler<Response> handler) {
        // Send streaming request to local node via StreamTransportService
        // Use transportService.getLocalNode() which has all attributes including streaming port
        streamTransportService.sendRequest(
            transportService.getLocalNode(),
            actionName,
            request,
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            handler
        );
    }

    /**
     * Execute the streaming request.
     * <p>
     * Implementations should:
     * <ul>
     *   <li>Send multiple responses via {@link TransportChannel#sendResponseBatch}</li>
     *   <li>Call {@link TransportChannel#completeStream} when done</li>
     *   <li>Handle errors by calling {@link TransportChannel#sendResponse} with the exception</li>
     * </ul>
     *
     * @param task the task
     * @param request the request
     * @param channel the transport channel for sending responses
     * @throws IOException if an I/O error occurs
     */
    protected abstract void executeStream(Task task, Request request, TransportChannel channel) throws IOException;
}
