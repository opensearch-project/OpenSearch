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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tasks.Task;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

/**
 * Base class for streaming transport actions that are executed on the local node.
 * Similar to {@link HandledTransportAction} but registers with {@link StreamTransportService}
 * to handle streaming requests.
 * <p>
 * This class properly integrates with the action filter chain and task management by
 * delegating to the standard {@link TransportAction#execute} method, but provides
 * a streaming-specific channel for sending responses.
 * <p>
 * Subclasses should extend {@link StreamTransportAction} instead and register their own
 * handlers if they need more control.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class HandledStreamTransportAction<Request extends ActionRequest, Response extends ActionResponse> extends
    StreamTransportAction<Request, Response> {

    protected final StreamTransportService streamTransportService;

    protected HandledStreamTransportAction(
        String actionName,
        TransportService transportService,
        StreamTransportService streamTransportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader
    ) {
        this(actionName, transportService, streamTransportService, actionFilters, requestReader, "generic");
    }

    protected HandledStreamTransportAction(
        String actionName,
        TransportService transportService,
        StreamTransportService streamTransportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        String executor
    ) {
        super(actionName, actionFilters, transportService.getTaskManager(), transportService, streamTransportService);
        this.streamTransportService = streamTransportService;

        // Register streaming handler - similar to HandledTransportAction but for streaming
        streamTransportService.registerRequestHandler(actionName, executor, requestReader, new StreamTransportHandler<>(this));
    }

    /**
     * Inner transport handler for streaming requests.
     * This can be used by actions that want to register their own handlers.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class StreamTransportHandler<Request extends ActionRequest, Response extends ActionResponse>
        implements
            TransportRequestHandler<Request> {

        private final StreamTransportAction<Request, Response> action;

        public StreamTransportHandler(StreamTransportAction<Request, Response> action) {
            this.action = action;
        }

        @Override
        public final void messageReceived(final Request request, final TransportChannel channel, Task task) {
            // Create a special listener that provides access to the channel for streaming
            ActionListener<Response> listener = new StreamingActionListener<>(channel, action.actionName, request);
            // Execute through the standard action filter chain
            action.execute(task, request, listener);
        }
    }

    /**
     * Special action listener that provides access to the transport channel for streaming responses.
     * Similar to {@link ChannelActionListener} but provides access to the channel for streaming.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class StreamingActionListener<Response extends ActionResponse, Request extends TransportRequest>
        implements
            ActionListener<Response> {

        private final TransportChannel channel;
        private final Request request;
        private final String actionName;

        public StreamingActionListener(TransportChannel channel, String actionName, Request request) {
            this.channel = channel;
            this.request = request;
            this.actionName = actionName;
        }

        /**
         * Get the transport channel for sending streaming responses
         */
        public TransportChannel getChannel() {
            return channel;
        }

        @Override
        public void onResponse(Response response) {
            try {
                channel.sendResponse(response);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            TransportChannel.sendErrorResponse(channel, actionName, request, e);
        }
    }
}
