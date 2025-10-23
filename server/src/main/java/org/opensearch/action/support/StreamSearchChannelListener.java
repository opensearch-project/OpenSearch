/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * A listener that sends the response back to the channel in streaming fashion.
 *
 * - onStreamResponse(): Send streaming responses
 * - onResponse(): Standard ActionListener method that send last stream response
 * - onFailure(): Handle errors and complete the stream
 */
@ExperimentalApi
public class StreamSearchChannelListener<Response extends TransportResponse, Request extends TransportRequest>
    implements
        ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(StreamSearchChannelListener.class);
    private final TransportChannel channel;
    private final Request request;
    private final String actionName;

    private final java.util.concurrent.atomic.AtomicBoolean completed = new java.util.concurrent.atomic.AtomicBoolean(false);

    public StreamSearchChannelListener(TransportChannel channel, String actionName, Request request) {
        this.channel = channel;
        this.request = request;
        this.actionName = actionName;
    }

    /**
     * Send streaming responses
     * This allows multiple responses to be sent for a single request.
     *
     * @param response    the intermediate response to send
     * @param isLastBatch whether this response is the last one
     */
    public void onStreamResponse(Response response, boolean isLastBatch) {
        assert response != null;
        if (completed.get()) {
            // Ignore late responses after completion to avoid double-completion and task tracker mismatches
            return;
        }
        channel.sendResponseBatch(response);
        if (isLastBatch) {
            try {
                channel.completeStream();
            } finally {
                completed.set(true);
            }
        }
    }

    /**
     * Reuse ActionListener method to send the last stream response
     * This maintains compatibility on data node side
     *
     * @param response the response to send
     */
    @Override
    public final void onResponse(Response response) {
        onStreamResponse(response, true);
    }

    @Override
    public void onFailure(Exception e) {
        // Ensure we only fail once per request/channel to keep task tracker consistent
        if (completed.getAndSet(true)) {
            // Already completed (success or failure); drop duplicate failure
            return;
        }
        try {
            channel.sendResponse(e);
        } catch (IOException exc) {
            logger.warn("Failed to send error response on streaming channel", exc);
            throw new RuntimeException(exc);
        }
    }
}
