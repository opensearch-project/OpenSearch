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

import java.io.IOException;

/** Streams transport responses through a {@link TransportChannel}. */
@ExperimentalApi
public class StreamSearchChannelListener<Response extends TransportResponse>
    implements
        ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(StreamSearchChannelListener.class);
    private final TransportChannel channel;
    private final String actionName;

    private final java.util.concurrent.atomic.AtomicBoolean completed = new java.util.concurrent.atomic.AtomicBoolean(false);

    public StreamSearchChannelListener(TransportChannel channel, String actionName) {
        this.channel = channel;
        this.actionName = actionName;
    }

    /** Sends a streamed response batch and optionally completes the stream. */
    public void onStreamResponse(Response response, boolean isLastBatch) {
        assert response != null;
        if (completed.get()) {
            return;
        }
        try {
            channel.sendResponseBatch(response);
            if (isLastBatch) {
                channel.completeStream();
                completed.set(true);
            }
        } catch (Exception e) {
            logger.warn("Failed to send streaming response on channel for action [{}]", actionName, e);
            throw e;
        }
    }

    @Override
    public final void onResponse(Response response) {
        onStreamResponse(response, true);
    }

    @Override
    public void onFailure(Exception e) {
        if (completed.getAndSet(true)) {
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
