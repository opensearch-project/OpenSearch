/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.http.HttpChannel;
import org.opensearch.rest.RestChannel;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ActionListener wrapper that ensures REST channels are properly untracked from
 * RestCancellableNodeClient when streaming requests reach terminal states.
 *
 * This wrapper detects terminal signals (onResponse, onFailure) and ensures
 * exactly-once untracking semantics for REST channel lifecycle management.
 *
 * @param <T> the response type
 */
public class TerminalChannelReleasingListener<T extends ActionResponse> implements ActionListener<T> {
    private static final Logger logger = LogManager.getLogger(TerminalChannelReleasingListener.class);

    private final RestCancellableNodeClient client;
    private final RestChannel channel;
    private final HttpChannel httpChannel;
    private final ActionListener<T> delegate;
    private final AtomicBoolean finished;

    public TerminalChannelReleasingListener(RestCancellableNodeClient client, RestChannel channel, ActionListener<T> delegate) {
        this.client = client;
        this.channel = channel;
        this.httpChannel = channel != null ? channel.request().getHttpChannel() : null;
        this.delegate = delegate;
        this.finished = new AtomicBoolean(false);
    }

    @Override
    public void onResponse(T response) {
        if (ensureTerminalHandling()) {
            try {
                delegate.onResponse(response);
            } finally {
                finishTracking();
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (ensureTerminalHandling()) {
            try {
                delegate.onFailure(e);
            } finally {
                finishTracking();
            }
        }
    }

    /**
     * Sets up backstop mechanisms to handle edge cases where terminal signals may not be received.
     */
    public void setupBackstops() {
        if (httpChannel != null) {
            // Add HTTP channel close listener as a backstop
            httpChannel.addCloseListener(new org.opensearch.core.action.ActionListener<Void>() {
                @Override
                public void onResponse(Void aVoid) {
                    if (finished.compareAndSet(false, true)) {
                        logger.debug("HTTP channel closed before terminal signal - performing backstop cleanup");
                        finishTracking();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // Only untrack, don't re-enter response path
                    if (finished.compareAndSet(false, true)) {
                        logger.debug("HTTP channel closed with error - performing backstop cleanup");
                        finishTracking();
                    }
                }
            });
        }
    }

    /**
     * Ensures terminal handling occurs exactly once.
     * @return true if this call should proceed with terminal handling, false if already handled
     */
    private boolean ensureTerminalHandling() {
        return finished.compareAndSet(false, true);
    }

    /**
     * Safely finish tracking the HttpChannel from the client.
     */
    private void finishTracking() {
        try {
            if (client != null && httpChannel != null) {
                client.finishTracking(httpChannel);
                logger.debug("Successfully finished tracking HTTP channel for streaming request");
            }
        } catch (Exception e) {
            logger.warn("Failed to finish tracking HTTP channel during terminal handling", e);
        }
    }

    /**
     * Provides access to the underlying channel for external untracking scenarios.
     * @return the REST channel being managed
     */
    public RestChannel getChannel() {
        return channel;
    }

    /**
     * Checks if terminal handling has already occurred.
     * @return true if terminal state has been reached
     */
    public boolean isFinished() {
        return finished.get();
    }
}
