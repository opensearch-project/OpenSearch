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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.http.HttpChannel;
import org.opensearch.rest.RestChannel;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ActionListener wrapper that ensures REST channels are properly untracked from
 * RestCancellableNodeClient when streaming requests reach terminal states.
 *
 * This wrapper preserves streaming semantics by only finishing tracking on
 * final completion or failure, not on partial responses.
 *
 * @param <T> the response type
 */
public class StreamingTerminalChannelReleasingListener<T extends SearchResponse> implements ActionListener<T> {
    private static final Logger logger = LogManager.getLogger(StreamingTerminalChannelReleasingListener.class);

    private final RestCancellableNodeClient client;
    private final RestChannel channel;
    private final HttpChannel httpChannel;
    private final ActionListener<T> delegate;
    private final AtomicBoolean finished;

    public StreamingTerminalChannelReleasingListener(RestCancellableNodeClient client, RestChannel channel, ActionListener<T> delegate) {
        this.client = client;
        this.channel = channel;
        this.httpChannel = channel != null ? channel.request().getHttpChannel() : null;
        this.delegate = delegate;
        this.finished = new AtomicBoolean(false);
    }

    @Override
    public void onResponse(T response) {
        // On the REST path, we wrap a plain ActionListener, so all streaming partials
        // are already coalesced by StreamSearchTransportService before the listener fires.
        // There's no isLast signal to inspect - this is inherently the terminal response.
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
     * Provides access to the underlying channel for external scenarios.
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
