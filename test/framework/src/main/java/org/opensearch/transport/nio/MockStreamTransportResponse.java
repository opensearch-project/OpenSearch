/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock implementation of StreamTransportResponse for testing streaming transport functionality.
 *
 * @opensearch.internal
 */
class MockStreamTransportResponse<T extends TransportResponse> implements StreamTransportResponse<T> {
    private static final Logger logger = LogManager.getLogger(MockStreamTransportResponse.class);

    private final List<T> responses;
    private final AtomicInteger currentIndex = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile boolean cancelled = false;

    // Constructor for multiple responses (new batching support)
    public MockStreamTransportResponse(List<T> responses) {
        this.responses = responses != null ? responses : List.of();
    }

    @Override
    public T nextResponse() {
        if (cancelled) {
            throw new IllegalStateException("Stream has been cancelled");
        }

        if (closed.get()) {
            throw new IllegalStateException("Stream has been closed");
        }

        // Return the next response from the list, or null if exhausted
        int index = currentIndex.getAndIncrement();
        if (index < responses.size()) {
            T response = responses.get(index);
            logger.debug("Returning mock streaming response {}/{}: {}", index + 1, responses.size(), response.getClass().getSimpleName());
            return response;
        } else {
            logger.debug("Mock stream exhausted, returning null (requested index {}, total responses: {})", index, responses.size());
            return null;
        }
    }

    @Override
    public void cancel(String reason, Throwable cause) {
        if (cancelled) {
            logger.warn("Stream already cancelled, ignoring cancel request: {}", reason);
            return;
        }

        cancelled = true;
        logger.debug("Mock stream cancelled: {} - {}", reason, cause != null ? cause.getMessage() : "no cause");
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.debug("Mock stream closed");
        } else {
            logger.warn("Stream already closed, ignoring close request");
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean isCancelled() {
        return cancelled;
    }
}
