/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;

/**
 * A {@link TransportResponseHandler} wrapper that measures the time spent deserializing
 * the response from a {@link StreamInput}. The measured duration is recorded as
 * {@code response_deserialization} in the {@link SearchLatencyBreakdown} using
 * max-aggregation across all shard responses.
 * <p>
 * This handler delegates all operations to the wrapped handler, only adding timing
 * instrumentation around the {@link #read(StreamInput)} call.
 *
 * @opensearch.internal
 */
final class TimedResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

    private final TransportResponseHandler<T> delegate;
    private final SearchLatencyBreakdown breakdown;

    /**
     * Creates a new timed response handler wrapping the given delegate.
     *
     * @param delegate  the original response handler to delegate to
     * @param breakdown the latency breakdown to record deserialization time into
     */
    TimedResponseHandler(TransportResponseHandler<T> delegate, SearchLatencyBreakdown breakdown) {
        this.delegate = delegate;
        this.breakdown = breakdown;
    }

    @Override
    public T read(StreamInput in) throws IOException {
        long deserStart = System.nanoTime();
        T response = delegate.read(in);
        long deserDuration = Math.max(0, System.nanoTime() - deserStart);
        breakdown.recordResponseDeserialization(deserDuration);
        return response;
    }

    @Override
    public void handleResponse(T response) {
        delegate.handleResponse(response);
    }

    @Override
    public void handleException(TransportException exp) {
        delegate.handleException(exp);
    }

    @Override
    public String executor() {
        return delegate.executor();
    }

    @Override
    public void handleRejection(Exception exp) {
        delegate.handleRejection(exp);
    }

    @Override
    public boolean skipsDeserialization() {
        return delegate.skipsDeserialization();
    }
}
