/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.arrow.flight.stats.FlightCallTracker;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;

/**
 * A response handler wrapper that tracks metrics for Flight calls.
 * This handler wraps another handler and adds metrics tracking.
 */
class MetricsTrackingResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {
    private final TransportResponseHandler<T> delegate;
    private final FlightCallTracker callTracker;

    /**
     * Creates a new metrics tracking response handler.
     *
     * @param delegate the delegate handler
     * @param callTracker the call tracker for metrics
     */
    MetricsTrackingResponseHandler(TransportResponseHandler<T> delegate, FlightCallTracker callTracker) {
        this.delegate = delegate;
        this.callTracker = callTracker;
    }

    @Override
    public void handleResponse(T response) {
        delegate.handleResponse(response);
    }

    @Override
    public void handleException(TransportException exp) {
        try {
            if (exp instanceof StreamException se) {
                callTracker.recordCallEnd(se.getErrorCode().name());
            } else {
                callTracker.recordCallEnd(StreamErrorCode.INTERNAL.name());
            }
        } finally {
            delegate.handleException(exp);
        }
    }

    @Override
    public void handleRejection(Exception exp) {
        try {
            callTracker.recordCallEnd(StreamErrorCode.UNAVAILABLE.name());
        } finally {
            delegate.handleRejection(exp);
        }
    }

    @Override
    public void handleStreamResponse(StreamTransportResponse<T> response) {

        FlightTransportResponse<T> flightResponse = (FlightTransportResponse<T>) response;
        StreamTransportResponse<T> wrappedResponse = new MetricsTrackingStreamResponse<>(flightResponse, callTracker);

        try {
            delegate.handleStreamResponse(wrappedResponse);
            callTracker.recordCallEnd(StreamErrorCode.OK.name());
        } catch (Exception e) {
            if (e instanceof StreamException se) {
                callTracker.recordCallEnd(se.getErrorCode().name());
            } else {
                callTracker.recordCallEnd(StreamErrorCode.INTERNAL.name());
            }
            throw e;
        }
    }

    @Override
    public T read(StreamInput streamInput) throws IOException {
        return delegate.read(streamInput);
    }

    @Override
    public String executor() {
        return delegate.executor();
    }

    /**
     * A stream response wrapper that tracks metrics for batches.
     */
    private static class MetricsTrackingStreamResponse<T extends TransportResponse> implements StreamTransportResponse<T> {
        private final FlightTransportResponse<T> delegate;
        private final FlightCallTracker callTracker;

        /**
         * Creates a new metrics tracking stream response.
         *
         * @param delegate the delegate stream response
         * @param callTracker the call tracker for metrics
         */
        MetricsTrackingStreamResponse(FlightTransportResponse<T> delegate, FlightCallTracker callTracker) {
            this.delegate = delegate;
            this.callTracker = callTracker;
        }

        @Override
        public T nextResponse() {
            long startTime = System.nanoTime();
            callTracker.recordBatchRequested();
            T response = delegate.nextResponse();
            if (response != null) {
                long batchSize = delegate.getCurrentBatchSize();
                long processingTimeNanos = System.nanoTime() - startTime;
                callTracker.recordBatchReceived(batchSize, processingTimeNanos);
            }
            return response;
        }

        @Override
        public void cancel(String reason, Throwable cause) {
            try {
                callTracker.recordCallEnd(StreamErrorCode.CANCELLED.name());
            } finally {
                delegate.cancel(reason, cause);
            }
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
