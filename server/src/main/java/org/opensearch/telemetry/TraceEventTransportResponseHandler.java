/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.telemetry.diagnostics.DiagnosticSpan;
import org.opensearch.telemetry.listeners.TraceEventListener;
import org.opensearch.telemetry.listeners.TraceEventListenerService;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;

/**
 * Used for invoking traceEventListener.onRunnableStart() and it is supposed to called after restoring context from
 * {@link org.opensearch.transport.TransportService.ContextRestoreResponseHandler}
 */
public final class TraceEventTransportResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {
    private static final Logger logger = LogManager.getLogger(TraceEventTransportResponseHandler.class);

    private final TransportResponseHandler<T> delegate;

    private final TraceEventListenerService traceEventListenerService;

    public TraceEventTransportResponseHandler(TransportResponseHandler<T> delegate,
                                              TraceEventListenerService traceEventListenerService) {
        this.delegate = delegate;
        this.traceEventListenerService = traceEventListenerService;
    }

    @Override
    public T read(StreamInput in) throws IOException {
        return delegate.read(in);
    }

    @Override
    public void handleResponse(T response) {
        try {
            Span span = traceEventListenerService.getTracer().getCurrentSpan();
            if (validatePrerequisites(span)) {
                for (TraceEventListener traceEventListener : traceEventListenerService.getTraceEventListeners().values()) {
                    if (traceEventListener.isEnabled(span)) {
                        try {
                            traceEventListener.onRunnableStart(span, Thread.currentThread());
                        } catch (Exception e) {
                            // failing diagnosis shouldn't impact the application
                            logger.warn("Error in TraceEventTransportResponseHandler handleResponse for TraceEventListener: " + traceEventListener.getClass().getName());
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Error in TraceEventTransportResponseHandler handleResponse: ", e);
        } finally {
             delegate.handleResponse(response);
        }
    }

    @Override
    public void handleException(TransportException exp) {
        delegate.handleException(exp);
    }

    @Override
    public String executor() {
        return delegate.executor();
    }

    private boolean validatePrerequisites(Span span) {
        if (traceEventListenerService.getTraceEventListeners() == null ||
            traceEventListenerService.getTraceEventListeners().isEmpty()) {
            return false;
        }
        if (span == null) {
            return false;
        }
        if (span instanceof DiagnosticSpan && !traceEventListenerService.isDiagnosisEnabled()) {
            return false;
        }
        if (span.hasEnded()) {
            logger.debug("TraceEventTransportResponseHandler is invoked post span completion",
                new Throwable());
            return false;
        }
        return true;
    }
}
