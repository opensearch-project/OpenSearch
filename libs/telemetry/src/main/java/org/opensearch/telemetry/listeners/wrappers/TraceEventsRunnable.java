/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.listeners.wrappers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.diagnostics.DiagnosticSpan;
import org.opensearch.telemetry.listeners.TraceEventListener;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;

import java.util.Map;

/**
 * Runnable implementation that wraps another Runnable and adds trace event listener functionality.
 */
public class TraceEventsRunnable implements Runnable {
    private final Runnable runnable;
    private final Tracer tracer;
    private final Map<String, TraceEventListener> traceEventListeners;

    private final boolean diagnosisEnabled;

    private static final Logger logger = LogManager.getLogger(TraceEventsRunnable.class);

    /**
     * Constructs a TraceEventsRunnable with the provided delegate Runnable.
     * Tracer is used to get current span information and
     * {@link org.opensearch.telemetry.listeners.RunnableEventListener} events are invoked for all traceEventListeners.
     * @param delegate            the underlying Runnable to be executed
     * @param tracer              the Tracer instance for tracing operations
     * @param traceEventListeners the map of TraceEventListeners
     */
    public TraceEventsRunnable(Runnable delegate, Tracer tracer, Map<String, TraceEventListener> traceEventListeners,
                               boolean diagnosisEnabled) {
        this.runnable = delegate;
        this.tracer = tracer;
        this.traceEventListeners = traceEventListeners;
        this.diagnosisEnabled = diagnosisEnabled;
    }

    @Override
    public void run() {
        try {
            for (TraceEventListener traceEventListener : traceEventListeners.values()) {
                Span span = tracer.getCurrentSpan();
                if (traceEventListener instanceof DiagnosticSpan && !diagnosisEnabled) {
                    continue;
                }
                if (span != null && traceEventListener.isEnabled(span)) {
                    try {
                        traceEventListener.onRunnableStart(span, Thread.currentThread());
                    } catch (Exception e) {
                        // failing diagnosis shouldn't impact the application
                        logger.warn("Error in onRunnableStart for TraceEventListener: " + traceEventListener.getClass().getName());
                    }
                }
            }
            runnable.run();
        } finally {
            for (TraceEventListener traceEventListener : traceEventListeners.values()) {
                Span span = tracer.getCurrentSpan();
                if (traceEventListener instanceof DiagnosticSpan && !diagnosisEnabled) {
                    continue;
                }
                if (span != null && traceEventListener.isEnabled(span)) {
                    try {
                        traceEventListener.onRunnableComplete(span, Thread.currentThread());
                    } catch (Exception e) {
                        // failing diagnosis shouldn't impact the application
                        logger.warn("Error in onRunnableComplete for TraceEventListener: " + traceEventListener.getClass().getName());
                    }
                }
            }
        }
    }

    /**
     * Unwraps and returns the underlying Runnable instance.
     *
     * @return the underlying Runnable instance
     */
    public Runnable unwrap() {
        return runnable;
    }
}

