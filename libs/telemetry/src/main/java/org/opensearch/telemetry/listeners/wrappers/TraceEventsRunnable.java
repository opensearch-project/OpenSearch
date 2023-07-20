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
    private static final Logger logger = LogManager.getLogger(TraceEventsRunnable.class);

    private final Runnable runnable;
    private final Tracer tracer;
    private final Map<String, TraceEventListener> traceEventListeners;

    private final boolean diagnosisEnabled;


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
            Span span = validateAndGetCurrentSpan();
            if (span != null) {
                for (TraceEventListener traceEventListener : traceEventListeners.values()) {
                    if (traceEventListener.isEnabled(span)) {
                        try {
                            traceEventListener.onRunnableStart(span, Thread.currentThread());
                        } catch (Exception e) {
                            // failing diagnosis shouldn't impact the application
                            logger.debug("Error in onRunnableStart for TraceEventListener: {}",
                                traceEventListener.getClass().getName(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Error in onRunnableStart", e);
        } finally {
            runnable.run();
        }
        try {
            Span span = validateAndGetCurrentSpan();
            if (span != null) {
                for (TraceEventListener traceEventListener : traceEventListeners.values()) {
                    if (traceEventListener.isEnabled(span)) {
                        try {
                            traceEventListener.onRunnableComplete(span, Thread.currentThread());
                        } catch (Exception e) {
                            // failing diagnosis shouldn't impact the application
                            logger.debug("Error in onRunnableComplete for TraceEventListener: {}",
                                traceEventListener.getClass().getName(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Error in onRunnableStart", e);
        }
    }

    private Span validateAndGetCurrentSpan() {
        if (traceEventListeners == null || traceEventListeners.isEmpty()) {
            return null;
        }
        Span span = tracer.getCurrentSpan();
        if (span == null) {
            return null;
        }
        if (span instanceof DiagnosticSpan && !diagnosisEnabled) {
            return null;
        }
        if (span.hasEnded()) {
            logger.debug("TraceEventsRunnable is invoked post span completion", new Throwable());
            return null;
        }
        return span;
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

