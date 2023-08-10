/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.diagnostics.DiagnosticSpan;
import org.opensearch.telemetry.diagnostics.DiagnosticsEventListener;
import org.opensearch.telemetry.tracing.listeners.TraceEventListener;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * The TraceEventService manages trace event listeners and provides their registration and de-registration functionality.
 *
 * It also provides wrapper utility to wrap {@link Tracer} using {@link #wrapAndSetTracer(Tracer)}, Runnable using {@link #wrapRunnable(Runnable)}
 * and {@link Span} using {@link #wrapWithDiagnosticSpan(Span)}.
 *
 * This is the core service for trace event listeners and must be instantiated at application start.
 * Once the telemetry and tracing is instantiated, this service should be used to wrap Tracer, Span and Runnables. Wrap will not have any effect until
 * trace event listeners are registered using {@link #registerTraceEventListener} and tracing is set as enabled using {@link #setTracingEnabled(boolean)}.
 *
 * The application must ensure that this service is updated with the latest set of TraceEventListener and latest value of {@link #tracingEnabled} and {@link #diagnosisEnabled}
 * are set, or it may produce undesirable results.
 */
public class TraceEventsService {

    private volatile Map<String, TraceEventListener> traceEventListeners;
    private volatile Tracer tracer;

    private volatile boolean tracingEnabled;

    private volatile boolean diagnosisEnabled;

    private static final Logger logger = LogManager.getLogger(TraceEventsService.class);

    /**
     * Constructs a new TraceEventService with the specified tracer.
     *
     */
    public TraceEventsService() {
        traceEventListeners = emptyMap();
        this.tracingEnabled = false;
        this.diagnosisEnabled = false;
    }

    /**
     * Registers a trace event listener with the specified name.
     *
     * @param name              the name of the trace event listener
     * @param traceEventListener the trace event listener to be registered
     */
    public synchronized void registerTraceEventListener(String name, TraceEventListener traceEventListener) {
        HashMap<String, TraceEventListener> newTraceEventListeners = new HashMap<>(traceEventListeners);
        newTraceEventListeners.put(name, traceEventListener);
        traceEventListeners = unmodifiableMap(newTraceEventListeners);
    }

    /**
     * de-registers the trace event listener with the specified name.
     *
     * @param name the name of the trace event listener to be deregistered
     */
    public synchronized void deregisterTraceEventListener(String name) {
        HashMap<String, TraceEventListener> newTraceEventListeners = new HashMap<>(traceEventListeners);
        newTraceEventListeners.remove(name);
        traceEventListeners = unmodifiableMap(newTraceEventListeners);
    }

    /**
     * Returns a map of all the registered trace event listeners.
     *
     * @return a map of trace event listeners, where the keys are the listener names and the values are the listener objects
     */
    public Map<String, TraceEventListener> getTraceEventListeners() {
        return traceEventListeners;
    }

    /**
     * Returns the tracer associated with the TraceEventService.
     *
     * @return the tracer object
     */
    public Tracer getTracer() {
        return tracer;
    }

    /**
     * Set the diagnosis enabled post which any {@link org.opensearch.telemetry.diagnostics.DiagnosticsEventListener} comes into effect.
     * @param diagnosisEnabled true to enable
     */
    public synchronized void setDiagnosisEnabled(boolean diagnosisEnabled) {
        this.diagnosisEnabled = diagnosisEnabled;
    }

    /**
     * true is diagnosis is enabled in the service
     */
    public boolean isDiagnosisEnabled() {
        return diagnosisEnabled;
    }

    /**
     * Set the tracing enable
     * @param tracingEnabled true to enable
     */
    public synchronized void setTracingEnabled(boolean tracingEnabled) {
        this.tracingEnabled = tracingEnabled;
    }

    /**
     * Checks if tracing is enabled.
     * @return true if enabled.
     */
    public boolean isTracingEnabled() {
        return tracingEnabled;
    }

    /**
     * Wraps the given Runnable with trace event listeners registered with {@link TraceEventsService}
     * Note: Runnable should be wrapped using this method only after thread context has been restored so that when
     * {@link TraceEventsRunnable#run()} is called, it has the right thread context with current Span information.
     * @param runnable the Runnable to wrap
     * @return the wrapped TraceEventsRunnable
     */
    public Runnable wrapRunnable(Runnable runnable) {
        if (runnable instanceof TraceEventsRunnable) {
            return runnable;
        }
        if (tracingEnabled) {
            return new TraceEventsRunnable(runnable, this);
        } else {
            return runnable;
        }
    }

    /**
     * Unwraps the given TraceEventsRunnable to retrieve the original Runnable.
     *  Note: Runnable should be unwrapped using this method before context is stashed so that when
     * {@link TraceEventsRunnable} delegate is complete and {@link TraceEventListener#onRunnableComplete(Span, Thread)}  is called,
     * it has the right context with current Span information.
     * @param runnableWrapper the TraceEventsRunnable to unwrap
     * @return the original Runnable
     */
    public Runnable unwrapRunnable(Runnable runnableWrapper) {
        if (runnableWrapper instanceof TraceEventsRunnable) {
            return ((TraceEventsRunnable) runnableWrapper).unwrap();
        } else {
            return runnableWrapper;
        }
    }

    /**
     * Wraps the given Tracer with trace event listeners.
     *
     * @param tracer the Tracer to wrap
     * @return the wrapped TracerWrapper
     */
    public synchronized TracerWrapper wrapAndSetTracer(Tracer tracer) {
        TracerWrapper tracerWrapper = new TracerWrapper(tracer, this);
        this.tracer = tracerWrapper;
        return tracerWrapper;
    }

    /**
     * Unwraps the given TracerWrapper to retrieve the original Tracer.
     *
     * @param tracerWrapper the TracerWrapper to unwrap
     * @return the original Tracer
     */
    public Tracer unwrapTracer(TracerWrapper tracerWrapper) {
        return tracerWrapper.unwrap();
    }

    /**
     * Wraps the given Span with diagnostic capabilities.
     * Ideally, {@link Tracer#startSpan} should be wrapped using this wrapper.
     * @param span the Span to wrap
     * @return the wrapped DiagnosticSpan
     */
    public static DiagnosticSpan wrapWithDiagnosticSpan(Span span) {
        return new DiagnosticSpan(span);
    }

    /**
     * Invokes the provided event for all TraceEventListener registered with the service
     * @param span associated span
     * @param listenerMethod the listener method to be invoked
     */
    public void executeListeners(Span span, Consumer<TraceEventListener> listenerMethod) {
        if (span == null || traceEventListeners == null) {
            return;
        }
        for (TraceEventListener traceEventListener : traceEventListeners.values()) {
            try {
                if (!traceEventListener.isEnabled(span)) {
                    continue;
                }
                if (traceEventListener instanceof DiagnosticsEventListener && !diagnosisEnabled) {
                    continue;
                }
                listenerMethod.accept(traceEventListener);
            } catch (Exception e) {
                // failing trace event listener shouldn't impact the application
                logger.debug("Error for TraceEventListener: {} {}", traceEventListener.getClass().getName(), e);
            }
        }
    }
}
