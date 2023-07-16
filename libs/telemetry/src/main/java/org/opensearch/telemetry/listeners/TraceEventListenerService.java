/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.listeners;

import org.opensearch.telemetry.diagnostics.DiagnosticSpan;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.listeners.wrappers.TraceEventsRunnable;
import org.opensearch.telemetry.listeners.wrappers.TracerWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * The TraceEventListenerService manages trace event listeners and provides registration and de-registration functionality.
 * It allows components to register themselves as consumers of trace event listeners and notifies them when a new
 * trace event listener is registered or deregistered.
 */
public class TraceEventListenerService {

    private volatile Map<String, TraceEventListener> traceEventListeners;
    private final List<TraceEventListenerConsumer> traceEventListenerConsumers;
    private final Tracer tracer;

    private volatile boolean diagnosisEnabled;

    /**
     * Constructs a new TraceEventListenerService with the specified tracer.
     *
     * @param tracer the tracer to be associated with the service
     */
    public TraceEventListenerService(Tracer tracer, boolean diagnosisEnabled) {
        this.tracer = tracer;
        traceEventListeners = emptyMap();
        traceEventListenerConsumers = new ArrayList<>();
        this.diagnosisEnabled = diagnosisEnabled;
    }

    /**
     * Registers a trace event listener with the specified name.
     *
     * @param name              the name of the trace event listener
     * @param traceEventListener the trace event listener to be registered
     */
    public void registerTraceEventListener(String name, TraceEventListener traceEventListener) {
        HashMap<String, TraceEventListener> newTraceEventListeners = new HashMap<>(traceEventListeners);
        newTraceEventListeners.put(name, traceEventListener);
        traceEventListeners = unmodifiableMap(newTraceEventListeners);
        for (TraceEventListenerConsumer traceEventListenerConsumer : traceEventListenerConsumers) {
            traceEventListenerConsumer.onTraceEventListenerRegister(name, traceEventListener);
        }
    }

    /**
     * de-registers the trace event listener with the specified name.
     *
     * @param name the name of the trace event listener to be deregistered
     */
    public void deregisterTraceEventListener(String name) {
        HashMap<String, TraceEventListener> newTraceEventListeners = new HashMap<>(traceEventListeners);
        newTraceEventListeners.remove(name);
        traceEventListeners = unmodifiableMap(newTraceEventListeners);
        for (TraceEventListenerConsumer traceEventListenerConsumer : traceEventListenerConsumers) {
            traceEventListenerConsumer.onTraceEventListenerDeregister(name);
        }
    }

    /**
     * Registers a trace event listener consumer.
     *
     * @param traceEventListenerConsumer the trace event listener consumer to be registered
     */
    public void registerTraceEventListenerConsumer(TraceEventListenerConsumer traceEventListenerConsumer) {
        traceEventListenerConsumers.add(traceEventListenerConsumer);
    }

    /**
     * Deregisters a trace event listener consumer.
     *
     * @param traceEventListenerConsumer the trace event listener consumer to be deregistered
     */
    public void deregisterTraceEventListenerConsumer(TraceEventListenerConsumer traceEventListenerConsumer) {
        traceEventListenerConsumers.remove(traceEventListenerConsumer);
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
     * Returns the tracer associated with the TraceEventListenerService.
     *
     * @return the tracer object
     */
    public Tracer getTracer() {
        return tracer;
    }

    public void setDiagnosis(boolean diagnosisEnabled) {
        this.diagnosisEnabled = diagnosisEnabled;
        for (TraceEventListenerConsumer traceEventListenerConsumer : traceEventListenerConsumers) {
            traceEventListenerConsumer.onDiagnosisSettingChange(diagnosisEnabled);
        }
    }

    /**
     * Wraps the given Runnable with trace event listeners registered with {@link TraceEventListenerService}
     *
     * @param runnable the Runnable to wrap
     * @return the wrapped TraceEventsRunnable
     */
    public TraceEventsRunnable wrapRunnable(Runnable runnable) {
        return new TraceEventsRunnable(runnable, this.getTracer(),
            this.getTraceEventListeners(), diagnosisEnabled);
    }

    /**
     * Unwraps the given TraceEventsRunnable to retrieve the original Runnable.
     *
     * @param runnableWrapper the TraceEventsRunnable to unwrap
     * @return the original Runnable
     */
    public Runnable unwrapRunnable(TraceEventsRunnable runnableWrapper) {
        return runnableWrapper.unwrap();
    }

    /**
     * Wraps the given Tracer with trace event listeners.
     *
     * @param tracer the Tracer to wrap
     * @return the wrapped TracerWrapper
     */
    public TracerWrapper wrapTracer(Tracer tracer) {
        TracerWrapper tracerWrapper = new TracerWrapper(tracer, this.getTraceEventListeners(), diagnosisEnabled);
        this.registerTraceEventListenerConsumer(tracerWrapper);
        return tracerWrapper;
    }

    /**
     * Unwraps the given TracerWrapper to retrieve the original Tracer.
     *
     * @param tracerWrapper the TracerWrapper to unwrap
     * @return the original Tracer
     */
    public Tracer unwrapTracer(TracerWrapper tracerWrapper) {
        this.deregisterTraceEventListenerConsumer(tracerWrapper);
        return tracerWrapper.unwrap();
    }

    /**
     * Wraps the given Span with diagnostic capabilities.
     *
     * @param span the Span to wrap
     * @return the wrapped DiagnosticSpan
     */
    public static DiagnosticSpan wrapSpan(Span span) {
        return new DiagnosticSpan(span);
    }
}

