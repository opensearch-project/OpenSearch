/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.listeners.wrappers;

import org.opensearch.telemetry.diagnostics.DiagnosticSpan;
import org.opensearch.telemetry.listeners.TraceEventListener;
import org.opensearch.telemetry.listeners.TraceEventListenerConsumer;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class invokes all events associated with {@link org.opensearch.telemetry.listeners.SpanEventListener}
 */
public class TracerWrapper implements Tracer, TraceEventListenerConsumer {
    private final Tracer tracer;
    private final Map<String, TraceEventListener> traceEventListeners;

    private volatile boolean diagnosisEnabled;

    /**
     * Constructs a TracerWrapper with the provided Tracer and TraceEventListener map.
     *
     * @param delegate            the underlying Tracer implementation
     * @param traceEventListeners the map of TraceEventListeners
     */
    public TracerWrapper(Tracer delegate, Map<String, TraceEventListener> traceEventListeners, boolean diagnosisEnabled) {
        assert delegate != null;
        this.tracer = delegate;
        if (traceEventListeners != null) {
            this.traceEventListeners = new HashMap<>(traceEventListeners);
        } else {
            this.traceEventListeners = new HashMap<>();
        }
        this.diagnosisEnabled = diagnosisEnabled;
    }

    @Override
    public SpanScope startSpan(String spanName) {
        SpanScope scope = tracer.startSpan(spanName);
        Span span = tracer.getCurrentSpan();
        for (TraceEventListener traceEventListener : traceEventListeners.values()) {
            if (traceEventListener instanceof DiagnosticSpan && !diagnosisEnabled) {
                continue;
            }
            if (traceEventListener.isEnabled(span)) {
                traceEventListener.onSpanStart(span, Thread.currentThread());
            }
        }
        return new SpanScopeWrapper(span, scope, new ArrayList<>(traceEventListeners.values()), diagnosisEnabled);
    }

    @Override
    public Span getCurrentSpan() {
        return tracer.getCurrentSpan();
    }

    @Override
    public void close() throws IOException {
        tracer.close();
    }

    @Override
    public void onTraceEventListenerRegister(String name, TraceEventListener traceEventListener) {
        this.traceEventListeners.put(name, traceEventListener);
    }

    @Override
    public void onTraceEventListenerDeregister(String name) {
        this.traceEventListeners.remove(name);
    }

    @Override
    public void onDiagnosisSettingChange(boolean diagnosisEnabled) {
        this.diagnosisEnabled = diagnosisEnabled;
    }

    /**
     * Unwraps and returns the underlying Tracer instance.
     *
     * @return the underlying Tracer instance
     */
    public Tracer unwrap() {
        return tracer;
    }

    private static class SpanScopeWrapper implements SpanScope {
        private final SpanScope scope;
        private final Span span;
        private final List<TraceEventListener> traceEventListeners;

        private final boolean diagnosisEnabled;

        SpanScopeWrapper(Span span, SpanScope delegate, List<TraceEventListener> traceEventListeners,
                         boolean diagnosisEnabled) {
            this.span = span;
            this.scope = delegate;
            this.traceEventListeners = traceEventListeners;
            this.diagnosisEnabled = diagnosisEnabled;
        }

        @Override
        public void addSpanAttribute(String key, String value) {
            scope.addSpanAttribute(key, value);
        }

        @Override
        public void addSpanAttribute(String key, long value) {
            scope.addSpanAttribute(key, value);
        }

        @Override
        public void addSpanAttribute(String key, double value) {
            scope.addSpanAttribute(key, value);
        }

        @Override
        public void addSpanAttribute(String key, boolean value) {
            scope.addSpanAttribute(key, value);
        }

        @Override
        public void addSpanEvent(String event) {
            scope.addSpanEvent(event);
        }

        @Override
        public void setError(Exception exception) {
            scope.setError(exception);
        }

        @Override
        public void close() {
            for (TraceEventListener traceEventListener : traceEventListeners) {
                if (traceEventListener instanceof DiagnosticSpan && !diagnosisEnabled) {
                    continue;
                }
                if (traceEventListener.isEnabled(span)) {
                    traceEventListener.onSpanComplete(span, Thread.currentThread());
                }
            }
            scope.close();
        }
    }
}

