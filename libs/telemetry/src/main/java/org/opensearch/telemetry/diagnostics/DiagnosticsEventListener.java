/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;

import org.opensearch.telemetry.diagnostics.metrics.MetricEmitter;
import org.opensearch.telemetry.listeners.TraceEventListener;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;

/**
 * When a {@link Tracer} is wrapped with {@link org.opensearch.telemetry.listeners.TraceEventListenerService#wrapTracer(Tracer)}
 * and all Runnable associated with a trace are wrapped with {@link org.opensearch.telemetry.listeners.TraceEventListenerService#wrapRunnable(Runnable)},
 * this class records the resource consumption of the complete trace using provided {@link ThreadResourceRecorder} and emits corresponding metrics using
 * {@link MetricEmitter}.
 * The span created by {@link org.opensearch.telemetry.tracing.TracingTelemetry#createSpan(String, Span)} must be wrapped with {@link DiagnosticSpan}
 * using {@link org.opensearch.telemetry.listeners.TraceEventListenerService#wrapSpan(Span)}
 */
public class DiagnosticsEventListener implements TraceEventListener {

    private final ThreadResourceRecorder<?> threadResourceRecorder;
    private final MetricEmitter metricEmitter;

    /**
     * Constructs a new DiagnosticsTraceEventListener with the specified tracer, thread resource recorder,
     * and metric emitter.
     *
     * @param threadResourceRecorder the thread resource recorder responsible for recording resource usage
     * @param metricEmitter          the metric emitter used for emitting diagnostic metrics
     */
    public DiagnosticsEventListener(ThreadResourceRecorder<?> threadResourceRecorder,
                                    MetricEmitter metricEmitter) {
        this.threadResourceRecorder = threadResourceRecorder;
        this.metricEmitter = metricEmitter;
    }

    /**
     * Called when a span is started. It starts recording resources for the associated thread.
     *
     * @param span the current span
     * @param t    the thread which started the span
     */
    @Override
    public void onSpanStart(Span span, Thread t) {
        ensureDiagnosticSpan(span);
        threadResourceRecorder.startRecording((DiagnosticSpan) span, t);
    }

    /**
     * Called when a span is completed for a thread. It emits the metric reported by
     * {@link ThreadResourceRecorder#endRecording}
     *
     * @param span the current span
     * @param t    the thread which completed the span
     */
    @Override
    public void onSpanComplete(Span span, Thread t) {
        ensureDiagnosticSpan(span);
        metricEmitter.emitMetric(threadResourceRecorder.endRecording((DiagnosticSpan) span, t, true));
    }

    /**
     * Called when a runnable is started within a span.
     * It starts recording resource usage for the thread.
     *
     * @param span the current span
     * @param t    the thread for which the runnable is started
     */
    @Override
    public void onRunnableStart(Span span, Thread t) {
        ensureDiagnosticSpan(span);
        threadResourceRecorder.startRecording((DiagnosticSpan) span, t);
    }

    /**
     * Called when a runnable is finished by a thread within a span. It emits the metric reported by
     * {@link ThreadResourceRecorder#endRecording}
     *
     * @param span the current span
     * @param t    the thread for which the runnable is finished
     */
    @Override
    public void onRunnableComplete(Span span, Thread t) {
        ensureDiagnosticSpan(span);
        metricEmitter.emitMetric(threadResourceRecorder.endRecording((DiagnosticSpan) span, t, false));
    }

    /**
     * Check if TraceEventListener is enabled
     * TODO - replace with operation based logic
     * @param span the current span
     * @return
     */
    @Override
    public boolean isEnabled(Span span) {
        return true;
    }

    private void ensureDiagnosticSpan(Span span) {
        if (!(span instanceof DiagnosticSpan)) {
            throw new IllegalArgumentException("Expected DiagnosticSpan");
        }
    }
}
