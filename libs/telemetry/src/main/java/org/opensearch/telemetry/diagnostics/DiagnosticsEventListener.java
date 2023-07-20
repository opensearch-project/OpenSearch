/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.diagnostics.metrics.DiagnosticMetric;
import org.opensearch.telemetry.diagnostics.metrics.MetricEmitter;
import org.opensearch.telemetry.listeners.TraceEventListener;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.Tracer;

/**
 * When a {@link Tracer} is wrapped with {@link org.opensearch.telemetry.listeners.TraceEventListenerService#wrapAndSetTracer(Tracer)}
 * and all Runnable associated with a trace are wrapped with {@link org.opensearch.telemetry.listeners.TraceEventListenerService#wrapRunnable(Runnable)},
 * this class records the resource consumption of the complete trace using provided {@link ThreadResourceRecorder} and emits corresponding metrics using
 * {@link MetricEmitter}.
 * The span created by {@link org.opensearch.telemetry.tracing.TracingTelemetry#createSpan(String, Span)} must be wrapped with {@link DiagnosticSpan}
 * using {@link org.opensearch.telemetry.listeners.TraceEventListenerService#wrapSpan(Span)}
 */
public class DiagnosticsEventListener implements TraceEventListener {
    private static final Logger logger = LogManager.getLogger(DiagnosticsEventListener.class);
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
        if(!ensureDiagnosticSpan(span)) {
            return;
        }
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
        if (!ensureDiagnosticSpan(span)) {
            return;
        }
        DiagnosticMetric diffMetric = threadResourceRecorder.endRecording((DiagnosticSpan) span, t, true);
        DiagnosticMetric endMetric = new DiagnosticMetric(diffMetric.getMeasurements(),
            ((DiagnosticSpan) span).getAttributes(), diffMetric.getObservationTime());
        metricEmitter.emitMetric(endMetric);
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
        if (!ensureDiagnosticSpan(span)) {
            return;
        }
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
        if (!ensureDiagnosticSpan(span)) {
            return;
        }
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

    private boolean ensureDiagnosticSpan(Span span) {
        if(span instanceof DiagnosticSpan) {
            return true;
        } else {
            logger.debug("Non diagnostic span detected while processing DiagnosticEventListener for span  {}",
                span, new Throwable());
            return false;
        }
    }
}
