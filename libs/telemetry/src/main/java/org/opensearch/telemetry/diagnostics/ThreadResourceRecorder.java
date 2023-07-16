/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;


import org.opensearch.telemetry.diagnostics.metrics.DiagnosticMetric;
import org.opensearch.telemetry.tracing.Span;

/**
 * Records the {@link DiagnosticMetric} for a {@link ThreadResourceObserver}.
 * It abstracts out the diff logic between gauge {@link DiagnosticMetric} reported by {@link ThreadResourceRecorder#startRecording} and
 * {@link ThreadResourceRecorder#endRecording} when endRecording is invoked.
 * Implementation of this class should be thread-safe.
 *
 * @param <T> the type of ThreadResourceObserver
 */
public abstract class ThreadResourceRecorder<T extends ThreadResourceObserver> {
    private final T observer;

    /**
     * Constructs a ThreadResourceRecorder with the specified ThreadResourceObserver.
     *
     * @param observer the ThreadResourceObserver responsible for observing the thread's resource
     */
    public ThreadResourceRecorder(T observer) {
        this.observer = observer;
    }

    /**
     * Starts recording the metric for the given Span and thread.
     * The observation is obtained from the associated ThreadResourceObserver.
     *
     * @param span the DiagnosticSpan to record the metric for
     * @param t    the thread for which to record the metric
     */
    public void startRecording(DiagnosticSpan span, Thread t) {
        DiagnosticMetric observation = observer.observe(t);
        span.putMetric(String.valueOf(t.getId()), observation);
    }

    /**
     * Ends recording the metric for the given Span and thread.
     * The start metric is retrieved from the DiagnosticSpan and the end metric is obtained
     * from the associated ThreadResourceObserver.
     * The computed diff metric is returned.
     *
     * @param span    the DiagnosticSpan to end the recording for
     * @param t       the thread for which to end the recording
     * @param endSpan a flag indicating whether its invoked as a result of {@link org.opensearch.telemetry.listeners.TraceEventListener#onSpanComplete(Span, Thread)}
     * @return the computed diff metric between the start and end metrics
     * @throws IllegalStateException if the start metric is missing for the specified span and thread
     */
    public DiagnosticMetric endRecording(DiagnosticSpan span, Thread t, boolean endSpan) {
        DiagnosticMetric startMetric = span.removeMetric(String.valueOf(t.getId()));
        if (startMetric == null) {
            // this can happen if
            throw new IllegalStateException("Start metric is missing for span: " + span.getSpanId());
        }
        DiagnosticMetric endMetric = observer.observe(t);
        return computeDiff(startMetric, endMetric);
    }

    /**
     * Computes the diff metric between the start and end metrics.
     * Subclasses should implement this method to define the specific diff logic.
     *
     * @param startMetric the start metric
     * @param endMetric   the end metric
     * @return the computed diff metric
     */
    protected abstract DiagnosticMetric computeDiff(DiagnosticMetric startMetric, DiagnosticMetric endMetric);
}

