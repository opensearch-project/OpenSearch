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
import org.opensearch.telemetry.metrics.MetricPoint;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.listeners.TraceEventListener;

/**
 * Records the {@link MetricPoint} for a {@link ThreadResourceObserver}.
 * It abstracts out the diff logic between gauge {@link MetricPoint} reported by {@link ThreadResourceRecorder#startRecording} and
 * {@link ThreadResourceRecorder#endRecording} when endRecording is invoked.
 * Implementation of this class should be thread-safe.
 * It maintains the state between {@link #startRecording(DiagnosticSpan, Thread, boolean)} and {@link #endRecording(DiagnosticSpan, Thread, boolean)}
 * using {@link DiagnosticSpan#putMetric(String, MetricPoint)} assuming {@link Span} context propagation is taken care by tracing framework.
 * @param <T> the type of ThreadResourceObserver
 */
public abstract class ThreadResourceRecorder<T extends ThreadResourceObserver> {

    private static final Logger logger = LogManager.getLogger(ThreadResourceRecorder.class);

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
     * @param span           the DiagnosticSpan to record the metric for
     * @param t              the thread for which to record the metric
     * @param startSpanEvent true if it is invoked as a result of start span trace event
     */
    public void startRecording(DiagnosticSpan span, Thread t, boolean startSpanEvent) {
        MetricPoint observation = observer.observe(t);
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
     * @param endSpan a flag indicating whether its invoked as a result of {@link TraceEventListener#onSpanComplete(Span, Thread)}
     * @return the computed diff metric between the start and end metrics
     */
    public MetricPoint endRecording(DiagnosticSpan span, Thread t, boolean endSpan) {
        MetricPoint startMetric = span.removeMetric(String.valueOf(t.getId()));
        MetricPoint endMetric = observer.observe(t);
        if (startMetric == null) {
            logger.debug("Start metric is missing for span:{} {}", span, new Throwable());
            // this scenario should never happen. We don't throw an exception instead return zero usage
            return computeDiff(endMetric, endMetric);
        }
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
    protected abstract MetricPoint computeDiff(MetricPoint startMetric, MetricPoint endMetric);
}
