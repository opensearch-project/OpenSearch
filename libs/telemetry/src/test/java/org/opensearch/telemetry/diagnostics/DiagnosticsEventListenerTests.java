/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;

import org.mockito.ArgumentCaptor;
import org.opensearch.telemetry.metrics.Measurement;
import org.opensearch.telemetry.metrics.MetricEmitter;
import org.opensearch.telemetry.metrics.MetricPoint;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.opensearch.telemetry.diagnostics.DiagnosticsEventListener.ELAPSED_TIME;

public class DiagnosticsEventListenerTests extends OpenSearchTestCase {

    private ThreadResourceRecorder<?> threadResourceRecorder;
    private MetricEmitter metricEmitter;
    private Span span;
    private DiagnosticSpan diagnosticSpan;
    private DiagnosticsEventListener diagnosticsEventListener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadResourceRecorder = Mockito.mock(ThreadResourceRecorder.class);
        metricEmitter = Mockito.mock(MetricEmitter.class);
        span = Mockito.mock(Span.class);
        diagnosticSpan = Mockito.mock(DiagnosticSpan.class);
        diagnosticsEventListener = new DiagnosticsEventListener(threadResourceRecorder, metricEmitter);
    }

    public void testOnSpanStart() {
        Thread t = Thread.currentThread();
        diagnosticsEventListener.onSpanStart(diagnosticSpan, t);
        // Verify expected interactions
        Mockito.verify(threadResourceRecorder).startRecording(Mockito.eq(diagnosticSpan), Mockito.eq(t), Mockito.eq(true));
        Mockito.verify(diagnosticSpan).putMetric(Mockito.eq(DiagnosticsEventListener.START_SPAN_TIME), any(MetricPoint.class));
    }

    public void testOnSpanComplete() {
        Thread t = Thread.currentThread();
        MetricPoint diffMetric = new MetricPoint(Collections.emptyMap(), null, System.currentTimeMillis());
        MetricPoint startMetric = new MetricPoint(Collections.emptyMap(), null, System.currentTimeMillis());
        Mockito.when(threadResourceRecorder.endRecording(any(DiagnosticSpan.class), Mockito.eq(t), Mockito.eq(true)))
            .thenReturn(diffMetric);
        Mockito.when(diagnosticSpan.removeMetric(Mockito.anyString())).thenReturn(startMetric);
        ArgumentCaptor<MetricPoint> metricCaptor = ArgumentCaptor.forClass(MetricPoint.class);
        diagnosticsEventListener.onSpanComplete(diagnosticSpan, t);
        Mockito.verify(metricEmitter).emitMetric(metricCaptor.capture());

        // Check if diffMetric contains "elapsed_time" measurement
        MetricPoint emittedMetric = metricCaptor.getValue();
        Measurement<Number> elapsedTimeMeasurement = emittedMetric.getMeasurement(ELAPSED_TIME);
        assertNotNull(elapsedTimeMeasurement);
    }

    public void testOnRunnableStart() {
        Thread t = Thread.currentThread();
        diagnosticsEventListener.onRunnableStart(diagnosticSpan, t);
        Mockito.verify(threadResourceRecorder).startRecording(Mockito.eq(diagnosticSpan), Mockito.eq(t), Mockito.eq(false));
    }

    public void testOnRunnableComplete() {
        Thread t = Thread.currentThread();
        MetricPoint diffMetric = new MetricPoint(Collections.emptyMap(), null, System.currentTimeMillis());
        Mockito.when(threadResourceRecorder.endRecording(any(DiagnosticSpan.class), Mockito.eq(t), Mockito.eq(false)))
            .thenReturn(diffMetric);

        diagnosticsEventListener.onRunnableComplete(diagnosticSpan, t);

        Mockito.verify(metricEmitter).emitMetric(Mockito.eq(diffMetric));
    }

    public void testIsEnabled() {
        boolean isEnabled = diagnosticsEventListener.isEnabled(diagnosticSpan);
        assertTrue(isEnabled);

        isEnabled = diagnosticsEventListener.isEnabled(span);
        assertFalse(isEnabled);
    }
}
