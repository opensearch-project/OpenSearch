/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class DefaultTracerTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    private OpenTelemetry openTelemetry;

    private TracerSettings tracerSettings;

    private ArgumentCaptor<OSSpan> captor;

    private SpanBuilder mockSpanBuilder;

    private io.opentelemetry.api.trace.Span mockOtelSpan;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        setupMock();
        threadPool = new TestThreadPool("test");
    }

    @After
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testStartAndEndSpans() {
        DefaultTracer tracer = new DefaultTracer(mock(OpenTelemetry.class), threadPool, mock(TracerSettings.class));

        tracer.startSpan("foo", Level.INFO);
        assertEquals("foo", tracer.getCurrentSpan().getSpanName());
        tracer.endSpan();
        assertEquals("RootSpan", tracer.getCurrentSpan().getSpanName());
    }

    public void testStartSpanAndEndWithNoopSpans() {
        DefaultTracer tracer = spy(new DefaultTracer(openTelemetry, threadPool, tracerSettings));

        tracer.startSpan("span1", Level.ROOT);
        tracer.startSpan("span2", Level.TERSE);
        tracer.startSpan("span3", Level.INFO);
        tracer.startSpan("noop-span-1", Level.DEBUG);
        tracer.startSpan("noop-span-2", Level.TRACE);
        tracer.startSpan("span4", Level.INFO);

        verify(tracer, times(4)).createOtelSpan(any(), captor.capture());
        verify(mockSpanBuilder, times(4)).startSpan();
        assertEquals("span4", tracer.getCurrentSpan().getSpanName());
        OSSpan value = captor.getValue();
        assertEquals("span3", value.getSpanName());
        tracer.endSpan();
        assertEquals("noop-span-2", tracer.getCurrentSpan().getSpanName());
        tracer.endSpan();
        assertEquals("noop-span-1", tracer.getCurrentSpan().getSpanName());
        tracer.endSpan();
        assertEquals("span3", tracer.getCurrentSpan().getSpanName());
        tracer.endSpan();
        assertEquals("span2", tracer.getCurrentSpan().getSpanName());
        tracer.endSpan();
        assertEquals("span1", tracer.getCurrentSpan().getSpanName());
        tracer.endSpan();
        assertEquals("RootSpan", tracer.getCurrentSpan().getSpanName());
        verify(mockOtelSpan, times(4)).end();
    }

    public void testAddEvent() {
        ArgumentCaptor<String> stringCaptorValues = ArgumentCaptor.forClass(String.class);

        DefaultTracer tracer = new DefaultTracer(openTelemetry, threadPool, tracerSettings);

        tracer.startSpan("foo", Level.INFO);
        tracer.addEvent("fooEvent");
        tracer.endSpan();

        tracer.startSpan("bar", Level.DEBUG);
        tracer.addEvent("fooEvent");
        tracer.endSpan();

        verify(mockOtelSpan, times(1)).addEvent(stringCaptorValues.capture());
        assertEquals("fooEvent", stringCaptorValues.getValue());
    }

    private void setupMock() {
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.INFO).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        tracerSettings = new TracerSettings(settings, clusterSettings);
        openTelemetry = mock(OpenTelemetry.class);
        Tracer mockTracer = mock(Tracer.class);
        when(openTelemetry.getTracer(any(String.class))).thenReturn(mockTracer);
        captor = ArgumentCaptor.forClass(OSSpan.class);
        mockSpanBuilder = mock(SpanBuilder.class);
        mockOtelSpan = mock(io.opentelemetry.api.trace.Span.class);
        when(mockOtelSpan.getSpanContext()).thenReturn(mock(SpanContext.class));
        when(mockSpanBuilder.startSpan()).thenReturn(mockOtelSpan);
        when(mockSpanBuilder.setParent(any())).thenReturn(mockSpanBuilder);
        when(mockTracer.spanBuilder(any(String.class))).thenReturn(mockSpanBuilder);
    }

}
