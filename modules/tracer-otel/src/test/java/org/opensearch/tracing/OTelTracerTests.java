/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.internal.InternalAttributeKeyImpl;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class OTelTracerTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    private OpenTelemetry openTelemetry;

    private TracerSettings tracerSettings;

    private ArgumentCaptor<OTelSpan> captor;

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
        OTelTracer tracer = new OTelTracer(mock(OpenTelemetry.class), threadPool, mock(TracerSettings.class));

        tracer.startSpan("foo", Level.INFO);
        assertEquals("foo", tracer.getCurrentSpan().getSpanName());
        tracer.endSpan();
        assertEquals("root_span", tracer.getCurrentSpan().getSpanName());
    }

    public void testStartSpanAndEndWithNoopSpans() {
        OTelTracer tracer = spy(new OTelTracer(openTelemetry, threadPool, tracerSettings));

        tracer.startSpan("span1", Level.ROOT);
        tracer.startSpan("span2", Level.TERSE);
        tracer.startSpan("span3", Level.INFO);
        tracer.startSpan("noop-span-1", Level.DEBUG);
        tracer.startSpan("noop-span-2", Level.TRACE);
        tracer.startSpan("span4", Level.INFO);

        verify(tracer, times(4)).createOtelSpan(any(), captor.capture());
        verify(mockSpanBuilder, times(4)).startSpan();
        assertEquals("span4", tracer.getCurrentSpan().getSpanName());
        OTelSpan value = captor.getValue();
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
        assertEquals("root_span", tracer.getCurrentSpan().getSpanName());
        verify(mockOtelSpan, times(4)).end();
    }

    public void testAddAttribute() {
        OTelTracer tracer = new OTelTracer(openTelemetry, threadPool, tracerSettings);

        tracer.startSpan("foo", Level.INFO);
        tracer.addSpanAttribute("stringKey", "value");
        tracer.addSpanAttribute("longKey", 1L);
        tracer.addSpanAttribute("doubleKey", 1.0);
        tracer.addSpanAttribute("booleanKey", true);
        tracer.endSpan();
        ArgumentCaptor<AttributeKey<String>> stringKeyCaptorValues = ArgumentCaptor.forClass(AttributeKey.class);
        ArgumentCaptor<String> stringValueCaptorValues = ArgumentCaptor.forClass(String.class);

        // 6 other default string attributes are added
        verify(mockOtelSpan, times(1 + 6)).setAttribute(stringKeyCaptorValues.capture(), stringValueCaptorValues.capture());
        verify(mockOtelSpan).setAttribute(eq(InternalAttributeKeyImpl.create("longKey", AttributeType.LONG)), eq(1L));
        verify(mockOtelSpan).setAttribute(eq(InternalAttributeKeyImpl.create("doubleKey", AttributeType.DOUBLE)), eq(1.0));
        verify(mockOtelSpan).setAttribute(eq(InternalAttributeKeyImpl.create("booleanKey", AttributeType.BOOLEAN)), eq(true));

        assertTrue(stringValueCaptorValues.getAllValues().contains("value"));
    }

    public void testAddEvent() {
        ArgumentCaptor<String> stringCaptorValues = ArgumentCaptor.forClass(String.class);

        OTelTracer tracer = new OTelTracer(openTelemetry, threadPool, tracerSettings);

        tracer.startSpan("foo", Level.INFO);
        tracer.addSpanEvent("fooEvent");
        tracer.endSpan();

        tracer.startSpan("bar", Level.DEBUG);
        tracer.addSpanEvent("fooEvent");
        tracer.endSpan();

        verify(mockOtelSpan, times(1)).addEvent(stringCaptorValues.capture());
        assertEquals("fooEvent", stringCaptorValues.getValue());
    }

    private void setupMock() {
        Set<Setting<?>> allTracerSettings = new HashSet<>();
        ClusterSettings.FEATURE_FLAGGED_CLUSTER_SETTINGS.get(List.of(FeatureFlags.TRACER)).stream().forEach((allTracerSettings::add));
        Settings settings = Settings.builder().put(TracerSettings.TRACER_LEVEL_SETTING.getKey(), Level.INFO).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, allTracerSettings);
        tracerSettings = new TracerSettings(settings, clusterSettings);
        openTelemetry = mock(OpenTelemetry.class);
        Tracer mockTracer = mock(Tracer.class);
        when(openTelemetry.getTracer(any(String.class))).thenReturn(mockTracer);
        captor = ArgumentCaptor.forClass(OTelSpan.class);
        mockSpanBuilder = mock(SpanBuilder.class);
        mockOtelSpan = mock(io.opentelemetry.api.trace.Span.class);
        SpanContext mockSpanContext = mock(SpanContext.class);
        when(mockSpanContext.getSpanId()).thenReturn("span_id");
        when(mockSpanContext.getTraceId()).thenReturn("trace_id");
        when(mockOtelSpan.getSpanContext()).thenReturn(mockSpanContext);
        when(mockSpanBuilder.startSpan()).thenReturn(mockOtelSpan);
        when(mockSpanBuilder.setParent(any())).thenReturn(mockSpanBuilder);
        when(mockTracer.spanBuilder(any(String.class))).thenReturn(mockSpanBuilder);
    }

}
