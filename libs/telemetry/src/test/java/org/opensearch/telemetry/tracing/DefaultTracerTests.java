/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.telemetry.tracing.MockSpan;
import org.opensearch.test.telemetry.tracing.MockTracingTelemetry;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultTracerTests extends OpenSearchTestCase {

    private TracingTelemetry mockTracingTelemetry;
    private TracerContextStorage<String, Span> mockTracerContextSpanStorage;
    private TracerContextStorage<String, SpanScope> mockTracerContextSpanScopeStorage;
    private Span mockSpan;
    private Span mockParentSpan;

    private SpanScope mockSpanScope;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        setupMocks();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testCreateSpan() {
        DefaultTracer defaultTracer = new DefaultTracer(
            mockTracingTelemetry,
            mockTracerContextSpanStorage,
            mockTracerContextSpanScopeStorage
        );

        defaultTracer.startSpan("span_name");

        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
    }

    public void testCreateSpanWithAttributesWithMock() {
        DefaultTracer defaultTracer = new DefaultTracer(
            mockTracingTelemetry,
            mockTracerContextSpanStorage,
            mockTracerContextSpanScopeStorage
        );
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        when(mockTracingTelemetry.createSpan("span_name", mockParentSpan, attributes)).thenReturn(mockSpan);
        defaultTracer.startSpan("span_name", attributes);
        verify(mockTracingTelemetry).createSpan("span_name", mockParentSpan, attributes);
    }

    public void testCreateSpanWithAttributesWithParentMock() {
        DefaultTracer defaultTracer = new DefaultTracer(
            mockTracingTelemetry,
            mockTracerContextSpanStorage,
            mockTracerContextSpanScopeStorage
        );
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        when(mockTracingTelemetry.createSpan("span_name", mockParentSpan, attributes)).thenReturn(mockSpan);
        defaultTracer.startSpan("span_name", new SpanContext(mockParentSpan), attributes);
        verify(mockTracingTelemetry).createSpan("span_name", mockParentSpan, attributes);
        verify(mockTracerContextSpanStorage, never()).get(TracerContextStorage.CURRENT_SPAN);
    }

    public void testCreateSpanWithAttributes() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextSpanStorage(threadContext, tracingTelemetry),
            new ThreadContextSpanScopeStorage(threadContext)
        );

        defaultTracer.startSpan(
            "span_name",
            Attributes.create().addAttribute("key1", 1.0).addAttribute("key2", 2l).addAttribute("key3", true).addAttribute("key4", "key4")
        );

        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(1.0, ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key1"));
        assertEquals(2l, ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key2"));
        assertEquals(true, ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key3"));
        assertEquals("key4", ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key4"));
    }

    public void testCreateSpanWithParent() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextSpanStorage(new ThreadContext(Settings.EMPTY), tracingTelemetry),
            mockTracerContextSpanScopeStorage
        );

        defaultTracer.startSpan("span_name", null);

        SpanContext parentSpan = defaultTracer.getCurrentSpan();

        defaultTracer.startSpan("span_name_1", parentSpan, Attributes.EMPTY);

        assertEquals("span_name_1", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(parentSpan.getSpan(), defaultTracer.getCurrentSpan().getSpan().getParentSpan());
    }

    public void testCreateSpanWithContext() {
        DefaultTracer defaultTracer = new DefaultTracer(
            mockTracingTelemetry,
            mockTracerContextSpanStorage,
            mockTracerContextSpanScopeStorage
        );
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        when(mockTracingTelemetry.createSpan("span_name", mockParentSpan, attributes)).thenReturn(mockSpan);
        defaultTracer.startSpan(new SpanCreationContext("span_name", attributes));
        verify(mockTracingTelemetry).createSpan("span_name", mockParentSpan, attributes);
    }

    public void testCreateSpanWithNullParent() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextSpanStorage(threadContext, tracingTelemetry),
            new ThreadContextSpanScopeStorage(threadContext)
        );

        defaultTracer.startSpan("span_name", (SpanContext) null, Attributes.EMPTY);

        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(null, defaultTracer.getCurrentSpan().getSpan().getParentSpan());
    }

    public void testEndSpanByClosingScopedSpan() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextSpanStorage spanTracerStorage = new ThreadContextSpanStorage(threadContext, tracingTelemetry);
        ThreadContextSpanScopeStorage spanScopeTracerContextStorage = new ThreadContextSpanScopeStorage(threadContext);
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage, spanScopeTracerContextStorage);
        ScopedSpan scopedSpan = defaultTracer.startScopedSpan(new SpanCreationContext("span_name", Attributes.EMPTY));
        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(
            ((DefaultScopedSpan) scopedSpan).getSpanScope(),
            spanScopeTracerContextStorage.getCurrentSpanScope(TracerContextStorage.CURRENT_SPAN_SCOPE)
        );
        scopedSpan.close();
        assertTrue(((MockSpan) ((DefaultScopedSpan) scopedSpan).getSpan()).hasEnded());
        assertEquals(null, defaultTracer.getCurrentSpan());
        assertEquals(null, spanScopeTracerContextStorage.getCurrentSpanScope(TracerContextStorage.CURRENT_SPAN_SCOPE));

    }

    public void testEndSpanByClosingScopedSpanMultiple() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextSpanStorage spanTracerStorage = new ThreadContextSpanStorage(threadContext, tracingTelemetry);
        ThreadContextSpanScopeStorage spanScopeTracerContextStorage = new ThreadContextSpanScopeStorage(threadContext);
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage, spanScopeTracerContextStorage);
        ScopedSpan scopedSpan = defaultTracer.startScopedSpan(new SpanCreationContext("span_name", Attributes.EMPTY));
        ScopedSpan scopedSpan1 = defaultTracer.startScopedSpan(new SpanCreationContext("span_name_1", Attributes.EMPTY));

        assertEquals("span_name_1", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(
            ((DefaultScopedSpan) scopedSpan1).getSpanScope(),
            spanScopeTracerContextStorage.getCurrentSpanScope(TracerContextStorage.CURRENT_SPAN_SCOPE)
        );

        scopedSpan1.close();
        assertTrue(((MockSpan) ((DefaultScopedSpan) scopedSpan1).getSpan()).hasEnded());
        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(
            ((DefaultScopedSpan) scopedSpan).getSpanScope(),
            spanScopeTracerContextStorage.getCurrentSpanScope(TracerContextStorage.CURRENT_SPAN_SCOPE)
        );

        scopedSpan.close();
        assertTrue(((MockSpan) ((DefaultScopedSpan) scopedSpan).getSpan()).hasEnded());
        assertEquals(null, defaultTracer.getCurrentSpan());
        assertEquals(null, spanScopeTracerContextStorage.getCurrentSpanScope(TracerContextStorage.CURRENT_SPAN_SCOPE));

    }

    public void testEndSpanByClosingSpanScope() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextSpanStorage spanTracerStorage = new ThreadContextSpanStorage(threadContext, tracingTelemetry);
        ThreadContextSpanScopeStorage spanScopeTracerContextStorage = new ThreadContextSpanScopeStorage(threadContext);
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage, spanScopeTracerContextStorage);
        Span span = defaultTracer.startSpan(new SpanCreationContext("span_name", Attributes.EMPTY));
        SpanScope spanScope = defaultTracer.createSpanScope(span);
        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(spanScope, spanScopeTracerContextStorage.getCurrentSpanScope(TracerContextStorage.CURRENT_SPAN_SCOPE));

        spanScope.close();
        assertEquals(null, defaultTracer.getCurrentSpan());
        assertFalse(((MockSpan) span).hasEnded());

    }

    public void testEndSpanByClosingSpanScopeMultiple() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextSpanStorage spanTracerStorage = new ThreadContextSpanStorage(threadContext, tracingTelemetry);
        ThreadContextSpanScopeStorage spanScopeTracerContextStorage = new ThreadContextSpanScopeStorage(threadContext);
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage, spanScopeTracerContextStorage);
        Span span = defaultTracer.startSpan(new SpanCreationContext("span_name", Attributes.EMPTY));
        Span span1 = defaultTracer.startSpan(new SpanCreationContext("span_name_1", Attributes.EMPTY));
        SpanScope spanScope = defaultTracer.createSpanScope(span);
        SpanScope spanScope1 = defaultTracer.createSpanScope(span1);
        assertEquals("span_name_1", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(spanScope1, spanScopeTracerContextStorage.getCurrentSpanScope(TracerContextStorage.CURRENT_SPAN_SCOPE));

        spanScope1.close();

        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(spanScope, spanScopeTracerContextStorage.getCurrentSpanScope(TracerContextStorage.CURRENT_SPAN_SCOPE));
        assertFalse(((MockSpan) span1).hasEnded());
        assertFalse(((MockSpan) span).hasEnded());
        spanScope.close();

        assertEquals(null, defaultTracer.getCurrentSpan());
        assertEquals(null, spanScopeTracerContextStorage.getCurrentSpanScope(TracerContextStorage.CURRENT_SPAN_SCOPE));
        assertFalse(((MockSpan) span).hasEnded());
        assertFalse(((MockSpan) span1).hasEnded());

    }

    public void testClose() throws IOException {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextSpanStorage, mockTracerContextSpanScopeStorage);

        defaultTracer.close();

        verify(mockTracingTelemetry).close();
    }

    @SuppressWarnings("unchecked")
    private void setupMocks() {
        mockTracingTelemetry = mock(TracingTelemetry.class);
        mockSpan = mock(Span.class);
        mockParentSpan = mock(Span.class);
        mockSpanScope = mock(SpanScope.class);
        mockTracerContextSpanStorage = mock(TracerContextStorage.class);
        mockTracerContextSpanScopeStorage = mock(TracerContextStorage.class);
        when(mockSpan.getSpanName()).thenReturn("span_name");
        when(mockSpan.getSpanId()).thenReturn("span_id");
        when(mockSpan.getTraceId()).thenReturn("trace_id");
        when(mockSpan.getParentSpan()).thenReturn(mockParentSpan);
        when(mockParentSpan.getSpanId()).thenReturn("parent_span_id");
        when(mockParentSpan.getTraceId()).thenReturn("trace_id");
        when(mockTracerContextSpanStorage.get(TracerContextStorage.CURRENT_SPAN)).thenReturn(mockParentSpan, mockSpan);
        when(mockTracerContextSpanScopeStorage.get(TracerContextStorage.CURRENT_SPAN)).thenReturn(mockSpanScope);
        when(mockTracingTelemetry.createSpan(eq("span_name"), eq(mockParentSpan), any(Attributes.class))).thenReturn(mockSpan);
    }
}
