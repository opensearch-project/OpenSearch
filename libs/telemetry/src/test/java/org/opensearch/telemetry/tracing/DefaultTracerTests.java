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
import org.opensearch.core.action.ActionListener;
import org.opensearch.node.Node;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.telemetry.tracing.MockSpan;
import org.opensearch.test.telemetry.tracing.MockTracingTelemetry;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultTracerTests extends OpenSearchTestCase {

    private TracingTelemetry mockTracingTelemetry;
    private TracerContextStorage<String, Span> mockTracerContextStorage;
    private Span mockSpan;
    private Span mockParentSpan;

    private SpanScope mockSpanScope;
    private ThreadPool threadPool;
    private ExecutorService executorService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new ThreadPool(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "default tracer tests").build());
        executorService = threadPool.executor(ThreadPool.Names.GENERIC);
        setupMocks();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        executorService.shutdown();
        threadPool.shutdownNow();
    }

    public void testCreateSpan() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);

        defaultTracer.startSpan("span_name");

        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
    }

    @SuppressWarnings("unchecked")
    public void testCreateSpanWithAttributesWithMock() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        when(mockTracingTelemetry.createSpan(eq("span_name"), eq(mockParentSpan), eq(attributes))).thenReturn(mockSpan);
        defaultTracer.startSpan("span_name", attributes);
        verify(mockTracingTelemetry).createSpan(eq("span_name"), eq(mockParentSpan), eq(attributes));
    }

    @SuppressWarnings("unchecked")
    public void testCreateSpanWithAttributesWithParentMock() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        when(mockTracingTelemetry.createSpan(eq("span_name"), eq(mockParentSpan), eq(attributes))).thenReturn(mockSpan);
        defaultTracer.startSpan("span_name", new SpanContext(mockParentSpan), attributes);
        verify(mockTracingTelemetry).createSpan(eq("span_name"), eq(mockParentSpan), eq(attributes));
        verify(mockTracerContextStorage, never()).get(TracerContextStorage.CURRENT_SPAN);
    }

    public void testCreateSpanWithAttributes() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextBasedTracerContextStorage(threadContext, tracingTelemetry)
        );

        Span span = defaultTracer.startSpan(
            "span_name",
            Attributes.create().addAttribute("key1", 1.0).addAttribute("key2", 2l).addAttribute("key3", true).addAttribute("key4", "key4")
        );

        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(1.0, ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key1"));
        assertEquals(2l, ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key2"));
        assertEquals(true, ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key3"));
        assertEquals("key4", ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("key4"));
        span.endSpan();
    }

    public void testCreateSpanWithParent() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextBasedTracerContextStorage(new ThreadContext(Settings.EMPTY), tracingTelemetry)
        );

        Span span = defaultTracer.startSpan("span_name", null);

        SpanContext parentSpan = defaultTracer.getCurrentSpan();

        Span span1 = defaultTracer.startSpan("span_name_1", parentSpan, Attributes.EMPTY);

        assertEquals("span_name_1", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(parentSpan.getSpan(), defaultTracer.getCurrentSpan().getSpan().getParentSpan());
        span1.endSpan();
        span.endSpan();
    }

    @SuppressWarnings("unchecked")
    public void testCreateSpanWithContext() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        when(mockTracingTelemetry.createSpan(eq("span_name"), eq(mockParentSpan), eq(attributes))).thenReturn(mockSpan);
        defaultTracer.startSpan(new SpanCreationContext("span_name", attributes));
        verify(mockTracingTelemetry).createSpan(eq("span_name"), eq(mockParentSpan), eq(attributes));
    }

    public void testCreateSpanWithNullParent() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextBasedTracerContextStorage(threadContext, tracingTelemetry)
        );

        Span span = defaultTracer.startSpan("span_name", (SpanContext) null, Attributes.EMPTY);

        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(null, defaultTracer.getCurrentSpan().getSpan().getParentSpan());
        span.endSpan();
    }

    public void testEndSpanByClosingScopedSpan() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);
        ScopedSpan scopedSpan = defaultTracer.startScopedSpan(new SpanCreationContext("span_name", Attributes.EMPTY));
        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(((DefaultScopedSpan) scopedSpan).getSpanScope(), DefaultSpanScope.getCurrentSpanScope());
        scopedSpan.close();
        assertTrue(((MockSpan) ((DefaultScopedSpan) scopedSpan).getSpan()).hasEnded());
        assertEquals(null, defaultTracer.getCurrentSpan());
        assertEquals(null, DefaultSpanScope.getCurrentSpanScope());

    }

    public void testEndSpanByClosingScopedSpanMultiple() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);
        ScopedSpan scopedSpan = defaultTracer.startScopedSpan(new SpanCreationContext("span_name", Attributes.EMPTY));
        ScopedSpan scopedSpan1 = defaultTracer.startScopedSpan(new SpanCreationContext("span_name_1", Attributes.EMPTY));

        assertEquals("span_name_1", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(((DefaultScopedSpan) scopedSpan1).getSpanScope(), DefaultSpanScope.getCurrentSpanScope());

        scopedSpan1.close();
        assertTrue(((MockSpan) ((DefaultScopedSpan) scopedSpan1).getSpan()).hasEnded());
        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(((DefaultScopedSpan) scopedSpan).getSpanScope(), DefaultSpanScope.getCurrentSpanScope());

        scopedSpan.close();
        assertTrue(((MockSpan) ((DefaultScopedSpan) scopedSpan).getSpan()).hasEnded());
        assertEquals(null, defaultTracer.getCurrentSpan());
        assertEquals(null, DefaultSpanScope.getCurrentSpanScope());

    }

    public void testEndSpanByClosingSpanScope() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);
        Span span = defaultTracer.startSpan(new SpanCreationContext("span_name", Attributes.EMPTY));
        SpanScope spanScope = defaultTracer.withSpanInScope(span);
        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(spanScope, DefaultSpanScope.getCurrentSpanScope());

        span.endSpan();
        spanScope.close();
        assertEquals(null, defaultTracer.getCurrentSpan());
        assertTrue(((MockSpan) span).hasEnded());

    }

    public void testEndSpanByClosingSpanScopeMultiple() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);
        Span span = defaultTracer.startSpan(new SpanCreationContext("span_name", Attributes.EMPTY));
        Span span1 = defaultTracer.startSpan(new SpanCreationContext("span_name_1", Attributes.EMPTY));
        SpanScope spanScope = defaultTracer.withSpanInScope(span);
        SpanScope spanScope1 = defaultTracer.withSpanInScope(span1);
        assertEquals("span_name_1", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(spanScope1, DefaultSpanScope.getCurrentSpanScope());

        span1.endSpan();
        spanScope1.close();

        assertEquals("span_name", defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(spanScope, DefaultSpanScope.getCurrentSpanScope());
        assertTrue(((MockSpan) span1).hasEnded());
        assertFalse(((MockSpan) span).hasEnded());
        span.endSpan();
        spanScope.close();

        assertEquals(null, defaultTracer.getCurrentSpan());
        assertEquals(null, DefaultSpanScope.getCurrentSpanScope());
        assertTrue(((MockSpan) span).hasEnded());
        assertTrue(((MockSpan) span1).hasEnded());

    }

    /**
     * 1. CreateSpan in ThreadA (NotScopedSpan)
     * 2. create Async task and pass the span
     * 3. Scope.close
     * 4. verify the current_span is still the same on async thread as the 2
     * 5. verify the main thread has current span as null.
     */
    public void testSpanAcrossThreads() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        AtomicReference<Span> currentSpanRefThread1 = new AtomicReference<>();
        AtomicReference<Span> currentSpanRefThread2 = new AtomicReference<>();
        AtomicReference<Span> currentSpanRefAfterEndThread2 = new AtomicReference<>();

        AtomicReference<Span> spanRef = new AtomicReference<>();
        AtomicReference<Span> spanT2Ref = new AtomicReference<>();

        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);

        executorService.execute(() -> {
            // create a span
            Span span = defaultTracer.startSpan(new SpanCreationContext("span_name_t_1", Attributes.EMPTY));
            SpanScope spanScope = defaultTracer.withSpanInScope(span);
            spanRef.set(span);

            executorService.execute(() -> {
                Span spanT2 = defaultTracer.startSpan(new SpanCreationContext("span_name_t_2", Attributes.EMPTY));
                SpanScope spanScopeT2 = defaultTracer.withSpanInScope(spanT2);
                spanT2Ref.set(spanT2);

                currentSpanRefThread2.set(defaultTracer.getCurrentSpan().getSpan());

                spanT2.endSpan();
                spanScopeT2.close();
                currentSpanRefAfterEndThread2.set(getCurrentSpanFromContext(defaultTracer));
            });
            spanScope.close();
            currentSpanRefThread1.set(getCurrentSpanFromContext(defaultTracer));
        });
        assertEquals(spanT2Ref.get(), currentSpanRefThread2.get());
        assertEquals(spanRef.get(), currentSpanRefAfterEndThread2.get());
        assertEquals(null, currentSpanRefThread1.get());
    }

    public void testSpanCloseOnThread2() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        AtomicReference<SpanContext> currentSpanRefThread1 = new AtomicReference<>();
        AtomicReference<SpanContext> currentSpanRefThread2 = new AtomicReference<>();
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);
        final Span span = defaultTracer.startSpan(new SpanCreationContext("span_name_t1", Attributes.EMPTY));
        try (SpanScope spanScope = defaultTracer.withSpanInScope(span)) {
            executorService.execute(() -> async(new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean response) {
                    span.endSpan();
                    currentSpanRefThread2.set(defaultTracer.getCurrentSpan());
                }

                @Override
                public void onFailure(Exception e) {

                }
            }));
            currentSpanRefThread1.set(defaultTracer.getCurrentSpan());
        }
        assertEquals(null, currentSpanRefThread2.get());
        assertEquals(span, currentSpanRefThread1.get().getSpan());
        assertEquals(null, defaultTracer.getCurrentSpan());
    }

    private void async(ActionListener<Boolean> actionListener) {
        actionListener.onResponse(true);
    }

    /**
     * 1. CreateSpan in ThreadA (NotScopedSpan)
     * 2. create Async task and pass the span
     * 3. Inside Async task start a new span.
     * 4. Scope.close
     * 5. Parent Scope.close
     * 6. verify the current_span is still the same on async thread as the 2
     * 7. verify the main thread has current span as null.
     */
    public void testSpanAcrossThreadsMultipleSpans() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        AtomicReference<Span> currentSpanRefThread1 = new AtomicReference<>();
        AtomicReference<Span> currentSpanRefThread2 = new AtomicReference<>();
        AtomicReference<Span> currentSpanRefAfterEndThread2 = new AtomicReference<>();

        AtomicReference<Span> parentSpanRef = new AtomicReference<>();
        AtomicReference<Span> spanRef = new AtomicReference<>();
        AtomicReference<Span> spanT2Ref = new AtomicReference<>();

        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);

        executorService.execute(() -> {
            // create a parent span
            Span parentSpan = defaultTracer.startSpan(new SpanCreationContext("p_span_name", Attributes.EMPTY));
            SpanScope parentSpanScope = defaultTracer.withSpanInScope(parentSpan);
            parentSpanRef.set(parentSpan);
            // create a span
            Span span = defaultTracer.startSpan(new SpanCreationContext("span_name_t_1", Attributes.EMPTY));
            SpanScope spanScope = defaultTracer.withSpanInScope(span);
            spanRef.set(span);

            executorService.execute(() -> {
                Span spanT2 = defaultTracer.startSpan(new SpanCreationContext("span_name_t_2", Attributes.EMPTY));
                SpanScope spanScopeT2 = defaultTracer.withSpanInScope(spanT2);

                Span spanT21 = defaultTracer.startSpan(new SpanCreationContext("span_name_t_2", Attributes.EMPTY));
                SpanScope spanScopeT21 = defaultTracer.withSpanInScope(spanT2);
                spanT2Ref.set(spanT21);
                currentSpanRefThread2.set(defaultTracer.getCurrentSpan().getSpan());

                spanT21.endSpan();
                spanScopeT21.close();

                spanT2.endSpan();
                spanScopeT2.close();
                currentSpanRefAfterEndThread2.set(getCurrentSpanFromContext(defaultTracer));
            });
            spanScope.close();
            parentSpanScope.close();
            currentSpanRefThread1.set(getCurrentSpanFromContext(defaultTracer));
        });
        assertEquals(spanT2Ref.get(), currentSpanRefThread2.get());
        assertEquals(spanRef.get(), currentSpanRefAfterEndThread2.get());
        assertEquals(null, currentSpanRefThread1.get());
    }

    private static Span getCurrentSpanFromContext(DefaultTracer defaultTracer) {
        return defaultTracer.getCurrentSpan() != null ? defaultTracer.getCurrentSpan().getSpan() : null;
    }

    public void testClose() throws IOException {
        Tracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);

        defaultTracer.close();

        verify(mockTracingTelemetry).close();
    }

    @SuppressWarnings("unchecked")
    private void setupMocks() {
        mockTracingTelemetry = mock(TracingTelemetry.class);
        mockSpan = mock(Span.class);
        mockParentSpan = mock(Span.class);
        mockSpanScope = mock(SpanScope.class);
        mockTracerContextStorage = mock(TracerContextStorage.class);
        when(mockSpan.getSpanName()).thenReturn("span_name");
        when(mockSpan.getSpanId()).thenReturn("span_id");
        when(mockSpan.getTraceId()).thenReturn("trace_id");
        when(mockSpan.getParentSpan()).thenReturn(mockParentSpan);
        when(mockParentSpan.getSpanId()).thenReturn("parent_span_id");
        when(mockParentSpan.getTraceId()).thenReturn("trace_id");
        when(mockTracerContextStorage.get(TracerContextStorage.CURRENT_SPAN)).thenReturn(mockParentSpan, mockSpan);
        when(mockTracingTelemetry.createSpan(eq("span_name"), eq(mockParentSpan), any(Attributes.class))).thenReturn(mockSpan);
    }
}
