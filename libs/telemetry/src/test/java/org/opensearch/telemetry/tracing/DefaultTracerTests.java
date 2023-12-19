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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
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

    private ThreadPool threadPool;
    private ExecutorService executorService;
    private SpanCreationContext spanCreationContext;

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

        defaultTracer.startSpan(spanCreationContext);

        String spanName = defaultTracer.getCurrentSpan().getSpan().getSpanName();
        assertEquals("span_name", spanName);
        assertTrue(defaultTracer.isRecording());
    }

    @SuppressWarnings("unchecked")
    public void testCreateSpanWithAttributesWithMock() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        SpanCreationContext spanCreationContext = buildSpanCreationContext("span_name", attributes, mockParentSpan);
        when(mockTracingTelemetry.createSpan(eq(spanCreationContext), eq(mockParentSpan))).thenReturn(mockSpan);
        defaultTracer.startSpan(spanCreationContext);
        verify(mockTracingTelemetry).createSpan(eq(spanCreationContext), eq(mockParentSpan));
    }

    @SuppressWarnings("unchecked")
    public void testCreateSpanWithAttributesWithParentMock() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        SpanCreationContext spanCreationContext = buildSpanCreationContext("span_name", attributes, mockParentSpan);
        when(mockTracingTelemetry.createSpan(eq(spanCreationContext), eq(mockParentSpan))).thenReturn(mockSpan);
        defaultTracer.startSpan(spanCreationContext);
        verify(mockTracingTelemetry).createSpan(eq(spanCreationContext), eq(mockParentSpan));
        verify(mockTracerContextStorage, never()).get(TracerContextStorage.CURRENT_SPAN);
    }

    public void testCreateSpanWithAttributes() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextBasedTracerContextStorage(threadContext, tracingTelemetry)
        );

        SpanCreationContext spanCreationContext = buildSpanCreationContext(
            "span_name",
            Attributes.create().addAttribute("key1", 1.0).addAttribute("key2", 2l).addAttribute("key3", true).addAttribute("key4", "key4"),
            null
        );

        Span span = defaultTracer.startSpan(spanCreationContext);

        assertThat(defaultTracer.getCurrentSpan(), is(nullValue()));
        assertEquals(1.0, ((MockSpan) span).getAttribute("key1"));
        assertEquals(2l, ((MockSpan) span).getAttribute("key2"));
        assertEquals(true, ((MockSpan) span).getAttribute("key3"));
        assertEquals("key4", ((MockSpan) span).getAttribute("key4"));
        span.endSpan();
    }

    public void testCreateSpanWithParent() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextBasedTracerContextStorage(new ThreadContext(Settings.EMPTY), tracingTelemetry)
        );

        SpanCreationContext spanCreationContext = buildSpanCreationContext("span_name", null, null);

        Span span = defaultTracer.startSpan(spanCreationContext, null);

        try (final SpanScope scope = defaultTracer.withSpanInScope(span)) {
            SpanContext parentSpan = defaultTracer.getCurrentSpan();

            SpanCreationContext spanCreationContext1 = buildSpanCreationContext("span_name_1", Attributes.EMPTY, parentSpan.getSpan());

            try (final ScopedSpan span1 = defaultTracer.startScopedSpan(spanCreationContext1)) {
                assertEquals("span_name_1", defaultTracer.getCurrentSpan().getSpan().getSpanName());
                assertEquals(parentSpan.getSpan(), defaultTracer.getCurrentSpan().getSpan().getParentSpan());
            }
        } finally {
            span.endSpan();
        }
    }

    @SuppressWarnings("unchecked")
    public void testCreateSpanWithContext() {
        DefaultTracer defaultTracer = new DefaultTracer(mockTracingTelemetry, mockTracerContextStorage);
        Attributes attributes = Attributes.create().addAttribute("name", "value");
        SpanCreationContext spanCreationContext = buildSpanCreationContext("span_name", attributes, mockParentSpan);
        when(mockTracingTelemetry.createSpan(eq(spanCreationContext), eq(mockParentSpan))).thenReturn(mockSpan);
        defaultTracer.startSpan(spanCreationContext);
        verify(mockTracingTelemetry).createSpan(eq(spanCreationContext), eq(mockParentSpan));
    }

    public void testCreateSpanWithNullParent() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        DefaultTracer defaultTracer = new DefaultTracer(
            tracingTelemetry,
            new ThreadContextBasedTracerContextStorage(threadContext, tracingTelemetry)
        );

        SpanCreationContext spanCreationContext = buildSpanCreationContext("span_name", Attributes.EMPTY, null);

        Span span = defaultTracer.startSpan(spanCreationContext);

        assertThat(defaultTracer.getCurrentSpan(), is(nullValue()));
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
        SpanCreationContext spanCreationContext = buildSpanCreationContext("span_name", Attributes.EMPTY, null);

        ScopedSpan scopedSpan = defaultTracer.startScopedSpan(spanCreationContext);

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
        SpanCreationContext spanCreationContext = buildSpanCreationContext("span_name", Attributes.EMPTY, null);
        SpanCreationContext spanCreationContext1 = buildSpanCreationContext("span_name_1", Attributes.EMPTY, null);

        ScopedSpan scopedSpan = defaultTracer.startScopedSpan(spanCreationContext);
        ScopedSpan scopedSpan1 = defaultTracer.startScopedSpan(spanCreationContext1);

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
        SpanCreationContext spanCreationContext = buildSpanCreationContext("span_name", Attributes.EMPTY, null);
        Span span = defaultTracer.startSpan(spanCreationContext);
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
        Span span = defaultTracer.startSpan(buildSpanCreationContext("span_name", Attributes.EMPTY, null));
        Span span1 = defaultTracer.startSpan(buildSpanCreationContext("span_name_1", Attributes.EMPTY, null));
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

        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);

        CompletableFuture<?> asyncTask = CompletableFuture.runAsync(() -> {
            // create a span
            Span span = defaultTracer.startSpan(buildSpanCreationContext("span_name_t_1", Attributes.EMPTY, null));
            SpanScope spanScope = defaultTracer.withSpanInScope(span);

            CompletableFuture<?> asyncTask1 = CompletableFuture.runAsync(() -> {
                Span spanT2 = defaultTracer.startSpan(buildSpanCreationContext("span_name_t_2", Attributes.EMPTY, null));
                SpanScope spanScopeT2 = defaultTracer.withSpanInScope(spanT2);
                assertEquals(spanT2, defaultTracer.getCurrentSpan().getSpan());

                spanScopeT2.close();
                spanT2.endSpan();
                assertEquals(null, defaultTracer.getCurrentSpan());
            }, executorService);
            asyncTask1.join();
            spanScope.close();
            span.endSpan();
            assertEquals(null, defaultTracer.getCurrentSpan());
        }, executorService);
        asyncTask.join();
    }

    public void testSpanCloseOnThread2() {
        TracingTelemetry tracingTelemetry = new MockTracingTelemetry();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);
        final Span span = defaultTracer.startSpan(buildSpanCreationContext("span_name_t1", Attributes.EMPTY, null));
        try (SpanScope spanScope = defaultTracer.withSpanInScope(span)) {
            CompletableFuture<?> asyncTask = CompletableFuture.runAsync(() -> async(new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean response) {
                    try (SpanScope s = defaultTracer.withSpanInScope(span)) {
                        assertEquals(span, defaultTracer.getCurrentSpan().getSpan());
                    } finally {
                        span.endSpan();
                    }
                }

                @Override
                public void onFailure(Exception e) {

                }
            }), executorService);
            assertEquals(span, defaultTracer.getCurrentSpan().getSpan());
            asyncTask.join();
        }
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

        ThreadContextBasedTracerContextStorage spanTracerStorage = new ThreadContextBasedTracerContextStorage(
            threadContext,
            tracingTelemetry
        );
        DefaultTracer defaultTracer = new DefaultTracer(tracingTelemetry, spanTracerStorage);

        CompletableFuture<?> asyncTask = CompletableFuture.runAsync(() -> {
            // create a parent span
            Span parentSpan = defaultTracer.startSpan(buildSpanCreationContext("p_span_name", Attributes.EMPTY, null));
            SpanScope parentSpanScope = defaultTracer.withSpanInScope(parentSpan);
            // create a span
            Span span = defaultTracer.startSpan(buildSpanCreationContext("span_name_t_1", Attributes.EMPTY, null));
            SpanScope spanScope = defaultTracer.withSpanInScope(span);

            CompletableFuture<?> asyncTask1 = CompletableFuture.runAsync(() -> {
                Span spanT2 = defaultTracer.startSpan(buildSpanCreationContext("span_name_t_2", Attributes.EMPTY, null));
                SpanScope spanScopeT2 = defaultTracer.withSpanInScope(spanT2);
                Span spanT21 = defaultTracer.startSpan(buildSpanCreationContext("span_name_t_2", Attributes.EMPTY, null));
                SpanScope spanScopeT21 = defaultTracer.withSpanInScope(spanT21);
                assertEquals(spanT21, defaultTracer.getCurrentSpan().getSpan());
                spanScopeT21.close();
                spanT21.endSpan();

                spanScopeT2.close();
                spanT2.endSpan();

                assertEquals(null, defaultTracer.getCurrentSpan());
            }, executorService);

            asyncTask1.join();

            spanScope.close();
            span.endSpan();
            parentSpanScope.close();
            parentSpan.endSpan();
            assertEquals(null, defaultTracer.getCurrentSpan());
        }, executorService);
        asyncTask.join();
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
        mockTracerContextStorage = mock(TracerContextStorage.class);
        when(mockSpan.getSpanName()).thenReturn("span_name");
        when(mockSpan.getSpanId()).thenReturn("span_id");
        when(mockSpan.getTraceId()).thenReturn("trace_id");
        when(mockSpan.getParentSpan()).thenReturn(mockParentSpan);
        when(mockParentSpan.getSpanId()).thenReturn("parent_span_id");
        when(mockParentSpan.getTraceId()).thenReturn("trace_id");
        spanCreationContext = buildSpanCreationContext("span_name", Attributes.EMPTY, mockParentSpan);
        when(mockTracerContextStorage.get(TracerContextStorage.CURRENT_SPAN)).thenReturn(mockSpan, mockParentSpan);
        when(mockTracingTelemetry.createSpan(eq(spanCreationContext), eq(mockParentSpan))).thenReturn(mockSpan);
    }

    private SpanCreationContext buildSpanCreationContext(String spanName, Attributes attributes, Span parentSpan) {
        SpanCreationContext spanCreationContext = SpanCreationContext.internal().name(spanName).attributes(attributes);
        if (parentSpan != null) {
            spanCreationContext.parent(new SpanContext(parentSpan));
        }
        return spanCreationContext;
    }
}
