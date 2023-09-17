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
import org.opensearch.telemetry.tracing.runnable.TraceableRunnable;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.telemetry.tracing.MockSpan;
import org.opensearch.test.telemetry.tracing.MockTracingTelemetry;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TraceableRunnableTests extends OpenSearchTestCase {

    private final ThreadContextBasedTracerContextStorage contextStorage = new ThreadContextBasedTracerContextStorage(
        new ThreadContext(Settings.EMPTY),
        new MockTracingTelemetry()
    );

    public void testRunnableWithNullParent() throws Exception {
        String spanName = "testRunnable";
        final DefaultTracer defaultTracer = new DefaultTracer(new MockTracingTelemetry(), contextStorage);
        final AtomicBoolean isRunnableCompleted = new AtomicBoolean(false);
        final AtomicReference<String> spanNameCaptured = new AtomicReference<>();
        final AtomicReference<String> attributeValue = new AtomicReference<>();
        TraceableRunnable traceableRunnable = new TraceableRunnable(
            defaultTracer,
            spanName,
            null,
            Attributes.create().addAttribute("name", "value"),
            () -> {
                spanNameCaptured.set(defaultTracer.getCurrentSpan().getSpan().getSpanName());
                attributeValue.set((String) ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("name"));
                isRunnableCompleted.set(true);
            }
        );
        traceableRunnable.run();
        assertTrue(isRunnableCompleted.get());
        assertEquals(spanName, spanNameCaptured.get());
        assertEquals(null, defaultTracer.getCurrentSpan());
        assertEquals(null, defaultTracer.getCurrentSpan());
        assertEquals("value", attributeValue.get());
    }

    public void testRunnableWithParent() throws Exception {
        String spanName = "testRunnable";
        String parentSpanName = "parentSpan";
        DefaultTracer defaultTracer = new DefaultTracer(new MockTracingTelemetry(), contextStorage);
        ScopedSpan scopedSpan = defaultTracer.startScopedSpan(new SpanCreationContext(parentSpanName, Attributes.EMPTY));
        SpanContext parentSpanContext = defaultTracer.getCurrentSpan();
        AtomicReference<SpanContext> currentSpan = new AtomicReference<>();
        final AtomicBoolean isRunnableCompleted = new AtomicBoolean(false);
        TraceableRunnable traceableRunnable = new TraceableRunnable(defaultTracer, spanName, parentSpanContext, Attributes.EMPTY, () -> {
            isRunnableCompleted.set(true);
            currentSpan.set(defaultTracer.getCurrentSpan());
        });
        traceableRunnable.run();
        assertTrue(isRunnableCompleted.get());
        assertEquals(spanName, currentSpan.get().getSpan().getSpanName());
        assertEquals(((DefaultScopedSpan) scopedSpan).getSpan(), currentSpan.get().getSpan().getParentSpan());
        assertEquals(((DefaultScopedSpan) scopedSpan).getSpan(), defaultTracer.getCurrentSpan().getSpan());
        scopedSpan.close();
    }
}
