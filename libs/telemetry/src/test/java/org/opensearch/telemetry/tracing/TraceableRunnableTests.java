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
        DefaultTracer defaultTracer = new DefaultTracer(new MockTracingTelemetry(), contextStorage);
        final AtomicBoolean isRunnableCompleted = new AtomicBoolean(false);

        TraceableRunnable traceableRunnable = new TraceableRunnable(
            defaultTracer,
            spanName,
            null,
            Attributes.create().addAttribute("name", "value"),
            () -> {
                isRunnableCompleted.set(true);
            }
        );
        traceableRunnable.run();
        assertTrue(isRunnableCompleted.get());
        assertEquals(spanName, defaultTracer.getCurrentSpan().getSpan().getSpanName());
        assertEquals(null, defaultTracer.getCurrentSpan().getSpan().getParentSpan());
        assertEquals("value", ((MockSpan) defaultTracer.getCurrentSpan().getSpan()).getAttribute("name"));

    }

    public void testRunnableWithParent() throws Exception {
        String spanName = "testRunnable";
        String parentSpanName = "parentSpan";
        DefaultTracer defaultTracer = new DefaultTracer(new MockTracingTelemetry(), contextStorage);
        defaultTracer.startSpan(parentSpanName);
        SpanContext parentSpan = defaultTracer.getCurrentSpan();
        AtomicReference<SpanContext> currrntSpan = new AtomicReference<>(new SpanContext(null));
        final AtomicBoolean isRunnableCompleted = new AtomicBoolean(false);
        TraceableRunnable traceableRunnable = new TraceableRunnable(defaultTracer, spanName, parentSpan, Attributes.EMPTY, () -> {
            isRunnableCompleted.set(true);
            currrntSpan.set(defaultTracer.getCurrentSpan());
        });
        traceableRunnable.run();
        assertTrue(isRunnableCompleted.get());
        assertEquals(spanName, currrntSpan.get().getSpan().getSpanName());
        assertEquals(parentSpan.getSpan(), currrntSpan.get().getSpan().getParentSpan());
        assertEquals(parentSpan.getSpan(), defaultTracer.getCurrentSpan().getSpan());
    }
}
