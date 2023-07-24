/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.telemetry.tracing.MockTracingTelemetry;

public class TracerTests extends OpenSearchTestCase {

    List<SpanScope> spansToBeClosed = new ArrayList<>();
    private final ThreadContextBasedTracerContextStorage contextStorage = new ThreadContextBasedTracerContextStorage(
        new ThreadContext(Settings.EMPTY),
        new MockTracingTelemetry()
    );

    public void testIterationScenarioFail() throws Exception {

        DefaultTracer defaultTracer = new DefaultTracer(new MockTracingTelemetry(), contextStorage);
        try (SpanScope parentSpanScope = defaultTracer.startSpan("parentSpan")) {
            Span parentSpan = defaultTracer.getCurrentSpan();
            Span newWrongParent = null;
            for (int i = 0; i < 3; i++) {
                String spanName = "childSpan_" + i;
                SpanScope child = defaultTracer.startSpan(spanName);
                spansToBeClosed.add(child);
                if (i == 0) {
                    assertEquals(parentSpan, defaultTracer.getCurrentSpan().getParentSpan());
                } else {
                    assertEquals(newWrongParent, defaultTracer.getCurrentSpan().getParentSpan());
                }
                newWrongParent = defaultTracer.getCurrentSpan();
            }
        }
        // closing at the end to mimic the async task submission behaviour.
        spansToBeClosed.forEach(a -> a.close());
    }

    public void testIterationScenarioSuccess() throws Exception {
        List<SpanScope> spansToBeClosed = new ArrayList<>();
        DefaultTracer defaultTracer = new DefaultTracer(new MockTracingTelemetry(), contextStorage);
        try (SpanScope parentSpanScope = defaultTracer.startSpan("parentSpan")) {
            Span parentSpan = defaultTracer.getCurrentSpan();
            for (int i = 0; i < 3; i++) {
                String spanName = "childSpan_" + i;
                try (Releasable releasable = defaultTracer.newTracerContextStorage()) {
                    SpanScope child = defaultTracer.startSpan(spanName);
                    spansToBeClosed.add(child);
                    assertEquals(parentSpan, defaultTracer.getCurrentSpan().getParentSpan());
                }
            }
        }
        // closing at the end to mimic the async task submission behaviour.
        spansToBeClosed.forEach(a -> a.close());
    }
}
