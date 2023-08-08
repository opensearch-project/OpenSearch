/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.runnable;

import org.opensearch.telemetry.tracing.SpanContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

/**
 * Wraps the runnable and add instrumentation to trace the {@link Runnable}
 */
public class TraceableRunnable implements Runnable {
    private final Runnable runnable;
    private final SpanContext parent;
    private final Tracer tracer;
    private final String spanName;

    /**
     * Constructor.
     * @param tracer tracer
     * @param spanName spanName
     * @param parent parent Span.
     * @param runnable runnable.
     */
    public TraceableRunnable(Tracer tracer, String spanName, SpanContext parent, Runnable runnable) {
        this.tracer = tracer;
        this.spanName = spanName;
        this.parent = parent;
        this.runnable = runnable;
    }

    @Override
    public void run() {
        try (SpanScope spanScope = tracer.startSpan(spanName, parent)) {
            runnable.run();
        }
    }
}
