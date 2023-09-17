/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.runnable;

import org.opensearch.telemetry.tracing.ScopedSpan;
import org.opensearch.telemetry.tracing.SpanContext;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.attributes.Attributes;

/**
 * Wraps the runnable and add instrumentation to trace the {@link Runnable}
 */
public class TraceableRunnable implements Runnable {
    private final Runnable runnable;
    private final SpanContext parent;
    private final Tracer tracer;
    private final String spanName;
    private final Attributes attributes;

    /**
     * Constructor.
     * @param tracer tracer
     * @param spanName spanName
     * @param parent parent Span.
     * @param attributes attributes.
     * @param runnable runnable.
     */
    public TraceableRunnable(Tracer tracer, String spanName, SpanContext parent, Attributes attributes, Runnable runnable) {
        this.tracer = tracer;
        this.spanName = spanName;
        this.parent = parent;
        this.attributes = attributes;
        this.runnable = runnable;
    }

    @Override
    public void run() {
        try (ScopedSpan spanScope = tracer.startScopedSpan(new SpanCreationContext(spanName, attributes), parent)) {
            runnable.run();
        }
    }
}
