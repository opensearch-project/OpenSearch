/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * Base span
 *
 * @opensearch.internal
 */
public abstract class AbstractSpan implements Span {

    /**
     * name of the span
     */
    private final String spanName;

    /**
     * The parent span of this span.
     */
    private final Span parentSpan;

    /**
     * Base constructor
     * @param spanName name of the span
     * @param parentSpan span's parent span
     */
    protected AbstractSpan(String spanName, Span parentSpan) {
        this.spanName = spanName;
        this.parentSpan = parentSpan;
    }

    /**
     * Gets the parent span of this span.
     *
     * @return the parent span of this span, or {@code null} if there's no parent.
     */
    @Override
    public Span getParentSpan() {
        return parentSpan;
    }

    /**
     * Gets the name of the span.
     *
     * @return the name of the span.
     */
    @Override
    public String getSpanName() {
        return spanName;
    }
}
