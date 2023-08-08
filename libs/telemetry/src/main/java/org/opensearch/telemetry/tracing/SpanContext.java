/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * Wrapped Span will be exposed to the code outside of tracing package for sharing the {@link Span} without having access to
 * its properties.
 */
public final class SpanContext {
    private final Span span;

    /**
     * Constructor.
     * @param span span to be wrapped.
     */
    public SpanContext(Span span) {
        this.span = span;
    }

    Span getSpan() {
        return span;
    }
}
