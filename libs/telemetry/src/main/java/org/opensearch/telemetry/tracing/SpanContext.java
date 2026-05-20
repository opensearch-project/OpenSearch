/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Wrapped Span will be exposed to the code outside of tracing package for sharing the {@link Span} without having access to
 * its properties.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
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

    /**
     * Sets the error for the current span behind this context
     * @param cause error
     */
    public void setError(final Exception cause) {
        span.setError(cause);
    }

    /**
     * Ends current span
     */
    public void endSpan() {
        span.endSpan();
    }
}
