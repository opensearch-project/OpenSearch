/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * Wrapper class to hold reference of Span
 *
 * @opensearch.internal
 */
final class SpanReference {

    private Span span;

    /**
     * Creates the wrapper with given span
     * @param span the span object to wrap
     */
    public SpanReference(Span span) {
        this.span = span;
    }

    /**
     * Returns the span object
     * @return underlying span
     */
    public Span getSpan() {
        return span;
    }

    /**
     * Updates the underlying span
     * @param span underlying span
     */
    public void setSpan(Span span) {
        this.span = span;
    }
}
