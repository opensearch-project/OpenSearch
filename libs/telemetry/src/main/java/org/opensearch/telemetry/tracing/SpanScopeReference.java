/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * Wrapper class to hold reference of {@link SpanScope}
 *
 * @opensearch.internal
 */
final class SpanScopeReference {

    private SpanScope spanScope;

    /**
     * Creates the wrapper with given spanScope
     * @param spanScope the spanScope object to wrap
     */
    public SpanScopeReference(SpanScope spanScope) {
        this.spanScope = spanScope;
    }

    /**
     * Returns the spanScope object
     * @return underlying spanScope
     */
    public SpanScope getSpanScope() {
        return spanScope;
    }

    /**
     * Updates the underlying span
     * @param spanScope underlying span
     */
    public void setSpanScope(SpanScope spanScope) {
        this.spanScope = spanScope;
    }
}
