/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.telemetry.tracing.attributes.Attributes;

/**
 * Context for span details.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class SpanCreationContext {
    private final String spanName;
    private final Attributes attributes;

    /**
     * Constructor.
     * @param spanName span name.
     * @param attributes attributes.
     */
    public SpanCreationContext(String spanName, Attributes attributes) {
        this.spanName = spanName;
        this.attributes = attributes;
    }

    /**
     * Returns the span name.
     * @return span name
     */
    public String getSpanName() {
        return spanName;
    }

    /**
     * Returns the span attributes.
     * @return attributes.
     */
    public Attributes getAttributes() {
        return attributes;
    }
}
