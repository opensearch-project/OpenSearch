/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.attributes.Attributes;

import java.io.Closeable;

/**
 * Interface for tracing telemetry providers
 *
 * @opensearch.internal
 */
public interface TracingTelemetry extends Closeable {

    /**
     * Creates span with provided arguments
     * @param spanName name of the span
     * @param parentSpan span's parent span
     * @param attributes attributes to be added.
     * @return span instance
     */
    Span createSpan(String spanName, Span parentSpan, Attributes attributes);

    /**
     * provides tracing context propagator
     * @return tracing context propagator instance
     */
    TracingContextPropagator getContextPropagator();

    /**
     * closes the resource
     */
    void close();

}
