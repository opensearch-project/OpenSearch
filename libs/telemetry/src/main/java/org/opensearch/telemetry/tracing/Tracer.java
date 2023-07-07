/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.io.Closeable;

/**
 * Tracer is the interface used to create a {@link Span}
 * It automatically handles the context propagation between threads, tasks, nodes etc.
 *
 * All methods on the Tracer object are multi-thread safe.
 */
public interface Tracer extends Closeable {

    /**
     * Starts the {@link Span} with given name
     *
     * @param spanName span name
     * @return scope of the span, must be closed with explicit close or with try-with-resource
     */
    SpanScope startSpan(String spanName);

}
