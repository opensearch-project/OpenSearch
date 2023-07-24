/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import java.io.Closeable;
import org.opensearch.common.lease.Releasable;

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

    /**
     * Creates new {@link TracerContextStorage}. This method should only be used cautiously for corner scenarios where
     * multiple independent tasks/async work needs to be submitted from a loop, etc. This is the caller's responsibility
     * to release the newly created storage to restore the parent {@link TracerContextStorage}.
     * @return returns {@link Releasable}, resets the {@link TracerContextStorage} to previous one.
     */
    Releasable newTracerContextStorage();

}
