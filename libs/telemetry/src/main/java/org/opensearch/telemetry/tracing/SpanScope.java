/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.telemetry.tracing.noop.NoopSpanScope;

/**
 * An auto-closeable that represents scope of the span.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SpanScope extends AutoCloseable {

    /**
     * No-op Scope implementation
     */
    SpanScope NO_OP = new NoopSpanScope();

    @Override
    void close();

    /**
     * Attaches span to the {@link SpanScope}
     * @return spanScope
     */
    SpanScope attach();

    /**
     * Returns span attached with the {@link SpanScope}
     * @return span.
     */
    Span getSpan();
}
