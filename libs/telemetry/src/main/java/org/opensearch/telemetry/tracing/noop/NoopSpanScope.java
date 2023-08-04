/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.noop;

import org.opensearch.telemetry.tracing.SpanScope;

/**
 * No-op implementation of {@link SpanScope}.
 *
 * <p>This class is used as a placeholder when actual span-scoping is not required.
 * It provides empty implementations for all methods in the {@link SpanScope} interface.
 *
 * @opensearch.internal
 */
public final class NoopSpanScope implements SpanScope {

    /**
     * No-args constructor
     */
    public NoopSpanScope() {}

    /**
     * No-op implementation of {@link SpanScope#addSpanAttribute(String, String)}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addSpanAttribute(String key, String value) {

    }

    /**
     * No-op implementation of {@link SpanScope#addSpanAttribute(String, long)}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addSpanAttribute(String key, long value) {

    }

    /**
     * No-op implementation of {@link SpanScope#addSpanAttribute(String, double)}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addSpanAttribute(String key, double value) {

    }

    /**
     * No-op implementation of {@link SpanScope#addSpanAttribute(String, boolean)}.
     *
     * @param key   attribute key
     * @param value attribute value
     */
    @Override
    public void addSpanAttribute(String key, boolean value) {

    }

    /**
     * No-op implementation of {@link SpanScope#addSpanEvent(String)}.
     *
     * @param event event name
     */
    @Override
    public void addSpanEvent(String event) {

    }

    /**
     * No-op implementation of {@link SpanScope#setError(Exception)}.
     *
     * @param exception exception to be recorded
     */
    @Override
    public void setError(Exception exception) {

    }

    /**
     * No-op implementation of {@link SpanScope#close()}.
     */
    @Override
    public void close() {

    }
}
