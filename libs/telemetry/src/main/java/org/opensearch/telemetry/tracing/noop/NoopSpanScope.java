/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.noop;

import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.StatusCode;

/**
 * No-op implementation of SpanScope
 */
public final class NoopSpanScope implements SpanScope {

    /**
     * No-args constructor
     */
    public NoopSpanScope() {}

    @Override
    public void addSpanAttribute(String key, String value) {

    }

    @Override
    public void addSpanAttribute(String key, long value) {

    }

    @Override
    public void addSpanAttribute(String key, double value) {

    }

    @Override
    public void addSpanAttribute(String key, boolean value) {

    }

    @Override
    public void addSpanEvent(String event) {

    }

    @Override
    public void setStatus(StatusCode statusCode) {

    }

    @Override
    public void close() {

    }
}
