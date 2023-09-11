/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.noop;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.tracing.ScopedSpan;

/**
 * No-op implementation of SpanScope
 *
 * @opensearch.internal
 */
@InternalApi
public final class NoopScopedSpan implements ScopedSpan {

    /**
     * No-args constructor
     */
    public NoopScopedSpan() {}

    @Override
    public void addAttribute(String key, String value) {

    }

    @Override
    public void addAttribute(String key, long value) {

    }

    @Override
    public void addAttribute(String key, double value) {

    }

    @Override
    public void addAttribute(String key, boolean value) {

    }

    @Override
    public void addEvent(String event) {

    }

    @Override
    public void setError(Exception exception) {

    }

    @Override
    public void close() {

    }
}
