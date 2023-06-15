/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.noop;

import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.AbstractSpan;
import org.opensearch.telemetry.tracing.Level;

/**
 * No-op implementation of Span
 */
public class NoopSpan extends AbstractSpan {

    /**
     * Creates a no-op span
     * @param spanName span's name
     * @param parentSpan span's parent span
     * @param level span's level
     */
    public NoopSpan(String spanName, Span parentSpan, Level level) {
        super(spanName, parentSpan, level);
    }

    @Override
    public void endSpan() {}

    @Override
    public void addAttribute(String key, String value) {}

    @Override
    public void addAttribute(String key, Long value) {}

    @Override
    public void addAttribute(String key, Double value) {}

    @Override
    public void addAttribute(String key, Boolean value) {}

    @Override
    public void addEvent(String event) {}

    @Override
    public String getTraceId() {
        return null;
    }

    @Override
    public String getSpanId() {
        return null;
    }

}
