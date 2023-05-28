/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.noop;

import org.opensearch.tracing.Level;
import org.opensearch.tracing.Span;

/**
 * No-op implementation of Span
 */
public class NoopSpan implements Span {
    private final String spanName;
    private final Span parentSpan;
    private final Level level;

    public NoopSpan(String spanName, Span parentSpan, Level level) {
        this.spanName = spanName;
        this.parentSpan = parentSpan;
        this.level = level;
    }

    @Override
    public Span getParentSpan() {
        return parentSpan;
    }

    @Override
    public Level getLevel() {
        return level;
    }

    @Override
    public String getSpanName() {
        return spanName;
    }

}
