/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

public abstract class AbstractSpan implements Span {

    protected final String spanName;
    protected final Span parentSpan;
    protected final Level level;

    protected AbstractSpan(String spanName, Span parentSpan, Level level) {
        this.spanName = spanName;
        this.parentSpan = parentSpan;
        this.level = level;
    }

    @Override
    public Span getParentSpan() {
        return parentSpan;
    }

    @Override
    public String getSpanName() {
        return spanName;
    }

    @Override
    public Level getLevel() {
        return level;
    }
}
