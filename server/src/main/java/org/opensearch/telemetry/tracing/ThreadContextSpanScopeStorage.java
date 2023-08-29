/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.util.concurrent.ThreadContext;

import java.util.Objects;
import java.util.Optional;

/**
 * Core's ThreadContext based TracerContextStorage implementation
 *
 * @opensearch.internal
 */
public class ThreadContextSpanScopeStorage implements TracerContextStorage<String, SpanScope> {

    private final ThreadContext threadContext;

    public ThreadContextSpanScopeStorage(ThreadContext threadContext) {
        this.threadContext = Objects.requireNonNull(threadContext);
    }

    @Override
    public SpanScope get(String key) {
        return getCurrentSpanScope(key);
    }

    @Override
    public void put(String key, SpanScope spanScope) {
        SpanScopeReference currentSpanRef = threadContext.getTransient(key);
        if (currentSpanRef == null) {
            threadContext.putTransient(key, new SpanScopeReference(spanScope));
        } else {
            currentSpanRef.setSpanScope(spanScope);
        }
    }

    SpanScope getCurrentSpanScope(String key) {
        return spanFromThreadContext(key).orElse(null);
    }

    private Optional<SpanScope> spanFromThreadContext(String key) {
        SpanScopeReference currentSpanScopeRef = threadContext.getTransient(key);
        return (currentSpanScopeRef == null) ? Optional.empty() : Optional.ofNullable(currentSpanScopeRef.getSpanScope());
    }

}
