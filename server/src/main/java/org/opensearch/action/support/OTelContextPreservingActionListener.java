/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.support;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.opensearch.action.ActionListener;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.otel.OtelService;

import java.util.Objects;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

/**
 * Restores the given {@link ThreadContext.StoredContext}
 * once the listener is invoked
 *
 * @opensearch.internal
 */
public final class OTelContextPreservingActionListener<R> implements ActionListener<R> {

    private final ActionListener<R> delegate;
    public final Context beforeAttachContext;
    public final Context afterAttachContext;

    private final String spanID;

    public final long startCPUUsage;
    public final long startMemoryUsage;
    public final long startThreadContentionTime;

    public OTelContextPreservingActionListener(Context beforeAttachContext, Context afterAttachContext, ActionListener<R> delegate, String spanID) {
        this.delegate = delegate;
        this.beforeAttachContext = beforeAttachContext;
        this.afterAttachContext = afterAttachContext;
        this.spanID = spanID;
        startCPUUsage = OtelService.getCPUUsage(Thread.currentThread().getId());
        startMemoryUsage = OtelService.getMemoryUsage(Thread.currentThread().getId());
        startThreadContentionTime = OtelService.getThreadContentionTime(Thread.currentThread().getId());
    }

    @Override
    public void onResponse(R r) {
        try (Scope ignored = Objects.requireNonNull(afterAttachContext).makeCurrent()) {
            Span span = Span.current();
            if (spanID != null && Span.current() != Span.getInvalid() &&
                span.getSpanContext().getSpanId().equals(spanID)) {
                span.setAttribute(AttributeKey.longKey("CPUUsage"),
                    OtelService.getCPUUsage(Thread.currentThread().getId())- startCPUUsage);
                span.setAttribute(AttributeKey.longKey("MemoryUsage"),
                    OtelService.getMemoryUsage(Thread.currentThread().getId())- startMemoryUsage);
                span.setAttribute(AttributeKey.longKey("ContentionTime"),
                    OtelService.getThreadContentionTime(Thread.currentThread().getId())- startThreadContentionTime);
                span.setAttribute(stringKey("finish-thread-name"), Thread.currentThread().getName());
                span.setAttribute(longKey("finish-thread-id"), Thread.currentThread().getId());
                Span.current().end();
            }
        }

        try (Scope ignored = Objects.requireNonNull(beforeAttachContext).makeCurrent()) {
            delegate.onResponse(r);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (Scope ignored = Objects.requireNonNull(afterAttachContext).makeCurrent()) {
            if (spanID != null && Span.current() != Span.getInvalid() &&
                Span.current().getSpanContext().getSpanId().equals(spanID)) {
                Span.current().end();
            }
        }
        try (Scope ignored = Objects.requireNonNull(beforeAttachContext).makeCurrent()) {
            delegate.onFailure(e);
        }
    }

    @Override
    public String toString() {
        return getClass().getName() + "/" + delegate.toString();
    }
}
