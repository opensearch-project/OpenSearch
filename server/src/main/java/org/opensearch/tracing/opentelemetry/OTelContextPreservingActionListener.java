/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.opensearch.action.ActionListener;

import java.util.Objects;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;

/**
 * It does follow -
 * 1. Pass the context to thread working on response/failure of delegated ActionListener.
 * 2. Close the Span if one passed while its construction.
 * 3. Set the scope back to previous context prior to starting the new Span.
 * In case, no Span was started and needs to be closed
 * {@link OTelContextPreservingActionListener#OTelContextPreservingActionListener(ActionListener, Context)} can be used
 * with beforeAttachContext as {@link Context#current()}.
 * @param <Response> Response object type
 */
public final class OTelContextPreservingActionListener<Response> implements ActionListener<Response> {
    private final ActionListener<Response> delegate;
    private final Context beforeAttachContext;
    private final Context afterAttachContext;
    private final String spanID;

    public OTelContextPreservingActionListener(ActionListener<Response> delegate, Context beforeAttachContext, String spanID) {
        this.delegate = delegate;
        this.beforeAttachContext = beforeAttachContext;
        this.afterAttachContext = Context.current();
        this.spanID = spanID;
    }

    public OTelContextPreservingActionListener(ActionListener<Response> delegate, Context beforeAttachContext) {
        this(delegate, beforeAttachContext, null);
    }

    @Override
    public void onResponse(Response r) {
        try (Scope ignored = Objects.requireNonNull(afterAttachContext).makeCurrent()) {
            Span span = Span.current();
            closeCurrentScope(span);
        }
        try (Scope ignored = Objects.requireNonNull(beforeAttachContext).makeCurrent()) {
            delegate.onResponse(r);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try (Scope ignored = Objects.requireNonNull(afterAttachContext).makeCurrent()) {
            Span span = Span.current();
            span.setStatus(StatusCode.ERROR);
            closeCurrentScope(span);
        }
        try (Scope ignored = Objects.requireNonNull(beforeAttachContext).makeCurrent()) {
            delegate.onFailure(e);
        }
    }

    private void closeCurrentScope(Span span) {
        assert spanID == null || span.getSpanContext().getSpanId().equals(spanID);
        span.setAttribute(stringKey("finish-thread-name"), Thread.currentThread().getName());
        span.setAttribute(longKey("finish-thread-id"), Thread.currentThread().getId());
        if (spanID != null) {
            Span.current().end();
        }
    }

    @Override
    public String toString() {
        return getClass().getName() + "/" + delegate.toString();
    }
}
