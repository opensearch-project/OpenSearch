/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.listener;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.telemetry.tracing.AttributeNames;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.SpanContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;

import static org.opensearch.core.common.Strings.capitalize;

/**
 * SearchRequestOperationsListener subscriber for search request tracing
 *
 * @opensearch.internal
 */
public final class TraceableSearchRequestOperationsListener extends SearchRequestOperationsListener {
    private final Tracer tracer;
    private final Span requestSpan;
    private Span phaseSpan;
    private SpanScope phaseSpanScope;

    public TraceableSearchRequestOperationsListener(final Tracer tracer, final Span requestSpan) {
        this.tracer = tracer;
        this.requestSpan = requestSpan;
        this.phaseSpan = null;
        this.phaseSpanScope = null;
    }

    public static SearchRequestOperationsListener create(final Tracer tracer, final Span requestSpan) {
        if (tracer.isRecording()) {
            return new TraceableSearchRequestOperationsListener(tracer, requestSpan);
        } else {
            return SearchRequestOperationsListener.NOOP;
        }
    }

    @Override
    protected void onPhaseStart(SearchPhaseContext context) {
        assert phaseSpan == null : "There should be only one search phase active at a time";
        phaseSpan = tracer.startSpan(SpanBuilder.from(capitalize(context.getCurrentPhase().getName()), new SpanContext(requestSpan)));
    }

    @Override
    protected void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        assert phaseSpan != null : "There should be a search phase active at that time";
        phaseSpan.endSpan();
        phaseSpan = null;
    }

    @Override
    protected void onPhaseFailure(SearchPhaseContext context, Throwable cause) {
        assert phaseSpan != null : "There should be a search phase active at that time";
        phaseSpan.setError(new Exception(cause));
        phaseSpan.endSpan();
        phaseSpan = null;
    }

    @Override
    public void onRequestStart(SearchRequestContext searchRequestContext) {}

    @Override
    public void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        // add response-related attributes on request end
        requestSpan.addAttribute(
            AttributeNames.TOTAL_HITS,
            searchRequestContext.totalHits() == null ? 0 : searchRequestContext.totalHits().value
        );
    }
}
