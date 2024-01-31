/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.telemetry.tracing.AttributeNames;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanBuilder;
import org.opensearch.telemetry.tracing.SpanContext;
import org.opensearch.telemetry.tracing.Tracer;

import static org.opensearch.core.common.Strings.capitalize;

/**
 * Listener for search request tracing on the coordinator node
 *
 * @opensearch.internal
 */
public final class SearchRequestCoordinatorTrace extends SearchRequestOperationsListener {
    private final Tracer tracer;

    public SearchRequestCoordinatorTrace(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    void onPhaseStart(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        searchRequestContext.setPhaseSpan(
            tracer.startSpan(
                SpanBuilder.from(
                    "coord" + capitalize(context.getCurrentPhase().getName()),
                    new SpanContext(searchRequestContext.getRequestSpan())
                )
            )
        );
    }

    @Override
    void onPhaseEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        searchRequestContext.getPhaseSpan().endSpan();
    }

    @Override
    void onPhaseFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
        searchRequestContext.getPhaseSpan().endSpan();
    }

    @Override
    void onRequestEnd(SearchRequestContext searchRequestContext) {
        Span requestSpan = searchRequestContext.getRequestSpan();

        // add response-related attributes on request end
        requestSpan.addAttribute(
            AttributeNames.TOTAL_HITS,
            searchRequestContext.totalHits() == null ? "0" : searchRequestContext.totalHits().toString()
        );
        requestSpan.addAttribute(
            AttributeNames.SHARDS,
            searchRequestContext.formattedShardStats().isEmpty() ? "null" : searchRequestContext.formattedShardStats()
        );
        requestSpan.addAttribute(
            AttributeNames.SOURCE,
            searchRequestContext.getSearchRequest().source() == null ? "null" : searchRequestContext.getSearchRequest().source().toString()
        );
    }
}
