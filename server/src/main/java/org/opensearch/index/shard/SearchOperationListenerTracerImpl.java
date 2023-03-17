/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.opensearch.rest.action.admin.indices.RestGetAliasesAction;
import org.opensearch.search.internal.SearchContext;

public class SearchOperationListenerTracerImpl implements SearchOperationListener {

    private Span parentSpan;
    private Span child1Span;
    private Span child2Span;
    private Tracer tracer =
        RestGetAliasesAction.openTelemetry.getTracer("instrumentation-library-name", "1.0.0");

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        SearchOperationListener.super.onPreQueryPhase(searchContext);
        System.out.println("parent opening");
        System.out.println("child opening");

        parentSpan = tracer.spanBuilder("onPreQueryPhase").startSpan();

// Make the span the current span
        try (Scope ss = parentSpan.makeCurrent()) {
            parentSpan.setAttribute("attrib", "parent_attrib");
            parentSpan.setAttribute("http.url", "mm55");
            childOne(tracer, parentSpan, "onQueryPhase", child1Span);
        } finally {
//            parent.end();
        }
        //parent open
    }

    private void childOne(Tracer tracer, Span parentSpan, String name, Span span) {
        child1Span = tracer.spanBuilder(name)
            .setParent(Context.current().with(parentSpan))
            .startSpan();
        try (Scope ss = child1Span.makeCurrent()) {
            child1Span.setAttribute("attrib", "child1_attrib");
        } finally {
//            childSpan.end();
        }
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        SearchOperationListener.super.onQueryPhase(searchContext, tookInNanos);
        System.out.println("child closing");
        child1Span.end();

        //parent close
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        SearchOperationListener.super.onPreFetchPhase(searchContext);
        System.out.println("child1 opening");
        child2Span = tracer.spanBuilder("onPreFetchPhase")
            .setParent(Context.current().with(parentSpan))
            .startSpan();
        try (Scope ss = child2Span.makeCurrent()) {
            child2Span.setAttribute("attrib", "child2_attrib");
        } finally {
//            childSpan.end();
        }
        // child open
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        SearchOperationListener.super.onFetchPhase(searchContext, tookInNanos);
        System.out.println("child1 closing");
        System.out.println("parent closing");
        child2Span.end();
        parentSpan.end();

        // child close
    }
}
