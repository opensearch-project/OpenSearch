/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.action.ActionListener;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.telemetry.listeners.TraceEventListenerService;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * tracer utility class to be deleted
 */
public class TracerUtil {

    private static TraceEventListenerService traceEventListenerService;


    public static <T, R, E extends Exception> CheckedFunction<T, R, E> wrapAndCallFunction(String spanName, CheckedFunction<T, R, E> originalFunction,
                                                                                           Map<String, String> attributes) {
        return t -> {
            try(SpanScope scope = traceEventListenerService.getTracer().startSpan(spanName, attributes)) {
                return  originalFunction.apply(t);
            }
        };
    }

    public static void setTraceEventListenerService(TraceEventListenerService traceEventListenerService) {
        TracerUtil.traceEventListenerService = traceEventListenerService;
    }

    /**
     * starts the span and invokes the function under the scope of new span, closes the scope when function is invoked.
     * Wraps the ActionListener with {@link OTelContextPreservingActionListener} for context propagation and ends the span
     * on response/failure of action listener.
     */
    public static <R> void callFunctionAndStartSpan(String spanName, BiFunction<Object[], ActionListener<?>, R> function,
                                                    ActionListener<?> actionListener, Object... args) {
        callFunctionAndStartSpan(spanName, function, actionListener, new HashMap<>(), args);
    }

    public static <R> void callFunctionAndStartSpan(String spanName, BiFunction<Object[], ActionListener<?>, R> function,
                                                    ActionListener<?> actionListener, Map<String, String> attributes, Object... args) {
        SpanScope scope = traceEventListenerService.getTracer().startSpan(spanName, attributes);
        actionListener = new OTelContextPreservingActionListener<>(actionListener, scope);
        function.apply(args, actionListener);
    }

    static final class OTelContextPreservingActionListener<Response> implements ActionListener<Response> {
        private final ActionListener<Response> delegate;
        private final SpanScope scope;
        private final Span span;
        public OTelContextPreservingActionListener(ActionListener<Response> delegate, SpanScope scope) {
            this.delegate = delegate;
            this.scope = scope;
            this.span = traceEventListenerService.getTracer().getCurrentSpan();
        }

        @Override
        public void onResponse(Response r) {
            // tracer.setCurrentSpanInContext(span);
            scope.close();
            delegate.onResponse(r);
        }

        @Override
        public void onFailure(Exception e) {
            // tracer.setCurrentSpanInContext(span);
            scope.setError(e);
            scope.close();
            delegate.onFailure(e);
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate.toString();
        }
    }
}
