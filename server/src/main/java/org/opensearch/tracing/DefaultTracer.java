/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 *
 * The default tracer implementation. This class implements the basic logic for span lifecycle and its state management.
 * It also handles tracing context propagation between spans.
 *
 * It internally uses OpenTelemetry tracer.
 *
 */
public class DefaultTracer implements Tracer {

    public static final String CURRENT_SPAN = "current_span";

    private static final Logger logger = LogManager.getLogger(DefaultTracer.class);
    static final String TRACE_ID = "trace_id";
    static final String SPAN_ID = "span_id";
    static final String SPAN_NAME = "span_name";
    static final String PARENT_SPAN_ID = "p_span_id";
    static final String THREAD_NAME = "th_name";
    static final String PARENT_SPAN_NAME = "p_span_name";

    private final ThreadPool threadPool;
    private final TracerSettings tracerSettings;
    private final Telemetry telemetry;
    private final SpanFactory spanFactory;

    /**
     * Creates DefaultTracer instance
     *
     * @param telemetry Otel global Opentelemetry instance
     * @param threadPool Thread pool
     * @param tracerSettings tracer related settings
     */
    public DefaultTracer(Telemetry telemetry, ThreadPool threadPool, TracerSettings tracerSettings) {
        this.telemetry = telemetry;
        this.threadPool = threadPool;
        this.tracerSettings = tracerSettings;
        this.spanFactory = new SpanFactory(tracerSettings, telemetry);
    }

    @Override
    public void startSpan(String spanName, Level level) {
        Span span = createSpan(spanName, getCurrentSpan(), level);
        setCurrentSpanInContext(span);
        addDefaultAttributes(span);
    }

    @Override
    public void endSpan() {
        Span currentSpan = getCurrentSpan();
        if (currentSpan != null) {
            currentSpan.endSpan();
            setCurrentSpanInContext(currentSpan.getParentSpan());
        }
    }

    @Override
    public void addSpanAttribute(String key, String value) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addAttribute(key, value);
    }

    @Override
    public void addSpanAttribute(String key, long value) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addAttribute(key, value);
    }

    @Override
    public void addSpanAttribute(String key, double value) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addAttribute(key, value);
    }

    @Override
    public void addSpanAttribute(String key, boolean value) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addAttribute(key, value);
    }

    @Override
    public void addSpanEvent(String event) {
        Span currentSpan = getCurrentSpan();
        currentSpan.addEvent(event);
    }

    @Override
    public void close() {
        try {
            ((Closeable) telemetry).close();
        } catch (IOException e) {
            logger.warn("Error while closing tracer", e);
        }
    }

    // Visible for testing
    Span getCurrentSpan() {
        Optional<Span> optionalSpanFromContext = spanFromThreadContext();
        return optionalSpanFromContext.orElse(spanFromHeader());
    }

    private Span spanFromHeader() {
        return telemetry.extractSpanFromHeader(threadPool.getThreadContext().getHeaders());
    }

    private Optional<Span> spanFromThreadContext() {
        ThreadContext threadContext = threadPool.getThreadContext();
        SpanHolder spanHolder = threadContext.getTransient(CURRENT_SPAN);

        return (spanHolder == null) ? Optional.empty() : Optional.ofNullable(spanHolder.getSpan());
    }

    private Span createSpan(String spanName, Span parentSpan, Level level) {
        return spanFactory.createSpan(spanName, parentSpan, level);
    }

    private void setCurrentSpanInContext(Span span) {
        if (span == null) {
            return;
        }
        ThreadContext threadContext = threadPool.getThreadContext();
        SpanHolder spanHolder = threadContext.getTransient(CURRENT_SPAN);
        if (spanHolder == null) {
            threadContext.putTransient(CURRENT_SPAN, new SpanHolder(span));
        } else {
            spanHolder.setSpan(span);
        }
    }

    private void addDefaultAttributes(Span span) {
        span.addAttribute(SPAN_ID, span.getSpanId());
        span.addAttribute(TRACE_ID, span.getTraceId());
        span.addAttribute(SPAN_NAME, span.getSpanName());
        span.addAttribute(THREAD_NAME, Thread.currentThread().getName());
        if (span.getParentSpan() != null) {
            span.addAttribute(PARENT_SPAN_ID, span.getParentSpan().getSpanId());
            span.addAttribute(PARENT_SPAN_NAME, span.getParentSpan().getSpanName());
        }
    }

}
