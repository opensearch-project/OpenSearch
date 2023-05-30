/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.context.Context;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tracing.noop.NoopSpan;

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
public class OTelTracer implements Tracer {

    private static final Logger logger = LogManager.getLogger(OTelTracer.class);
    private static final String TRACE_ID = "trace_id";
    private static final String SPAN_ID = "span_id";
    private static final String SPAN_NAME = "span_name";
    private static final String PARENT_SPAN_ID = "p_span_id";
    private static final String THREAD_NAME = "th_name";
    private static final String PARENT_SPAN_NAME = "p_span_name";
    private static final String ROOT_SPAN = "root_span";

    private final ThreadPool threadPool;
    private final TracerSettings tracerSettings;
    private final io.opentelemetry.api.trace.Tracer otelTracer;
    private final OpenTelemetry openTelemetry;

    /**
     * Creates DefaultTracer instance
     *
     * @param openTelemetry Otel global Opentelemetry instance
     * @param threadPool Thread pool
     * @param tracerSettings tracer related settings
     */
    public OTelTracer(OpenTelemetry openTelemetry, ThreadPool threadPool, TracerSettings tracerSettings) {
        this.openTelemetry = openTelemetry;
        this.otelTracer = openTelemetry.getTracer("os-tracer");
        this.threadPool = threadPool;
        this.tracerSettings = tracerSettings;
    }

    @Override
    public void startSpan(String spanName, Level level) {
        Span span = createSpan(spanName, getCurrentSpan(), level);
        setCurrentSpanInContext(span);
        setSpanAttributes(span);
    }

    @Override
    public void endSpan() {
        Span currentSpan = getCurrentSpan();
        if (currentSpan != null) {
            endSpan(currentSpan);
            setCurrentSpanInContext(currentSpan.getParentSpan());
        }
    }

    @Override
    public void addSpanAttribute(String key, String value) {
        addSingleAttribute(AttributeKey.stringKey(key), value);
    }

    @Override
    public void addSpanAttribute(String key, long value) {
        addSingleAttribute(AttributeKey.longKey(key), value);

    }

    @Override
    public void addSpanAttribute(String key, double value) {
        addSingleAttribute(AttributeKey.doubleKey(key), value);
    }

    @Override
    public void addSpanAttribute(String key, boolean value) {
        addSingleAttribute(AttributeKey.booleanKey(key), value);
    }

    @Override
    public void addSpanEvent(String event) {
        Span currentSpan = getCurrentSpan();
        if (currentSpan instanceof OTelSpan && ((OTelSpan) currentSpan).getOtelSpan() != null) {
            ((OTelSpan) currentSpan).getOtelSpan().addEvent(event);
        }
    }

    @Override
    public void close() {
        if (openTelemetry instanceof Closeable) {
            try {
                ((Closeable) openTelemetry).close();
            } catch (IOException e) {
                logger.warn("Error while closing tracer", e);
            }
        }
    }

    @Override
    public Span getCurrentSpan() {
        Optional<Span> optionalSpanFromContext = spanFromThreadContext();
        return optionalSpanFromContext.orElse(spanFromHeader());
    }

    private Span spanFromHeader() {
        Context context = TracerUtils.extractTracerContextFromHeader(threadPool.getThreadContext().getHeaders());
        if (context != null) {
            io.opentelemetry.api.trace.Span span = io.opentelemetry.api.trace.Span.fromContext(context);
            return new OTelSpan(ROOT_SPAN, span, null, Level.ROOT);
        }
        return null;
    }

    private Optional<Span> spanFromThreadContext() {
        ThreadContext threadContext = threadPool.getThreadContext();
        SpanHolder spanHolder = threadContext.getTransient(CURRENT_SPAN);

        return (spanHolder == null) ? Optional.empty() : Optional.ofNullable(spanHolder.getSpan());
    }

    private Span createSpan(String spanName, Span parentSpan, Level level) {
        return isLevelEnabled(level) ? createDefaultSpan(spanName, parentSpan, level) : createNoopSpan(spanName, parentSpan, level);
    }

    private Span createDefaultSpan(String spanName, Span parentSpan, Level level) {
        OTelSpan parentOTelSpan = getLastValidSpanInChain(parentSpan);
        io.opentelemetry.api.trace.Span otelSpan = createOtelSpan(spanName, parentOTelSpan);
        Span span = new OTelSpan(spanName, otelSpan, parentSpan, level);
        logger.trace(
            "Starting OtelSpan spanId:{} name:{}: traceId:{}",
            otelSpan.getSpanContext().getSpanId(),
            span.getSpanName(),
            otelSpan.getSpanContext().getTraceId()
        );
        return span;
    }

    private NoopSpan createNoopSpan(String spanName, Span parentSpan, Level level) {
        logger.trace("Starting Noop span name:{}", spanName);
        return new NoopSpan(spanName, parentSpan, level);
    }

    private OTelSpan getLastValidSpanInChain(Span parentSpan) {
        while (parentSpan instanceof NoopSpan) {
            parentSpan = parentSpan.getParentSpan();
        }
        return (OTelSpan) parentSpan;
    }

    // visible for testing
    io.opentelemetry.api.trace.Span createOtelSpan(String spanName, OTelSpan parentOTelSpan) {
        return parentOTelSpan == null
            ? otelTracer.spanBuilder(spanName).startSpan()
            : otelTracer.spanBuilder(spanName).setParent(Context.current().with(parentOTelSpan.getOtelSpan())).startSpan();
    }

    private boolean isLevelEnabled(Level level) {
        Level configuredLevel = tracerSettings.getTracerLevel();
        return level.isHigherOrEqual(configuredLevel);
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

    private void endSpan(Span span) {
        if (span instanceof OTelSpan && ((OTelSpan) span).getOtelSpan() != null) {
            OTelSpan oTelSpan = (OTelSpan) span;
            logger.trace(
                "Ending span spanId:{} name:{}: traceId:{}",
                oTelSpan.getSpanContext().getSpanId(),
                span.getSpanName(),
                oTelSpan.getSpanContext().getTraceId()
            );
            oTelSpan.getOtelSpan().end();
        } else {
            logger.trace("Ending noop span name:{}", span.getSpanName());
        }
    }

    private void setSpanAttributes(Span span) {
        if (span instanceof OTelSpan) {
            addDefaultAttributes((OTelSpan) span);
        }
    }

    private <T> void addSingleAttribute(AttributeKey<T> key, T value) {
        Span currentSpan = getCurrentSpan();
        if (currentSpan instanceof OTelSpan && ((OTelSpan) currentSpan).getOtelSpan() != null) {
            ((OTelSpan) currentSpan).getOtelSpan().setAttribute(key, value);
        }
    }

    private void addDefaultAttributes(OTelSpan oTelSpan) {
        if (oTelSpan != null) {
            addSingleAttribute(AttributeKey.stringKey(SPAN_ID), oTelSpan.getSpanContext().getSpanId());
            addSingleAttribute(AttributeKey.stringKey(TRACE_ID), oTelSpan.getSpanContext().getTraceId());
            addSingleAttribute(AttributeKey.stringKey(SPAN_NAME), oTelSpan.getSpanName());
            addSingleAttribute(AttributeKey.stringKey(THREAD_NAME), Thread.currentThread().getName());
            if (oTelSpan.getParentSpan() != null && oTelSpan.getParentSpan() instanceof OTelSpan) {
                addSingleAttribute(
                    AttributeKey.stringKey(PARENT_SPAN_ID),
                    ((OTelSpan) oTelSpan.getParentSpan()).getSpanContext().getSpanId()
                );
                addSingleAttribute(AttributeKey.stringKey(PARENT_SPAN_NAME), oTelSpan.getParentSpan().getSpanName());
            }
        }
    }

}
