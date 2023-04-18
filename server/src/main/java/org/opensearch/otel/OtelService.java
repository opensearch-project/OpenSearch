/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.otel;

import com.sun.management.ThreadMXBean;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.opensearch.action.ActionListener;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;


public class OtelService {
    public static Resource resource;
    public static SdkTracerProvider sdkTracerProvider;
    public static SdkMeterProvider sdkMeterProvider;
    public static OpenTelemetry openTelemetry;
    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    public List<OtelEventListener> otelEventListenerList;
    static {
        resource = Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "opensearch-tasks")));

        sdkTracerProvider = SdkTracerProvider.builder()
            //.addSpanProcessor(SimpleSpanProcessor.create(LoggingSpanExporter.create()))
            .addSpanProcessor(SimpleSpanProcessor.create(OtlpHttpSpanExporter.builder().build()))
            .setResource(resource)
            .build();

        sdkMeterProvider = SdkMeterProvider.builder()
            .registerMetricReader(PeriodicMetricReader.builder(OtlpGrpcMetricExporter.builder().build()).build())
            .setResource(resource)
            .build();

        openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setMeterProvider(sdkMeterProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();
    }

    public OtelService(List<OtelEventListener> otelEventListenerList) {
        this.otelEventListenerList = otelEventListenerList;
    }

    public static long getCPUUsage(long threadId) {
        return threadMXBean.getThreadCpuTime(threadId);
    }

    public static long getMemoryUsage(long threadId) {
        return threadMXBean.getThreadAllocatedBytes(threadId);
    }

    public static long getThreadContentionTime(long threadId) {
        return threadMXBean.getThreadInfo(threadId).getBlockedTime();
    }

    public static <R> void callFunctionAndStartSpan(String spanName, BiFunction<Object[], ActionListener<?>, R> function,
                                                    ActionListener<?> actionListener, Object... args) {
        Context beforeAttach = Context.current();
        Span span = startSpan(spanName);
        try(Scope ignored = span.makeCurrent()) {
            actionListener = new SpanPreservingActionListener<>(actionListener, beforeAttach, span.getSpanContext().getSpanId());
            emitResources(span, spanName + "-Start", OtelEventListeners.getInstance(null));
            function.apply(args, actionListener);
        } finally {
            emitResources(span, spanName + "-End", OtelEventListeners.getInstance(null));
        }
    }

    private static Span startSpan(String spanName) {
        Tracer tracer = OtelService.sdkTracerProvider.get("recover");
        Span span = tracer.spanBuilder(spanName).setParent(Context.current()).startSpan();
        span.setAttribute(stringKey("start-thread-name"), Thread.currentThread().getName());
        span.setAttribute(longKey("start-thread-id"), Thread.currentThread().getId());
        return span;
    }

    private static void emitResources(Span span, String name, List<OtelEventListener> otelEventListenerList) {
        emitResources(span, name, Thread.currentThread(), otelEventListenerList);
    }
    private static void emitResources(Span span, String name, Thread t, List<OtelEventListener> otelEventListenerList) {
        Context context = Context.current();
        if (context != Context.root()) {
            try (Scope ignored = span.makeCurrent()) {
                if (otelEventListenerList != null && !otelEventListenerList.isEmpty()) {
                    for (OtelEventListener eventListener : otelEventListenerList) {
                        eventListener.onEvent(span);
                    }
                }
                span.addEvent(name,
                    Attributes.of(
                        AttributeKey.longKey("ThreadID"), t.getId(),
                        AttributeKey.stringKey("ThreadName"), t.getName(),
                        AttributeKey.longKey("CPUUsage"), OtelService.getCPUUsage(t.getId()),
                        AttributeKey.longKey("MemoryUsage"), OtelService.getMemoryUsage(t.getId()),
                        AttributeKey.longKey("ContentionTime"), OtelService.getThreadContentionTime(t.getId())
                    )
                );
            }
        }
    }

    public static class OtelEventListeners {
        static volatile List<OtelEventListener> INSTANCE;
        public static List<OtelEventListener> getInstance(List<OtelEventListener> otelEventListenerList) {
            if (INSTANCE == null) {
                synchronized (OtelEventListeners.class) {
                    if (INSTANCE == null) {
                        INSTANCE = otelEventListenerList;
                    }
                }
            }
            return INSTANCE;
        }
    }

    public static ExecutorService taskWrapping(ExecutorService delegate) {
        return new CurrentContextExecutorService(delegate, OtelEventListeners.getInstance(null));
    }

    public static ExecutorService taskWrapping(ExecutorService delegate, List<OtelEventListener> otelEventListeners) {
        return new CurrentContextExecutorService(delegate, otelEventListeners);
    }
    private static <T> Callable<T> wrapTask(Callable<T> callable, List<OtelEventListener> otelEventListeners) {
        return () -> {
            try (Scope ignored = Context.current().makeCurrent()) {
                emitResources(Span.current(), Span.current().getSpanContext().getSpanId() + "-" +
                    Thread.currentThread().getName() + "-Start", otelEventListeners);
                return callable.call();
            } finally {
                emitResources(Span.current(), Span.current().getSpanContext().getSpanId() + "-" +
                    Thread.currentThread().getName() + "-End", otelEventListeners);
            }
        };
    }

    static Runnable wrapTask(Runnable runnable, List<OtelEventListener> otelEventListeners) {
        return () -> {
            try (Scope ignored = Context.current().makeCurrent()) {
                emitResources(Span.current(), Span.current().getSpanContext().getSpanId() + "-" +
                    Thread.currentThread().getName() + "-Start", otelEventListeners);
                runnable.run();
                emitResources(Span.current(), Span.current().getSpanContext().getSpanId() + "-" +
                    Thread.currentThread().getName() + "-End", otelEventListeners);
            }
        };
    }

    static final class CurrentContextExecutorService extends ForwardingExecutorService {

        private List<OtelEventListener> otelEventListeners;

        CurrentContextExecutorService(ExecutorService delegate) {
            this(delegate, null);
        }

        CurrentContextExecutorService(ExecutorService delegate, List<OtelEventListener> otelEventListeners) {
            super(delegate);
            this.otelEventListeners = otelEventListeners;
        }

        public List<OtelEventListener> getOtelEventListeners() {
            return otelEventListeners;
        }
        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return delegate().submit(wrapTask(task, otelEventListeners));
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return delegate().submit(wrapTask(task, otelEventListeners), result);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return delegate().submit(wrapTask(task, otelEventListeners));
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
            return delegate().invokeAll(wrap(tasks, otelEventListeners));
        }

        @Override
        public <T> List<Future<T>> invokeAll(
            Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
            return delegate().invokeAll(wrap(tasks, otelEventListeners), timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException {
            return delegate().invokeAny(wrap(tasks, otelEventListeners));
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            return delegate().invokeAny(wrap(tasks, otelEventListeners), timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate().execute(wrapTask(command, otelEventListeners));
        }
    }

    static abstract class ForwardingExecutorService implements ExecutorService {

        private final ExecutorService delegate;

        protected ForwardingExecutorService(ExecutorService delegate) {
            this.delegate = delegate;
        }

        ExecutorService delegate() {
            return delegate;
        }

        @Override
        public final void shutdown() {
            delegate.shutdown();
        }

        @Override
        public final List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public final boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public final boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public final boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        protected static <T> Collection<? extends Callable<T>> wrap(Collection<? extends Callable<T>> tasks,
                                                                    List<OtelEventListener> otelEventListeners) {
            List<Callable<T>> wrapped = new ArrayList<>();
            for (Callable<T> task : tasks) {
                wrapped.add(wrapTask(task, otelEventListeners));
            }
            return wrapped;
        }
    }

    public static final class SpanPreservingActionListener<R> implements ActionListener<R> {
        private final ActionListener<R> delegate;
        private final Context beforeAttachContext;
        private final Context afterAttachContext;
        private final String spanID;

        public SpanPreservingActionListener(ActionListener<R> delegate, Context beforeAttachContext, String spanID) {
            this.delegate = delegate;
            this.beforeAttachContext = beforeAttachContext;
            this.afterAttachContext = Context.current();
            this.spanID = spanID;
        }

        public SpanPreservingActionListener(ActionListener<R> delegate, Context beforeAttachContext) {
            this(delegate, beforeAttachContext, null);
        }

        @Override
        public void onResponse(R r) {
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
}
