/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.opentelemetry;

import com.sun.management.ThreadMXBean;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
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
import org.opensearch.performanceanalyzer.listener.DiskStatsTaskEventListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.tracing.TaskEventListener;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.function.BiFunction;

import static io.opentelemetry.api.common.AttributeKey.longKey;
import static io.opentelemetry.api.common.AttributeKey.stringKey;


public class OpenTelemetryService {
    public static Resource resource;
    public static SdkTracerProvider sdkTracerProvider;
    public static SdkMeterProvider sdkMeterProvider;
    public static OpenTelemetry openTelemetry;
    private static final List<TaskEventListener> DEFAULT_TASK_EVENT_LISTENERS;
    private static final List<String> allowedThreadPools = List.of(ThreadPool.Names.GENERIC);
    private static final ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();

    static {
        resource = Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "opensearch-tasks")));

        sdkTracerProvider = SdkTracerProvider.builder()
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
        var defaultListener = new TaskEventListener() {
            @Override
            public void onStart(String operationName, String eventName, Thread t) {
                Span span = Span.current();
                if (span != Span.getInvalid()) {
                    span.addEvent(eventName,
                        Attributes.of(
                            AttributeKey.longKey("ThreadID"), t.getId(),
                            AttributeKey.stringKey("ThreadName"), t.getName(),
                            AttributeKey.longKey("CPUUsage"), threadMXBean.getThreadCpuTime(t.getId()),
                            AttributeKey.longKey("MemoryUsage"), threadMXBean.getThreadAllocatedBytes(t.getId()),
                            AttributeKey.longKey("ContentionTime"), threadMXBean.getThreadInfo(t.getId()).getBlockedTime()
                        )
                    );
                }
            }

            @Override
            public void onEnd(String operationName, String eventName, Thread t) {
                Span span = Span.current();
                if (span != Span.getInvalid()) {
                    span.addEvent(eventName,
                        Attributes.of(
                            AttributeKey.longKey("ThreadID"), t.getId(),
                            AttributeKey.stringKey("ThreadName"), t.getName(),
                            AttributeKey.longKey("CPUUsage"), threadMXBean.getThreadCpuTime(t.getId()),
                            AttributeKey.longKey("MemoryUsage"), threadMXBean.getThreadAllocatedBytes(t.getId()),
                            AttributeKey.longKey("ContentionTime"), threadMXBean.getThreadInfo(t.getId()).getBlockedTime()
                        )
                    );
                }
            }

            @Override
            public boolean isApplicable(String operationName, String eventName) {
                return true;
            }
        };

        DEFAULT_TASK_EVENT_LISTENERS = List.of(defaultListener, new DiskStatsTaskEventListener());
    }

    public static boolean isThreadPoolAllowed(String threadPoolName) {
        return allowedThreadPools.contains(threadPoolName);
    }

    /**
     * starts the span and invokes the function under the scope of new span, closes the scope when function is invoked.
     * Wraps the ActionListener with {@link OTelContextPreservingActionListener} for context propagation and ends the span
     * on response/failure of action listener.
     */
    public static <R> void callFunctionAndStartSpan(String spanName, BiFunction<Object[], ActionListener<?>, R> function,
                                                    ActionListener<?> actionListener, Object... args) {
        Context beforeAttach = Context.current();
        Span span = startSpan(spanName);
        try(Scope ignored = span.makeCurrent()) {
            actionListener = new OTelContextPreservingActionListener<>(actionListener, beforeAttach, span.getSpanContext().getSpanId());
            callTaskEventListeners(true, "", spanName + "-Start", Thread.currentThread(),
                TaskEventListeners.getInstance(null));
            function.apply(args, actionListener);
        } finally {
            callTaskEventListeners(false, "", spanName + "-End", Thread.currentThread(),
                TaskEventListeners.getInstance(null));
        }
    }

    /**
     * TODO - to be replaced when OpenSearch tracing APIs are available
     */
    private static Span startSpan(String spanName) {
        Tracer tracer = OpenTelemetryService.sdkTracerProvider.get("recover");
        Span span = tracer.spanBuilder(spanName).setParent(Context.current()).startSpan();
        span.setAttribute(stringKey("start-thread-name"), Thread.currentThread().getName());
        span.setAttribute(longKey("start-thread-id"), Thread.currentThread().getId());
        return span;
    }

    /**
     * Lazy initialization of all TaskEventListener.
     */
    public static class TaskEventListeners {
        static volatile List<TaskEventListener> INSTANCE;
        public static List<TaskEventListener> getInstance(List<TaskEventListener> otelEventListenerList) {
            if (INSTANCE == null) {
                synchronized (TaskEventListener.class) {
                    if (INSTANCE == null) {
                        INSTANCE = otelEventListenerList;
                        INSTANCE.addAll(DEFAULT_TASK_EVENT_LISTENERS);
                    }
                }
            }
            return INSTANCE;
        }
    }

    protected static void callTaskEventListeners(boolean startEvent, String operationName, String eventName, Thread t,
                                               List<TaskEventListener> taskEventListeners) {
        if (Context.current() != Context.root()) {
            try (Scope ignored = Span.current().makeCurrent()) {
                if (taskEventListeners != null && !taskEventListeners.isEmpty()) {
                    for (TaskEventListener eventListener : taskEventListeners) {
                        if (eventListener.isApplicable(operationName, eventName)) {
                            if (startEvent) {
                                eventListener.onStart(operationName, eventName, t);
                            } else {
                                eventListener.onEnd(operationName, eventName, t);
                            }
                        }
                    }
                }
            }
        }
    }
}
