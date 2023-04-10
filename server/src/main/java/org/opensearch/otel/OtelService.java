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
import org.opensearch.action.support.OTelContextPreservingActionListener;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.stream.Collectors;

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

//        ManagedChannel jaegerChannel = ManagedChannelBuilder.forAddress("localhost", 3336)
//            .usePlaintext()
//            .build();

//        JaegerGrpcSpanExporter jaegerExporter = JaegerGrpcSpanExporter.builder()
//            .setEndpoint("http://localhost:3336")
//            .setTimeout(30, TimeUnit.SECONDS)
//            .build();

//        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
//            .addSpanProcessor(SimpleSpanProcessor.create(jaegerExporter))
//            .build();
//        Meter recoverMeter = OtelService.sdkMeterProvider.meterBuilder("recover").build();

//        DoubleGaugeBuilder cpuGauge = recoverMeter.gaugeBuilder("cpu");
//        recoverMeter
//            .gaugeBuilder("cpu_usage")
//            .setDescription("CPU Usage")
//            .setUnit("ms")
//            .buildWithCallback(measurement -> {
//                measurement.record(, Attributes.of(stringKey("operation"), "recover-start"));
//            });

//        recoverMeter
//            .gaugeBuilder("cpu_usage")
//            .setDescription("CPU Usage")
//            .setUnit("ms")
//            .buildWithCallback(measurement -> {
//                measurement.record(OtelService.getCPUUsage(threadId), Attributes.of(stringKey("operation"), "recover-start"));
//            });

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

    public OtelService(PluginsService pluginsService) {
        otelEventListenerList = pluginsService.filterPlugins(Plugin.class)
            .stream()
            .map(Plugin::getOtelEventListeners)
            .flatMap(List::stream)
            .collect(Collectors.toList());
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

    public static <Response> ActionListener<Response> startSpan(String spanName, ActionListener<Response> actionListener) {
        Tracer tracer = OtelService.sdkTracerProvider.get("recover");
        Context beforeAttach = Context.current();
        Span span = tracer.spanBuilder(spanName).startSpan();
        span.setAttribute(stringKey("start-thread-name"), Thread.currentThread().getName());
        span.setAttribute(longKey("start-thread-id"), Thread.currentThread().getId());
        span.makeCurrent();
        return new OTelContextPreservingActionListener<>(beforeAttach, Context.current(), actionListener,
                span.getSpanContext().getSpanId());
    }

    public void emitResources(Span span, String name) {
        for (OtelEventListener eventListener : otelEventListenerList) {
            eventListener.onEvent(span);
        }
        span.addEvent(name,
            Attributes.of(
                AttributeKey.longKey("ThreadID"), Thread.currentThread().getId(),
                AttributeKey.stringKey("ThreadName"), Thread.currentThread().getName(),
                AttributeKey.longKey("CPUUsage"), OtelService.getCPUUsage(Thread.currentThread().getId()),
                AttributeKey.longKey("MemoryUsage"), OtelService.getMemoryUsage(Thread.currentThread().getId()),
                AttributeKey.longKey("ContentionTime"), OtelService.getThreadContentionTime(Thread.currentThread().getId())
            )
        );
    }

}
