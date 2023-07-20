/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;

import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.opensearch.common.settings.Settings;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.TimeUnit;
import org.opensearch.telemetry.tracing.exporter.OTelSpanExporterFactory;

import static org.opensearch.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_BATCH_SIZE_SETTING;
import static org.opensearch.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING;
import static org.opensearch.telemetry.OTelTelemetrySettings.TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING;

/**
 * This class encapsulates all OpenTelemetry related resources
 */
public final class OTelResourceProvider {
    private OTelResourceProvider() {}

    /**
     * Creates OpenTelemetry instance with default configuration
     * @param settings cluster settings
     * @return OpenTelemetry instance
     */
    public static OpenTelemetry get(Settings settings) {
        return get(
            settings,
            OTelSpanExporterFactory.create(settings),
            ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
            Sampler.alwaysOn()
        );
    }

    /**
     * Creates OpenTelemetry instance with provided configuration
     * @param settings cluster settings
     * @param spanExporter span exporter instance
     * @param contextPropagators context propagator instance
     * @param sampler sampler instance
     * @return Opentelemetry instance
     */
    public static OpenTelemetry get(Settings settings, SpanExporter spanExporter, ContextPropagators contextPropagators, Sampler sampler) {
        Resource resource = Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "OpenSearch"));
        SdkTracerProvider sdkTracerProvider = null;
        // TODO - change to default metricExporter
        try {
            SpanExporter spanExporter1 = AccessController.doPrivileged((PrivilegedExceptionAction<OtlpGrpcSpanExporter>) () ->
                OtlpGrpcSpanExporter.builder().setEndpoint("http://localhost:4317").build());
            sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor(settings, spanExporter1))
                .setResource(resource)
                .setSampler(sampler)
                .build();
            OtlpGrpcMetricExporter metricExporter = AccessController.doPrivileged(
                (PrivilegedExceptionAction<OtlpGrpcMetricExporter>) () ->
                    OtlpGrpcMetricExporter.builder().setEndpoint("http://localhost:4317") // Replace with the actual endpoint
                    .build()
            );
            SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
                .registerMetricReader(PeriodicMetricReader.builder(metricExporter).build())
                .setResource(resource)
                .build();

            return OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(contextPropagators)
                .setMeterProvider(sdkMeterProvider)
                .buildAndRegisterGlobal();
        } catch (Throwable e) {
            return OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(contextPropagators)
                .buildAndRegisterGlobal();
        }
    }

    private static BatchSpanProcessor spanProcessor(Settings settings, SpanExporter spanExporter) {
        return BatchSpanProcessor.builder(spanExporter)
            .setScheduleDelay(TRACER_EXPORTER_DELAY_SETTING.get(settings).getSeconds(), TimeUnit.SECONDS)
            .setMaxExportBatchSize(TRACER_EXPORTER_BATCH_SIZE_SETTING.get(settings))
            .setMaxQueueSize(TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING.get(settings))
            .build();
    }
}
