/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.telemetry.metrics.exporter.OTelMetricsExporterFactory;
import org.opensearch.telemetry.tracing.exporter.OTelSpanExporterFactory;
import org.opensearch.telemetry.tracing.sampler.ProbabilisticSampler;
import org.opensearch.telemetry.tracing.sampler.RequestSampler;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

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
     * @param telemetrySettings telemetry settings
     * @param settings cluster settings
     * @return OpenTelemetrySdk instance
     */
    public static Optional<OpenTelemetrySdk> get(TelemetrySettings telemetrySettings, Settings settings) {
        return AccessController.doPrivileged((PrivilegedAction<Optional<OpenTelemetrySdk>>) () -> {
            if (TelemetrySettings.TRACER_FEATURE_ENABLED_SETTING.get(settings)
                || TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.get(settings)) {
                Resource resource = Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "OpenSearch"));
                OpenTelemetrySdkBuilder builder = OpenTelemetrySdk.builder();
                if (TelemetrySettings.TRACER_FEATURE_ENABLED_SETTING.get(settings)) {
                    builder.setTracerProvider(createSdkTracerProvider(telemetrySettings, settings, resource))
                        .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()));
                }
                if (TelemetrySettings.METRICS_FEATURE_ENABLED_SETTING.get(settings)) {
                    builder.setMeterProvider(createSdkMetricProvider(settings, resource));
                }
                return Optional.of(builder.buildAndRegisterGlobal());
            } else {
                return Optional.empty();
            }
        });
    }

    private static SdkMeterProvider createSdkMetricProvider(Settings settings, Resource resource) {
        return SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.builder(OTelMetricsExporterFactory.create(settings))
                    .setInterval(TelemetrySettings.METRICS_PUBLISH_INTERVAL_SETTING.get(settings).getSeconds(), TimeUnit.SECONDS)
                    .build()
            )
            .build();
    }

    private static SdkTracerProvider createSdkTracerProvider(TelemetrySettings telemetrySettings, Settings settings, Resource resource) {
        return SdkTracerProvider.builder()
            .addSpanProcessor(spanProcessor(settings, OTelSpanExporterFactory.create(settings)))
            .setResource(resource)
            .setSampler(Sampler.parentBased(new RequestSampler(new ProbabilisticSampler(telemetrySettings))))
            .build();
    }

    private static BatchSpanProcessor spanProcessor(Settings settings, SpanExporter spanExporter) {
        return BatchSpanProcessor.builder(spanExporter)
            .setScheduleDelay(TRACER_EXPORTER_DELAY_SETTING.get(settings).getSeconds(), TimeUnit.SECONDS)
            .setMaxExportBatchSize(TRACER_EXPORTER_BATCH_SIZE_SETTING.get(settings))
            .setMaxQueueSize(TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING.get(settings))
            .build();
    }

}
