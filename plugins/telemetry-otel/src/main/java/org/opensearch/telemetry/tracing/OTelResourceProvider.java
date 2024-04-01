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
import org.opensearch.telemetry.tracing.processor.OTelSpanProcessor;
import org.opensearch.telemetry.tracing.sampler.OTelSamplerFactory;
import org.opensearch.telemetry.tracing.sampler.RequestSampler;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.metrics.internal.view.Base2ExponentialHistogramAggregation;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.ResourceAttributes;

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
    @SuppressWarnings("removal")
    public static OpenTelemetrySdk get(TelemetrySettings telemetrySettings, Settings settings) {
        return AccessController.doPrivileged(
            (PrivilegedAction<OpenTelemetrySdk>) () -> get(
                settings,
                OTelSpanExporterFactory.create(settings),
                ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
                Sampler.parentBased(new RequestSampler(OTelSamplerFactory.create(telemetrySettings, settings)))
            )
        );
    }

    /**
     * Creates OpenTelemetry instance with provided configuration
     * @param settings cluster settings
     * @param spanExporter span exporter instance
     * @param contextPropagators context propagator instance
     * @param sampler sampler instance
     * @return OpenTelemetrySdk instance
     */
    public static OpenTelemetrySdk get(
        Settings settings,
        SpanExporter spanExporter,
        ContextPropagators contextPropagators,
        Sampler sampler
    ) {
        Resource resource = Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "OpenSearch"));
        SdkTracerProvider sdkTracerProvider = createSdkTracerProvider(settings, spanExporter, sampler, resource);
        SdkMeterProvider sdkMeterProvider = createSdkMetricProvider(settings, resource);
        return OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setMeterProvider(sdkMeterProvider)
            .setPropagators(contextPropagators)
            .buildAndRegisterGlobal();
    }

    private static SdkMeterProvider createSdkMetricProvider(Settings settings, Resource resource) {
        return SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.builder(OTelMetricsExporterFactory.create(settings))
                    .setInterval(TelemetrySettings.METRICS_PUBLISH_INTERVAL_SETTING.get(settings).getSeconds(), TimeUnit.SECONDS)
                    .build()
            )
            .registerView(
                InstrumentSelector.builder().setType(InstrumentType.HISTOGRAM).build(),
                View.builder().setAggregation(Base2ExponentialHistogramAggregation.getDefault()).build()
            )
            .build();
    }

    private static SdkTracerProvider createSdkTracerProvider(
        Settings settings,
        SpanExporter spanExporter,
        Sampler sampler,
        Resource resource
    ) {
        return SdkTracerProvider.builder()
            .addSpanProcessor(spanProcessor(settings, spanExporter))
            .setResource(resource)
            .setSampler(sampler)
            .build();
    }

    private static OTelSpanProcessor spanProcessor(Settings settings, SpanExporter spanExporter) {
        return new OTelSpanProcessor(batchSpanProcessor(settings, spanExporter));
    }

    private static BatchSpanProcessor batchSpanProcessor(Settings settings, SpanExporter spanExporter) {
        return BatchSpanProcessor.builder(spanExporter)
            .setScheduleDelay(TRACER_EXPORTER_DELAY_SETTING.get(settings).getSeconds(), TimeUnit.SECONDS)
            .setMaxExportBatchSize(TRACER_EXPORTER_BATCH_SIZE_SETTING.get(settings))
            .setMaxQueueSize(TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING.get(settings))
            .build();
    }

}
