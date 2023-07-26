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
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.opensearch.common.settings.Settings;

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
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(spanProcessor(settings, spanExporter))
            .setResource(resource)
            .setSampler(sampler)
            .build();

        return OpenTelemetrySdk.builder().setTracerProvider(sdkTracerProvider).setPropagators(contextPropagators).buildAndRegisterGlobal();
    }

    private static BatchSpanProcessor spanProcessor(Settings settings, SpanExporter spanExporter) {
        return BatchSpanProcessor.builder(spanExporter)
            .setScheduleDelay(TRACER_EXPORTER_DELAY_SETTING.get(settings).getSeconds(), TimeUnit.SECONDS)
            .setMaxExportBatchSize(TRACER_EXPORTER_BATCH_SIZE_SETTING.get(settings))
            .setMaxQueueSize(TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING.get(settings))
            .build();
    }

}
