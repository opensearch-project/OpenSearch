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
import org.opensearch.common.SetOnce;
import org.opensearch.telemetry.tracing.exporter.FileSpanExporter;

import java.util.concurrent.TimeUnit;

/**
 * This class encapsulates all OpenTelemetry related resources
 */
final class OTelResourceProvider {

    private final static SetOnce<OpenTelemetry> openTelemetry = new SetOnce<>();

    static OpenTelemetry get(TracerSettings tracerSettings) {
        return get(
            tracerSettings,
            new FileSpanExporter(),
            ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
            Sampler.alwaysOn()
        );
    }

    static OpenTelemetry get(
        TracerSettings tracerSettings,
        SpanExporter spanExporter,
        ContextPropagators contextPropagators,
        Sampler sampler
    ) {
        if (openTelemetry.get() == null) {
            Resource resource = Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "OpenSearch"));
            synchronized (openTelemetry) {
                if (openTelemetry.get() == null) {
                    SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                        .addSpanProcessor(spanProcessor(tracerSettings, spanExporter))
                        .setResource(resource)
                        .setSampler(sampler)
                        .build();

                    openTelemetry.set(
                        OpenTelemetrySdk.builder()
                            .setTracerProvider(sdkTracerProvider)
                            .setPropagators(contextPropagators)
                            .buildAndRegisterGlobal()
                    );
                }
            }
        }
        return openTelemetry.get();
    }

    private static BatchSpanProcessor spanProcessor(TracerSettings tracerSettings, SpanExporter spanExporter) {
        return BatchSpanProcessor.builder(spanExporter)
            .setScheduleDelay(tracerSettings.getExporterDelay().getSeconds(), TimeUnit.SECONDS)
            .setMaxExportBatchSize(tracerSettings.getExporterBatchSize())
            .setMaxQueueSize(tracerSettings.getExporterMaxQueueSize())
            .build();
    }

}
