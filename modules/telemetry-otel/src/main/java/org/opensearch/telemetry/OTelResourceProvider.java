/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

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
import org.opensearch.telemetry.tracing.exporter.FileSpanExporter;

import java.util.concurrent.TimeUnit;

/**
 * This class encapsulates all OpenTelemetry related resources
 */
final class OTelResourceProvider {

    static OpenTelemetry get(TelemetrySettings telemetrySettings) {
        return get(
            telemetrySettings,
            new FileSpanExporter(),
            ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
            Sampler.alwaysOn()
        );
    }

    static OpenTelemetry get(
        TelemetrySettings telemetrySettings,
        SpanExporter spanExporter,
        ContextPropagators contextPropagators,
        Sampler sampler
    ) {
        Resource resource = Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "OpenSearch"));
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(spanProcessor(telemetrySettings, spanExporter))
            .setResource(resource)
            .setSampler(sampler)
            .build();

        return OpenTelemetrySdk.builder().setTracerProvider(sdkTracerProvider).setPropagators(contextPropagators).buildAndRegisterGlobal();
    }

    private static BatchSpanProcessor spanProcessor(TelemetrySettings telemetrySettings, SpanExporter spanExporter) {
        return BatchSpanProcessor.builder(spanExporter)
            .setScheduleDelay(telemetrySettings.getExporterDelay().getSeconds(), TimeUnit.SECONDS)
            .setMaxExportBatchSize(telemetrySettings.getExporterBatchSize())
            .setMaxQueueSize(telemetrySettings.getExporterMaxQueueSize())
            .build();
    }

}
