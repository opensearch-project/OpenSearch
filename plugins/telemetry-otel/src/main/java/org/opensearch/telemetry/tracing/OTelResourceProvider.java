/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.Settings;

import java.util.Map;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

/**
 * This class encapsulates all OpenTelemetry related resources
 */
public final class OTelResourceProvider {
    private OTelResourceProvider() {}

    /**
     * Creates OpenTelemetry instance with AutoConfiguredOpenTelemetrySdk settings configuration
     * @param settings cluster settings
     * @return OpenTelemetry instance
     */
    public static OpenTelemetry get(Settings settings) {
        OpenTelemetry openTelemetry = AutoConfiguredOpenTelemetrySdk.builder()
            .setResultAsGlobal(false)
            .addPropertiesSupplier(() -> Map.of("otel.logs.exporter", "none", "otel.metrics.exporter", "none"))
            .addResourceCustomizer(
                (resource, config) -> resource.merge(Resource.builder().put(ResourceAttributes.SERVICE_NAME, "OpenSearch").build())
            )
            .build()
            .getOpenTelemetrySdk();
        return openTelemetry;
    }
}
