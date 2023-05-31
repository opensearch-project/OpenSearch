/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import io.opentelemetry.api.OpenTelemetry;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TracerPlugin;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.opensearch.common.util.FeatureFlags.TRACER;

/**
 * Tracer plugin based on Otel
 */
public class OTelTracerModulePlugin extends Plugin implements TracerPlugin {

    static final String OTEL_TRACER_NAME = "otel";

    /**
     * No-args constructor
     */
    public OTelTracerModulePlugin() {}

    @Override
    public Settings additionalSettings() {
        if (FeatureFlags.isEnabled(TRACER)) {
            return Settings.builder()
                // set Otel tracer as default tracer
                .put(TracerModule.TRACER_DEFAULT_TYPE_SETTING.getKey(), OTEL_TRACER_NAME)
                .build();
        }
        return Settings.EMPTY;
    }

    @Override
    public Map<String, Supplier<Telemetry>> getTelemetries(TracerSettings tracerSettings) {
        return Collections.singletonMap(OTEL_TRACER_NAME, () -> getTelemetry(tracerSettings));
    }

    private Telemetry getTelemetry(TracerSettings tracerSettings) {
        OpenTelemetry openTelemetry = OTelResourceProvider.getOrCreateOpenTelemetryInstance(tracerSettings);
        return new OtelTelemetry(openTelemetry);
    }
}
