/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.tracing.OtelTelemetryImpl;
import org.opensearch.telemetry.tracing.OtelTracingTelemetry;

import java.util.Optional;

import static org.opensearch.common.util.FeatureFlags.TELEMETRY;

/**
 * Telemetry plugin based on Otel
 */
public class OTelTelemetryModulePlugin extends Plugin implements TelemetryPlugin {

    static final String OTEL_TRACER_NAME = "otel";

    /**
     * No-args constructor
     */
    public OTelTelemetryModulePlugin() {}

    @Override
    public Settings additionalSettings() {
        if (FeatureFlags.isEnabled(TELEMETRY)) {
            return Settings.builder()
                // set Otel tracer as default telemetry provider
                .put(TelemetryModule.TELEMETRY_DEFAULT_TYPE_SETTING.getKey(), OTEL_TRACER_NAME)
                .build();
        }
        return Settings.EMPTY;
    }

    @Override
    public Optional<Telemetry> getTelemetry(TelemetrySettings telemetrySettings) {
        return Optional.of(telemetry(telemetrySettings));
    }

    @Override
    public String getName() {
        return OTEL_TRACER_NAME;
    }

    private Telemetry telemetry(TelemetrySettings telemetrySettings) {
        return new OtelTelemetryImpl(new OtelTracingTelemetry(OTelResourceProvider.get(telemetrySettings)), new MetricsTelemetry() {
        });
    }

}
