/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.tracing.OTelResourceProvider;
import org.opensearch.telemetry.tracing.OTelTelemetry;
import org.opensearch.telemetry.tracing.OTelTracingTelemetry;

import java.util.Optional;

/**
 * Telemetry plugin based on Otel
 */
public class OTelTelemetryPlugin extends Plugin implements TelemetryPlugin {

    static final String OTEL_TRACER_NAME = "otel";

    private final Settings settings;

    /**
     * Creates Otel plugin
     * @param settings cluster settings
     */
    public OTelTelemetryPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Optional<Telemetry> getTelemetry() {
        return Optional.of(telemetry());
    }

    @Override
    public String getName() {
        return OTEL_TRACER_NAME;
    }

    private Telemetry telemetry() {
        return new OTelTelemetry(new OTelTracingTelemetry(OTelResourceProvider.get(settings)), new MetricsTelemetry() {
        });
    }

}
