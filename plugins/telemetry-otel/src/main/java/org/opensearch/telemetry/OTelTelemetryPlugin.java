/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.concurrent.RefCountedReleasable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.tracing.OTelResourceProvider;
import org.opensearch.telemetry.tracing.OTelTelemetry;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.opentelemetry.sdk.OpenTelemetrySdk;

/**
 * Telemetry plugin based on Otel
 */
public class OTelTelemetryPlugin extends Plugin implements TelemetryPlugin {

    /**
     * Instrumentation scope name.
     */
    public static final String INSTRUMENTATION_SCOPE_NAME = "org.opensearch.telemetry";

    static final String OTEL_TRACER_NAME = "otel";

    private final Settings settings;

    private RefCountedReleasable<OpenTelemetrySdk> refCountedOpenTelemetry;

    /**
     * Creates Otel plugin
     * @param settings cluster settings
     */
    public OTelTelemetryPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            OTelTelemetrySettings.TRACER_EXPORTER_BATCH_SIZE_SETTING,
            OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING,
            OTelTelemetrySettings.TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING,
            OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING,
            OTelTelemetrySettings.OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS,
            OTelTelemetrySettings.OTEL_METRICS_EXPORTER_CLASS_SETTING,
            OTelTelemetrySettings.TRACER_SAMPLER_ACTION_PROBABILITY
        );
    }

    @Override
    public Optional<Telemetry> getTelemetry(TelemetrySettings telemetrySettings) {
        initializeOpenTelemetrySdk(telemetrySettings);
        return Optional.of(telemetry(telemetrySettings));
    }

    private void initializeOpenTelemetrySdk(TelemetrySettings telemetrySettings) {
        if (refCountedOpenTelemetry != null) {
            return;
        }
        OpenTelemetrySdk openTelemetrySdk = OTelResourceProvider.get(telemetrySettings, settings);
        refCountedOpenTelemetry = new RefCountedReleasable<>("openTelemetry", openTelemetrySdk, openTelemetrySdk::close);
    }

    @Override
    public String getName() {
        return OTEL_TRACER_NAME;
    }

    private Telemetry telemetry(TelemetrySettings telemetrySettings) {
        return new OTelTelemetry(refCountedOpenTelemetry);
    }

    @Override
    public void close() {
        if (refCountedOpenTelemetry != null) {
            refCountedOpenTelemetry.close();
        }
    }

}
