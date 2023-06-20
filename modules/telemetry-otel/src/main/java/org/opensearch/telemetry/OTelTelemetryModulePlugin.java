/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.tracing.OtelTelemetryImpl;
import org.opensearch.telemetry.tracing.OtelTracingTelemetry;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.opensearch.common.util.FeatureFlags.TELEMETRY;

/**
 * Telemetry plugin based on Otel
 */
public class OTelTelemetryModulePlugin extends Plugin implements TelemetryPlugin {

    static final String OTEL_TRACER_NAME = "otel";

    public static final Setting<Integer> TRACER_EXPORTER_BATCH_SIZE_SETTING = Setting.intSetting(
        "telemetry.tracer.exporter.batch_size",
        512,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING = Setting.intSetting(
        "telemetry.tracer.exporter.max_queue_size",
        2048,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> TRACER_EXPORTER_DELAY_SETTING = Setting.timeSetting(
        "telemetry.tracer.exporter.delay",
        TimeValue.timeValueSeconds(2),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * No-args constructor
     */
    public OTelTelemetryModulePlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(TRACER_EXPORTER_BATCH_SIZE_SETTING, TRACER_EXPORTER_DELAY_SETTING, TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING);
    }

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
    public Optional<Telemetry> getTelemetry(Settings settings) {
        return Optional.of(telemetry(settings));
    }

    @Override
    public String getName() {
        return OTEL_TRACER_NAME;
    }

    private Telemetry telemetry(Settings settings) {
        return new OtelTelemetryImpl(new OtelTracingTelemetry(OTelResourceProvider.get(settings)), new MetricsTelemetry() {
        });
    }

}
