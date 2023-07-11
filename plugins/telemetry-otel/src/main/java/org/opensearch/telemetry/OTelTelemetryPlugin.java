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
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryPlugin;
import org.opensearch.telemetry.metrics.MetricsTelemetry;
import org.opensearch.telemetry.tracing.OTelResourceProvider;
import org.opensearch.telemetry.tracing.OTelTelemetry;
import org.opensearch.telemetry.tracing.OTelTracingTelemetry;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.opensearch.telemetry.tracing.exporter.SpanExporterFactory;

/**
 * Telemetry plugin based on Otel
 */
public class OTelTelemetryPlugin extends Plugin implements TelemetryPlugin {

    static final String OTEL_TRACER_NAME = "otel";

    /**
     * span exporter batch size
     */
    public static final Setting<Integer> TRACER_EXPORTER_BATCH_SIZE_SETTING = Setting.intSetting(
        "telemetry.otel.tracer.exporter.batch_size",
        512,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
    /**
     * span exporter max queue size
     */
    public static final Setting<Integer> TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING = Setting.intSetting(
        "telemetry.otel.tracer.exporter.max_queue_size",
        2048,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );
    /**
     * span exporter delay in seconds
     */
    public static final Setting<TimeValue> TRACER_EXPORTER_DELAY_SETTING = Setting.timeSetting(
        "telemetry.otel.tracer.exporter.delay",
        TimeValue.timeValueSeconds(60),
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    private final Settings settings;

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
            TRACER_EXPORTER_BATCH_SIZE_SETTING,
            TRACER_EXPORTER_DELAY_SETTING,
            TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING,
            SpanExporterFactory.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING
        );
    }

    @Override
    public Optional<Telemetry> getTelemetry(TelemetrySettings settings) {
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
