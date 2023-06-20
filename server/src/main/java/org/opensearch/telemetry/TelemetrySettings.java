/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Wrapper class to encapsulate tracing related settings
 */
public class TelemetrySettings {
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
    public static final Setting<Boolean> TRACER_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.tracer.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile boolean tracingEnabled;

    private volatile int exporterBatchSize;

    private volatile int exporterMaxQueueSize;

    private volatile TimeValue exporterDelay;

    public TelemetrySettings(Settings settings, ClusterSettings clusterSettings) {
        this.tracingEnabled = TRACER_ENABLED_SETTING.get(settings);
        this.exporterBatchSize = TRACER_EXPORTER_BATCH_SIZE_SETTING.get(settings);
        this.exporterMaxQueueSize = TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING.get(settings);
        this.exporterDelay = TRACER_EXPORTER_DELAY_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(TRACER_ENABLED_SETTING, this::setTracingEnabled);
        clusterSettings.addSettingsUpdateConsumer(TRACER_EXPORTER_BATCH_SIZE_SETTING, this::setExporterBatchSize);
        clusterSettings.addSettingsUpdateConsumer(TRACER_EXPORTER_MAX_QUEUE_SIZE_SETTING, this::setExporterMaxQueueSize);
        clusterSettings.addSettingsUpdateConsumer(TRACER_EXPORTER_DELAY_SETTING, this::setExporterDelay);
    }

    public void setTracingEnabled(boolean tracingEnabled) {
        this.tracingEnabled = tracingEnabled;
    }

    public void setExporterBatchSize(int exporterBatchSize) {
        this.exporterBatchSize = exporterBatchSize;
    }

    public void setExporterMaxQueueSize(int exporterMaxQueueSize) {
        this.exporterMaxQueueSize = exporterMaxQueueSize;
    }

    public void setExporterDelay(TimeValue exporterDelay) {
        this.exporterDelay = exporterDelay;
    }

    public boolean isTracingEnabled() {
        return tracingEnabled;
    }

    public int getExporterBatchSize() {
        return exporterBatchSize;
    }

    public int getExporterMaxQueueSize() {
        return exporterMaxQueueSize;
    }

    public TimeValue getExporterDelay() {
        return exporterDelay;
    }
}
