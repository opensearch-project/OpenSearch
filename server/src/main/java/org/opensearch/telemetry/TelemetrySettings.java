/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Wrapper class to encapsulate tracing related settings
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TelemetrySettings {
    public static final Setting<Boolean> TRACER_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.tracer.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<Boolean> METRICS_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.metrics.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Probability of sampler
     */
    public static final Setting<Double> TRACER_SAMPLER_PROBABILITY = Setting.doubleSetting(
        "telemetry.tracer.sampler.probability",
        0.01d,
        0.00d,
        1.00d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * metrics publish interval in seconds.
     */
    public static final Setting<TimeValue> METRICS_PUBLISH_INTERVAL_SETTING = Setting.timeSetting(
        "telemetry.otel.metrics.publish.interval",
        TimeValue.timeValueSeconds(60),
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    private volatile boolean tracingEnabled;
    private volatile double samplingProbability;

    private volatile boolean metricsEnabled;

    public TelemetrySettings(Settings settings, ClusterSettings clusterSettings) {
        this.tracingEnabled = TRACER_ENABLED_SETTING.get(settings);
        this.samplingProbability = TRACER_SAMPLER_PROBABILITY.get(settings);
        this.metricsEnabled = METRICS_ENABLED_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(TRACER_ENABLED_SETTING, this::setTracingEnabled);
        clusterSettings.addSettingsUpdateConsumer(TRACER_SAMPLER_PROBABILITY, this::setSamplingProbability);
        clusterSettings.addSettingsUpdateConsumer(METRICS_ENABLED_SETTING, this::setMetricsEnabled);
    }

    public void setTracingEnabled(boolean tracingEnabled) {
        this.tracingEnabled = tracingEnabled;
    }

    public boolean isTracingEnabled() {
        return tracingEnabled;
    }

    /**
     * Set sampling ratio
     * @param samplingProbability double
     */
    public void setSamplingProbability(double samplingProbability) {
        this.samplingProbability = samplingProbability;
    }

    /**
     * Get sampling ratio
     */
    public double getSamplingProbability() {
        return samplingProbability;
    }

    /**
     * update the metrics enabled property.
     * @param metricsEnabled metrics enabled.
     */
    public void setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }

    /**
     * Returns whether metrics are enabled or not.
     * @return enabled/disabled flag.
     */
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

}
