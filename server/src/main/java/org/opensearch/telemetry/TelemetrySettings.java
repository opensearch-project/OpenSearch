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

import java.util.HashMap;
import java.util.Map;

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

    public static final Setting<Boolean> TRACER_FEATURE_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.feature.tracer.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
    );

    public static final Setting<Boolean> METRICS_FEATURE_ENABLED_SETTING = Setting.boolSetting(
        "telemetry.feature.metrics.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Final
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

    /**
     * Probability of action based sampler
     */
    public static final Setting.AffixSetting<Double> TRACER_SAMPLER_ACTION_PROBABILITY = Setting.affixKeySetting(
        "telemetry.tracer.action.sampler.",
        "probability",
        (ns, key) -> Setting.doubleSetting(key, 0.00d, 0.00d, 1.00d, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    private volatile boolean tracingEnabled;
    private volatile double samplingProbability;

    private final boolean tracingFeatureEnabled;
    private final boolean metricsFeatureEnabled;
    private volatile Map<String, Double> affixSamplingProbability;

    public TelemetrySettings(Settings settings, ClusterSettings clusterSettings) {
        this.tracingEnabled = TRACER_ENABLED_SETTING.get(settings);
        this.samplingProbability = TRACER_SAMPLER_PROBABILITY.get(settings);
        this.tracingFeatureEnabled = TRACER_FEATURE_ENABLED_SETTING.get(settings);
        this.metricsFeatureEnabled = METRICS_FEATURE_ENABLED_SETTING.get(settings);
        this.affixSamplingProbability = new HashMap<>();

        clusterSettings.addSettingsUpdateConsumer(TRACER_ENABLED_SETTING, this::setTracingEnabled);
        clusterSettings.addSettingsUpdateConsumer(TRACER_SAMPLER_PROBABILITY, this::setSamplingProbability);
        clusterSettings.addAffixMapUpdateConsumer(TRACER_SAMPLER_ACTION_PROBABILITY, this::setActionSamplingProbability, (a, b) -> {});
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
     * Set sampling ratio for action
     * @param filters map of action and corresponding value
     */
    public void setActionSamplingProbability(Map<String, Double> filters) {
        synchronized (this) {
            for (String name : filters.keySet()) {
                this.affixSamplingProbability.put(name, filters.get(name));
            }
        }
    }

    /**
     * Get sampling ratio
     * @return double
     */
    public double getSamplingProbability() {
        return samplingProbability;
    }

    public boolean isTracingFeatureEnabled() {
        return tracingFeatureEnabled;
    }

    public boolean isMetricsFeatureEnabled() {
        return metricsFeatureEnabled;
    }

    /**
     * Returns if sampling override is set for action
     * @param action string
     * @return boolean if sampling override is present for an action
     */
    public boolean isActionSamplingOverrideSet(String action) {
        return affixSamplingProbability.containsKey(action);
    }

    /**
     * Get action sampling ratio
     * @param action string
     * @return double value of sampling probability for that action
     */
    public double getActionSamplingProbability(String action) {
        return this.affixSamplingProbability.get(action);
    }
}
