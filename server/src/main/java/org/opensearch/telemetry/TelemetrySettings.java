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

    private volatile boolean tracingEnabled;
    private volatile double samplingProbability;

    public TelemetrySettings(Settings settings, ClusterSettings clusterSettings) {
        this.tracingEnabled = TRACER_ENABLED_SETTING.get(settings);
        this.samplingProbability = TRACER_SAMPLER_PROBABILITY.get(settings);

        clusterSettings.addSettingsUpdateConsumer(TRACER_ENABLED_SETTING, this::setTracingEnabled);
        clusterSettings.addSettingsUpdateConsumer(TRACER_SAMPLER_PROBABILITY, this::setSamplingProbability);
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
}
