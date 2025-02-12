/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.sampler;

import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.TelemetrySettings;

import java.util.List;
import java.util.Objects;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

/**
 * ProbabilisticSampler implements a probability sampling strategy based on configured sampling ratio.
 */
public class ProbabilisticSampler implements Sampler {
    private Sampler defaultSampler;
    private final TelemetrySettings telemetrySettings;
    private final Settings settings;
    private final Sampler fallbackSampler;

    private double samplingRatio;

    /**
     * Constructor
     *
     * @param telemetrySettings Telemetry settings.
     */
    private ProbabilisticSampler(TelemetrySettings telemetrySettings, Settings settings, Sampler fallbackSampler) {
        this.telemetrySettings = Objects.requireNonNull(telemetrySettings);
        this.settings = Objects.requireNonNull(settings);
        this.samplingRatio = telemetrySettings.getSamplingProbability();
        this.defaultSampler = Sampler.traceIdRatioBased(samplingRatio);
        this.fallbackSampler = fallbackSampler;
    }

    /**
     * Create probabilistic sampler.
     *
     * @param telemetrySettings the telemetry settings
     * @param settings          the settings
     * @param fallbackSampler   the fallback sampler
     * @return the probabilistic sampler
     */
    public static Sampler create(TelemetrySettings telemetrySettings, Settings settings, Sampler fallbackSampler) {
        return new ProbabilisticSampler(telemetrySettings, settings, fallbackSampler);
    }

    private boolean isSamplingRatioChanged(double newSamplingRatio) {
        return Double.compare(this.samplingRatio, newSamplingRatio) != 0;
    }

    double getSamplingRatio() {
        return samplingRatio;
    }

    @Override
    public SamplingResult shouldSample(
        Context parentContext,
        String traceId,
        String name,
        SpanKind spanKind,
        Attributes attributes,
        List<LinkData> parentLinks
    ) {
        double newSamplingRatio = telemetrySettings.getSamplingProbability();
        if (isSamplingRatioChanged(newSamplingRatio)) {
            synchronized (this) {
                this.samplingRatio = newSamplingRatio;
                defaultSampler = Sampler.traceIdRatioBased(samplingRatio);
            }
        }
        final SamplingResult result = defaultSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
        if (result.getDecision() != SamplingDecision.DROP && fallbackSampler != null) {
            return fallbackSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
        } else {
            return result;
        }
    }

    @Override
    public String getDescription() {
        return "Probabilistic Sampler";
    }

    @Override
    public String toString() {
        return getDescription();
    }
}
