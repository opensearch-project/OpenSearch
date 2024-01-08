/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.sampler;

import org.opensearch.telemetry.TelemetrySettings;

import java.util.List;
import java.util.Objects;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

/**
 * ProbabilisticSampler implements a head-based sampling strategy based on provided settings.
 */
public class ProbabilisticSampler implements Sampler {
    private Sampler defaultSampler;
    private final TelemetrySettings telemetrySettings;
    private double samplingRatio;

    /**
     * Constructor
     *
     * @param telemetrySettings Telemetry settings.
     */
    public ProbabilisticSampler(TelemetrySettings telemetrySettings) {
        this.telemetrySettings = Objects.requireNonNull(telemetrySettings);
        this.samplingRatio = telemetrySettings.getSamplingProbability();
        this.defaultSampler = Sampler.traceIdRatioBased(samplingRatio);
    }

    Sampler getSampler() {
        double newSamplingRatio = telemetrySettings.getSamplingProbability();
        if (isSamplingRatioChanged(newSamplingRatio)) {
            synchronized (this) {
                this.samplingRatio = newSamplingRatio;
                defaultSampler = Sampler.traceIdRatioBased(samplingRatio);
            }
        }
        return defaultSampler;
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
        return getSampler().shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
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
