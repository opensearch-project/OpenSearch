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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import static org.opensearch.telemetry.tracing.AttributeNames.TRANSPORT_ACTION;

/**
 * ProbabilisticTransportActionSampler sampler samples request with action based on defined probability
 */
public class ProbabilisticTransportActionSampler implements Sampler {
    private Sampler actionSampler;
    private final TelemetrySettings telemetrySettings;
    private double actionSamplingRatio;

    /**
     * Creates ProbabilisticTransportActionSampler sampler
     * @param telemetrySettings TelemetrySettings
     */
    public ProbabilisticTransportActionSampler(TelemetrySettings telemetrySettings) {
        this.telemetrySettings = Objects.requireNonNull(telemetrySettings);
        this.actionSamplingRatio = telemetrySettings.getActionSamplingProbability();
        this.actionSampler = Sampler.traceIdRatioBased(actionSamplingRatio);
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
        final String action = attributes.get(AttributeKey.stringKey(TRANSPORT_ACTION));
        if (action != null) {
            double newActionSamplingRatio = telemetrySettings.getActionSamplingProbability();
            if (isActionSamplingRatioChanged(newActionSamplingRatio)) {
                synchronized (this) {
                    this.actionSamplingRatio = newActionSamplingRatio;
                    actionSampler = Sampler.traceIdRatioBased(actionSamplingRatio);
                }
            }
            return actionSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
        }
        return SamplingResult.drop();
    }

    private boolean isActionSamplingRatioChanged(double newSamplingRatio) {
        return Double.compare(this.actionSamplingRatio, newSamplingRatio) != 0;
    }

    double getSamplingRatio() {
        return actionSamplingRatio;
    }

    @Override
    public String getDescription() {
        return "Transport Action Sampler";
    }

    @Override
    public String toString() {
        return getDescription();
    }
}
