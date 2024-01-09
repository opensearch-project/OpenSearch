/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.sampler;

import org.opensearch.telemetry.TelemetrySettings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import static org.opensearch.telemetry.tracing.AttributeNames.TRANSPORT_ACTION;

/**
 * ProbabilisticSampler implements a head-based sampling strategy based on provided settings.
 */
public class ProbabilisticSampler implements Sampler {
    private Sampler defaultSampler;
    private final TelemetrySettings telemetrySettings;
    private double samplingRatio;

    private final Map<String, Sampler> actionBasedSampler;
    private final Map<String, Double> actionBasedSamplingProbability;

    /**
     * Constructor
     *
     * @param telemetrySettings Telemetry settings.
     */
    public ProbabilisticSampler(TelemetrySettings telemetrySettings) {
        this.telemetrySettings = Objects.requireNonNull(telemetrySettings);
        this.samplingRatio = telemetrySettings.getSamplingProbability();
        this.defaultSampler = Sampler.traceIdRatioBased(samplingRatio);
        this.actionBasedSampler = new HashMap<>();
        this.actionBasedSamplingProbability = new HashMap<>();
    }

    /**
     * Returns custom sampler based on the type of request
     * @param attributes Telemetry attributes
     */
    Sampler getSampler(Attributes attributes) {
        // Evaluate if sampling is overridden at action level
        final String action = attributes.get(AttributeKey.stringKey(TRANSPORT_ACTION));
        if (action != null && telemetrySettings.isActionSamplingOverrideSet(action)) {
            if (isActionSamplerUpdateRequired(action)) {
                synchronized (this) {
                    updateActionSampler(action);
                }
            }
            return actionBasedSampler.get(action);
        } else {
            // Update default sampling if no override is present for action
            double newSamplingRatio = telemetrySettings.getSamplingProbability();
            if (isSamplingRatioChanged(newSamplingRatio)) {
                synchronized (this) {
                    this.samplingRatio = newSamplingRatio;
                    defaultSampler = Sampler.traceIdRatioBased(samplingRatio);
                }
            }
            return defaultSampler;
        }
    }

    private boolean isSamplingRatioChanged(double newSamplingRatio) {
        return Double.compare(this.samplingRatio, newSamplingRatio) != 0;
    }

    private boolean isActionSamplerUpdateRequired(String action) {
        return (!actionBasedSampler.containsKey(action)
            || (actionBasedSamplingProbability.get(action) != telemetrySettings.getActionSamplingProbability(action)));
    }

    private void updateActionSampler(String action) {
        double samplingRatio = telemetrySettings.getActionSamplingProbability(action);
        this.actionBasedSamplingProbability.put(action, samplingRatio);
        this.actionBasedSampler.put(action, Sampler.traceIdRatioBased(samplingRatio));
    }

    double getActionSamplingRatio(String action) {
        return actionBasedSamplingProbability.get(action);
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
        // Use action sampler only if it is root span & override is present for the action
        SpanContext parentSpanContext = Span.fromContext(parentContext).getSpanContext();
        if (!parentSpanContext.isValid()) {
            return getSampler(attributes).shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
        }
        return defaultSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
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
