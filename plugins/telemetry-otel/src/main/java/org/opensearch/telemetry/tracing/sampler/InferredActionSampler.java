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
import org.opensearch.telemetry.tracing.TracerContextStorage;

import java.util.List;
import java.util.Objects;

import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

/**
 * InferredActionSampler implements a probability sampling strategy with sampling ratio as 1.0.
 */
public class InferredActionSampler implements Sampler {

    private final Sampler fallbackSampler;
    private final Sampler actionSampler;
    private final TelemetrySettings telemetrySettings;
    private final Settings settings;
    private final double samplingRatio;

    /**
     * Constructor
     *  @param telemetrySettings the telemetry settings
     * @param settings          the settings
     * @param fallbackSampler   the fallback sampler
     */
    private InferredActionSampler(TelemetrySettings telemetrySettings, Settings settings, Sampler fallbackSampler) {
        this.telemetrySettings = Objects.requireNonNull(telemetrySettings);
        this.settings = Objects.requireNonNull(settings);
        this.samplingRatio = 1.0;
        this.actionSampler = Sampler.traceIdRatioBased(samplingRatio);
        this.fallbackSampler = fallbackSampler;
    }

    /**
     * Create Inferred sampler.
     *
     * @param telemetrySettings the telemetry settings
     * @param settings          the settings
     * @param fallbackSampler   the fallback sampler
     * @return the inferred sampler
     */
    public static Sampler create(TelemetrySettings telemetrySettings, Settings settings, Sampler fallbackSampler) {
        return new InferredActionSampler(telemetrySettings, settings, fallbackSampler);
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
        boolean inferredSamplingAllowListed = telemetrySettings.getInferredSamplingAllowListed();
        if (inferredSamplingAllowListed) {
            // Using baggage to store the common sampler attribute for context propagation
            Baggage.fromContext(parentContext)
                .toBuilder()
                .put(TracerContextStorage.INFERRED_SAMPLER, "true")
                .build()
                .storeInContext(parentContext)
                .makeCurrent();
            return actionSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
        } else {
            if (fallbackSampler != null) {
                return fallbackSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
            }
        }
        return SamplingResult.drop();
    }

    @Override
    public String getDescription() {
        return "Inferred Action Sampler";
    }
}
