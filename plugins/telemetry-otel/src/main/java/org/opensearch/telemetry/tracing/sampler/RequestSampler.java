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

import java.util.HashMap;
import java.util.List;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import static org.opensearch.telemetry.OTelTelemetrySettings.OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS;
import static org.opensearch.telemetry.tracing.AttributeNames.TRACE;

/**
 * RequestSampler based on HeadBased sampler
 */
public class RequestSampler implements Sampler {
    private final HashMap<String, Sampler> samplers;
    private final List<String> samplerList;

    /**
     * Creates request sampler which applies based on all applicable sampler
     * @param telemetrySettings TelemetrySettings
     * @param setting Settings
     */
    public RequestSampler(TelemetrySettings telemetrySettings, Settings setting) {
        this.samplers = OTelSamplerFactory.create(telemetrySettings, setting);
        this.samplerList = OTEL_TRACER_SPAN_SAMPLER_CLASS_SETTINGS.get(setting);
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
        final String trace = attributes.get(AttributeKey.stringKey(TRACE));

        if (trace != null) {
            return (Boolean.parseBoolean(trace) == true) ? SamplingResult.recordAndSample() : SamplingResult.drop();
        }

        for (String samplerName : this.samplerList) {
            SamplingResult result = this.samplers.get(samplerName)
                .shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
            if (result == SamplingResult.recordAndSample() || result == SamplingResult.drop()) {
                return result;
            }
        }
        return SamplingResult.drop();
    }

    @Override
    public String getDescription() {
        return "Request Sampler";
    }

    @Override
    public String toString() {
        return getDescription();
    }
}
