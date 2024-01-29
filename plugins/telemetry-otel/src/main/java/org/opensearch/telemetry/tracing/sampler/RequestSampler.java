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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import static org.opensearch.telemetry.tracing.AttributeNames.TRACE;
import static org.opensearch.telemetry.tracing.AttributeNames.TRANSPORT_ACTION;

/**
 * HeadBased sampler
 */
public class RequestSampler implements Sampler {
    private final Sampler defaultSampler;

    private final Sampler actionSampler;

    /**
     * Creates action sampler which samples request for all actions based on defined probability
     * @param telemetrySettings TelemetrySettings
     */
    public RequestSampler(TelemetrySettings telemetrySettings) {
        this.defaultSampler = new ProbabilisticSampler(telemetrySettings);
        this.actionSampler = new TransportActionSampler(telemetrySettings);
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
        final String action = attributes.get(AttributeKey.stringKey(TRANSPORT_ACTION));

        // Determine the sampling decision based on the availability of trace information and action,
        // either recording and sampling, delegating to action sampler, or falling back to default sampler.
        if (trace != null) {
            return (Boolean.parseBoolean(trace) == true) ? SamplingResult.recordAndSample() : SamplingResult.drop();
        } else if (action != null) {
            return actionSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
        } else {
            return defaultSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
        }
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
