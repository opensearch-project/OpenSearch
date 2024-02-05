/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.sampler;

import java.util.List;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import static org.opensearch.telemetry.tracing.AttributeNames.TRACE;

/**
 * RequestSampler based on HeadBased sampler
 */
public class RequestSampler implements Sampler {
    private final List<Sampler> samplersList;

    /**
     * Creates request sampler which applies based on all applicable sampler
     * @param samplersList list of Sampler
     */
    public RequestSampler(List<Sampler> samplersList) {
        this.samplersList = samplersList;
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

        for (Sampler sampler : this.samplersList) {
            SamplingResult result = sampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
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
