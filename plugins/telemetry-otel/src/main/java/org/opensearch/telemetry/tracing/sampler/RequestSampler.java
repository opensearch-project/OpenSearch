/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.sampler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.TelemetrySettings;

import java.lang.reflect.Constructor;
import java.util.HashMap;
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
    private final HashMap<String, Sampler> samplers;
    private final TelemetrySettings telemetrySettings;
    /**
     * Logger instance for logging messages related to the request sampler.
     */
    protected Logger logger;

    /**
     * Creates request sampler which applies based on all applicable sampler
     * @param telemetrySettings TelemetrySettings
     */
    public RequestSampler(TelemetrySettings telemetrySettings) {
        this.telemetrySettings = telemetrySettings;
        this.samplers = new HashMap<>();
        this.logger = LogManager.getLogger(getClass());
        this.SamplerInit();
    }

    /**
     * Initialises all samplers based on telemetry setting
     */
    private void SamplerInit() {
        for (String samplerName : this.telemetrySettings.getSamplingOrder()) {
            try {
                Class<?> samplerClass = Class.forName(samplerName);
                Constructor<?> constructor = samplerClass.getConstructor(TelemetrySettings.class);
                Sampler sampler = (Sampler) constructor.newInstance(telemetrySettings);
                this.samplers.put(samplerName, sampler);
            } catch (Exception e) {
                logger.error("error while creatin class object: ", e);
            }
        }
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

        List<String> samplers = telemetrySettings.getSamplingOrder();
        if (trace != null) {
            return (Boolean.parseBoolean(trace) == true) ? SamplingResult.recordAndSample() : SamplingResult.drop();
        }

        for (String samplerName : samplers) {
            SamplingResult result = this.samplers.get(samplerName)
                .shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
            if (result == SamplingResult.recordAndSample()) {
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
