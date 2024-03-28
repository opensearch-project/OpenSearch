/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.samplingResult;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.trace.samplers.SamplingDecision;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

/**
 * Custom Sampling Result Class*
 */
public class OTelSamplingResult implements SamplingResult {

    private final SamplingDecision samplingDecision;

    private final Attributes attributes;

    /**
     * Constructor*
     * @param samplingDecision decision that needs to be added
     * @param attributes attribute list that needs to be added
     */
    public OTelSamplingResult(SamplingDecision samplingDecision, Attributes attributes) {
        this.samplingDecision = samplingDecision;
        this.attributes = attributes;
    }

    /**
     * Return decision on whether a span should be recorded, recorded and sampled or not recorded.
     *
     * @return sampling result.
     */
    @Override
    public SamplingDecision getDecision() {
        return samplingDecision;
    }

    /**
     * Return tags which will be attached to the span.
     *
     * @return attributes added to span. These attributes should be added to the span only when
     * {@linkplain #getDecision() the sampling decision} is {@link SamplingDecision#RECORD_ONLY}
     * or {@link SamplingDecision#RECORD_AND_SAMPLE}.
     */
    @Override
    public Attributes getAttributes() {
        return attributes;
    }
}
