/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing.validators;

import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.test.telemetry.tracing.MockSpanData;
import org.opensearch.test.telemetry.tracing.TracingValidator;

import java.util.ArrayList;
import java.util.List;

/**
 *  Validates whether all the spans have the desired traceId or not.
 */
public class AllSpansHaveCorrectTraceId implements TracingValidator {

    private final String traceId;
    private final Attributes attributes;

    /**
     * Constructor which takes the desired traceId as input.
     * @param traceId traceId to verify
     * @param attributes attributes to filter out desired spans
     */
    public AllSpansHaveCorrectTraceId(String traceId, Attributes attributes) {
        this.traceId = traceId;
        this.attributes = attributes;
    }

    /**
     * Validates whether all the spans have the desired traceId or not.
     * @param spans spans
     * @param requests requests for e.g. search/index call
     * @return List of spans which do not have the desired traceId.
     */
    @Override
    public List<MockSpanData> validate(List<MockSpanData> spans, int requests) {
        List<MockSpanData> problematicSpans = new ArrayList<>();
        for (MockSpanData span : spans) {
            if (span.getAttributes().equals(attributes) && !span.getTraceID().equals(traceId)) {
                problematicSpans.add(span);
            }
        }
        return problematicSpans;
    }
}
