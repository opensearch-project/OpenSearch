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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * NumberOfTraceIDsEqualToRequests checks if number of unique traceIDs are equal to unique requests.
 */
public class NumberOfTraceIDsEqualToRequests implements TracingValidator {

    private static final String FILTERING_ATTRIBUTE = "action";
    private final Attributes attributes;

    /**
     * Constructor.
     * @param attributes attributes.
     */
    public NumberOfTraceIDsEqualToRequests(Attributes attributes) {
        this.attributes = attributes;
    }

    /**
     * validates if all spans emitted for a particular request have same traceID.
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public List<MockSpanData> validate(List<MockSpanData> spans, int requests) {
        final Collection<MockSpanData> totalTraceIDs = spans.stream().filter(span -> isMatchingSpan(span)).collect(Collectors.toList());
        List<MockSpanData> problematicSpans = new ArrayList<>();
        if (totalTraceIDs.stream().map(MockSpanData::getTraceID).distinct().count() != requests) {
            problematicSpans.addAll(totalTraceIDs);
        }
        return problematicSpans;
    }

    private boolean isMatchingSpan(MockSpanData mockSpanData) {
        if (attributes.getAttributesMap().isEmpty()) {
            return true;
        } else {
            return Objects.equals(
                mockSpanData.getAttributes().get(FILTERING_ATTRIBUTE),
                attributes.getAttributesMap().get(FILTERING_ATTRIBUTE)
            );
        }
    }
}
