/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing.validators;

import org.opensearch.test.telemetry.tracing.MockSpanData;
import org.opensearch.test.telemetry.tracing.TracingValidator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * NumberOfTraceIDsEqualToRequests checks if number of unique traceIDs are equal to unique requests.
 */
public class NumberOfTraceIDsEqualToRequests implements TracingValidator {

    /**
     * Base Constructor
     */
    public NumberOfTraceIDsEqualToRequests() {}

    /**
     * validates if all spans emitted for a particular request have same traceID.
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public List<MockSpanData> validate(List<MockSpanData> spans, int requests) {
        Set<String> totalTraceIDs = spans.stream().map(MockSpanData::getTraceID).collect(Collectors.toSet());
        List<MockSpanData> problematicSpans = new ArrayList<>();
        if (totalTraceIDs.size() != requests) {
            problematicSpans.addAll(spans);
        }
        return problematicSpans;
    }
}
