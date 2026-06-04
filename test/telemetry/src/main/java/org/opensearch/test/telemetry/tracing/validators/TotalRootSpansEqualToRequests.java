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
import java.util.stream.Collectors;

/**
 * TotalRootSpansEqualToRequests validator to check sanity on total parent spans.
 */
public class TotalRootSpansEqualToRequests implements TracingValidator {

    /**
     * Base Constructor
     */
    public TotalRootSpansEqualToRequests() {}

    /**
     * validates if total parent spans are equal to number of requests.
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public List<MockSpanData> validate(List<MockSpanData> spans, int requests) {
        List<MockSpanData> problematicSpans = new ArrayList<>();
        List<MockSpanData> totalParentSpans = spans.stream().filter(s -> s.getParentSpanID().isEmpty()).collect(Collectors.toList());
        if (totalParentSpans.size() != requests) {
            problematicSpans.addAll(totalParentSpans);
        }
        return problematicSpans;
    }
}
