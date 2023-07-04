/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing.validators;

import org.opensearch.telemetry.tracing.Span;
import org.opensearch.test.telemetry.tracing.*;

import java.util.List;

/**
 * TotalParentSpansEqualToRequests validator to check sanity on total parent spans.
 */
public class TotalParentSpansEqualToRequests implements SpanDataValidator {

    /**
     * validates if total parent spans are equal to number of requests.
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public boolean validate(List<MockSpanData> spans, int requests) {
        int totalParentSpans = 0;
        for (MockSpanData s : spans) {
            if (s.getParentSpanID().startsWith("00000")){
                totalParentSpans++;
            }
        }
        return totalParentSpans == requests;
    }
}
