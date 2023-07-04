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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * NumberOfTraceIDsEqualToRequests checks if number of unique traceIDs are equal to unique requests.
 */
public class NumberOfTraceIDsEqualToRequests implements SpanDataValidator {

    /**
     * validates if all spans emitted for a particular request have same traceID.
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public boolean validate(List<MockSpanData> spans, int requests) {
        Set<String> uniqueTraceIds = new HashSet<>();
        for (MockSpanData s : spans) {
            uniqueTraceIds.add(s.getTraceID());
        }
        return Integer.compare(uniqueTraceIds.size(), requests) == 0;
    }
}
