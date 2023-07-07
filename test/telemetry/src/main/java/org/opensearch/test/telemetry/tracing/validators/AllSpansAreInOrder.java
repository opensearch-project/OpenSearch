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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * AllSpansAreInOrder validator to check if all spans are emitted in order.
 */
public class AllSpansAreInOrder implements TracingValidator {

    /**
     * Base Constructor
     */
    public AllSpansAreInOrder() {}

    /**
     * validates if all spans emitted have startTime after a parent span and endTime before a parent span.
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public List<MockSpanData> validate(List<MockSpanData> spans, int requests) {
        List<MockSpanData> problematicSpans = new ArrayList<>();
        // Create Map, add all entries
        Map<String, MockSpanData> map = spans.stream().collect(Collectors.toMap(MockSpanData::getSpanID, Function.identity()));

        for (MockSpanData s : spans) {
            if (s.getParentSpanID().isEmpty()) {
                continue;
            }
            long spanStartEpochNanos = s.getStartEpochNanos();
            long spanEndEpochNanos = s.getEndEpochNanos();

            MockSpanData parentSpanData = map.get(s.getParentSpanID());
            long parentSpanStartEpochNanos = parentSpanData.getStartEpochNanos();
            long parentSpanEndEpochNanos = parentSpanData.getEndEpochNanos();

            if ((parentSpanStartEpochNanos >= spanStartEpochNanos) || (parentSpanEndEpochNanos <= spanEndEpochNanos)) {
                problematicSpans.add(s);
            }
        }
        return problematicSpans;
    }
}
