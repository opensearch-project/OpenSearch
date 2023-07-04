/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing.validators;

import org.opensearch.test.telemetry.tracing.*;

import java.util.HashMap;
import java.util.List;

/**
 * AllSpansAreInOrder validator to check if all spans are emitted in order.
 */
public class AllSpansAreInOrder implements SpanDataValidator {

    /**
     * validates if all spans emitted have startTime after a parent span and endTime before a parent span.
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public boolean validate(List<MockSpanData> spans, int requests) {
        //Create Map, add all entries
        HashMap<String, MockSpanData> map= new HashMap<>();
        for (MockSpanData s : spans) {
            map.put(s.getSpanID(), s);
        }

        for (MockSpanData s : spans) {
            if (s.getParentSpanID().startsWith("00000")){
                continue;
            }
            long spanStartEpochNanos  = s.getStartEpochNanos();
            long spanEndEpochNanos = s.getEndEpochNanos();

            MockSpanData parentSpanData = map.get(s.getParentSpanID());
            long parentSpanStartEpochNanos = parentSpanData.getStartEpochNanos();
            long parentSpanEndEpochNanos = parentSpanData.getEndEpochNanos();

            if ((parentSpanStartEpochNanos >= spanStartEpochNanos) || (parentSpanEndEpochNanos <= spanEndEpochNanos)) {
                return false;
            }
        }
        return true;
    }
}
