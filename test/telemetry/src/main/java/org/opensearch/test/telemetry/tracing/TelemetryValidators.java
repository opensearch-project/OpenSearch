/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TelemetryValidators for running validate on all applicable span Validator classes.
 */
public class TelemetryValidators {
    private List<TracingValidator> validators;

    /**
     * Base constructor.
     * @param validators list of validators applicable
     */
    public TelemetryValidators(List<TracingValidator> validators) {
        this.validators = validators;
    }

    /**
     * calls validate of all validators and throws exception in case of error.
     * @param spans List of spans emitted
     * @param requests Request can be indexing/search call
     */
    public void validate(List<MockSpanData> spans, int requests) {
        Map<String, List<MockSpanData>> problematicSpansMap = new HashMap<>();
        for (TracingValidator validator : this.validators) {
            List<MockSpanData> problematicSpans = validator.validate(spans, requests);
            if (!problematicSpans.isEmpty()) {
                problematicSpansMap.put(validator.getClass().getName(), problematicSpans);
            }
        }
        if (!problematicSpansMap.isEmpty()) {
            AssertionError error = new AssertionError(printProblematicSpansMap(problematicSpansMap));
            throw error;
        }
    }

    private String printProblematicSpansMap(Map<String, List<MockSpanData>> spanMap) {
        StringBuilder sb = new StringBuilder();
        for (var entry : spanMap.entrySet()) {
            sb.append("SpanData validation failed for validator " + entry.getKey());
            sb.append("\n");
            for (MockSpanData span : entry.getValue()) {
                sb.append(span.toString());
            }
        }
        return sb.toString();
    }
}
