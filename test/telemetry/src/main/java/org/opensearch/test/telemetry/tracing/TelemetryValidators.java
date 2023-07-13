/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * TelemetryValidators for running validate on all applicable span Validator classes.
 */
public class TelemetryValidators {
    private Collection<Class<? extends TracingValidator>> validators;

    /**
     * Base constructor.
     * @param validators list of validators applicable
     */
    public TelemetryValidators(Collection<Class<? extends TracingValidator>> validators) {
        this.validators = validators;
    }

    /**
     * calls validate of all validators and throws exception in case of error.
     * @param spans List of spans emitted
     * @param requests Request can be indexing/search call
     */
    public void validate(List<MockSpanData> spans, int requests) {
        List<MockSpanData> totalProblematicSpans = new ArrayList<>();
        for (Class<? extends TracingValidator> v : this.validators) {
            try {
                TracingValidator validator = v.getConstructor().newInstance();
                List<MockSpanData> problematicSpans = validator.validate(spans, requests);
                StringBuilder sb = new StringBuilder();
                for (MockSpanData span : problematicSpans) {
                    sb.append(span.toString());
                }
                totalProblematicSpans.addAll(problematicSpans);
            } catch (Exception e) {
                e.getStackTrace();
            }
        }
        if (!totalProblematicSpans.isEmpty()) {
            AssertionError error = new AssertionError(" SpanData validation failed for following spans " + totalProblematicSpans);
            throw error;
        }
    }
}
