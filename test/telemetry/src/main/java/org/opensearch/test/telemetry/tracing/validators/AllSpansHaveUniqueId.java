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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * AllSpansHaveUniqueId validator checks if all spans emitted have a unique spanID.
 */
public class AllSpansHaveUniqueId implements TracingValidator {

    /**
     * Base Constructor
     */
    public AllSpansHaveUniqueId() {}

    /**
     * validates if all spans emitted have a unique spanID
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public List<MockSpanData> validate(List<MockSpanData> spans, int requests) {
        List<MockSpanData> problematicSpans = new ArrayList<>();
        Set<String> set = new HashSet<>();
        for (MockSpanData span : spans) {
            if (set.contains(span.getSpanID())) {
                problematicSpans.add(span);
            }
            set.add(span.getSpanID());
        }
        return problematicSpans;
    }
}
