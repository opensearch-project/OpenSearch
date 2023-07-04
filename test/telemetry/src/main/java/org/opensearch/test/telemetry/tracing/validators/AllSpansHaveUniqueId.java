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
 * AllSpansHaveUniqueId validator checks if all spans emitted have a unique spanID.
 */
public class AllSpansHaveUniqueId implements SpanDataValidator {

    /**
     * validates if all spans emitted have a unique spanID
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public boolean validate(List<MockSpanData> spans, int requests) {
        Set<String> uniqueIds = new HashSet<>();
        for (MockSpanData s : spans) {
            uniqueIds.add(s.getSpanID());
        }
        return Integer.compare(uniqueIds.size(), spans.size()) == 0;
    }
}
