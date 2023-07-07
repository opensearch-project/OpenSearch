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

import java.util.List;
import java.util.stream.Collectors;

/**
 * AllSpansAreEndedProperly validator to check if all spans are closed properly.
 */
public class AllSpansAreEndedProperly implements TracingValidator {

    /**
     * Base Constructor
     */
    public AllSpansAreEndedProperly() {}

    /**
     * validates if all spans emitted have hasEnded attribute as true.
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public List<MockSpanData> validate(List<MockSpanData> spans, int requests) {
        List<MockSpanData> problematicSpans = spans.stream().filter(s -> s.isHasEnded() == false).collect(Collectors.toList());
        return problematicSpans;
    }
}
