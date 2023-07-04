/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing.validators;

import org.opensearch.test.telemetry.tracing.*;

import java.util.List;

/**
 * AllSpansAreEndedProperly validator to check if all spans are closed properly.
 */
public class AllSpansAreEndedProperly implements SpanDataValidator {

    /**
     * validates if all spans emitted have hasEnded attribute as true.
     * @param spans spans emitted.
     * @param requests requests for e.g. search/index call
     */
    @Override
    public boolean validate(List<MockSpanData> spans, int requests) {
        for (MockSpanData s : spans) {
            if (!s.isHasEnded()) {
                return false;
            }
        }
        return true;
    }
}
