/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import java.util.List;

/**
 * Performs validations on traces emitted.
 */
public interface TracingValidator {
    /**
     * Validates spanData and return list of problematic spans.
     * @param spans spans emitted at any point of time.
     * @param requests requests can be search/index calls.
     */
    public List<MockSpanData> validate(List<MockSpanData> spans, int requests);
}
