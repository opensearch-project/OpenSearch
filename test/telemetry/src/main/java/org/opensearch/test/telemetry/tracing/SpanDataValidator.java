/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import org.opensearch.telemetry.tracing.Span;

import java.util.List;

/**
 * Performs validations on span Data.
 */
public interface SpanDataValidator {
    /**
     * Validates spanData and return boolean value on validation status.
     * @param spans spans emitted at any point of time.
     * @param requests requests can be search/index calls.
     */
    public boolean validate(List<MockSpanData> spans, int requests);
}
