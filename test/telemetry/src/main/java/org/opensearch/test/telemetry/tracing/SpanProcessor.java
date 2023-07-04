/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import org.opensearch.telemetry.tracing.Span;

/**
 * Processes the span and can perform any action on the span start and end.
 */
public interface SpanProcessor {
    /**
     * Logic to be executed on span start.
     * @param span span which is starting.
     */
    void onStart(Span span);

    /**
     * Logic to be executed on span end.
     * @param span span which is ending.
     */
    void onEnd(Span span);
}
