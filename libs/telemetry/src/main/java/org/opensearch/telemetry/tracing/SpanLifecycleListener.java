/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

/**
 * A listener for Span Lifecycle
 */
public interface SpanLifecycleListener {
    /**
     * On Span start
     * @param span span
     */
    void onStart(Span span);

    /**
     * On span end
     * @param span span
     */
    void onEnd(Span span);
}
