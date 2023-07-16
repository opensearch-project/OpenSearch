/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.listeners;

import org.opensearch.telemetry.diagnostics.DiagnosticSpan;
import org.opensearch.telemetry.tracing.Span;

/**
 * The TraceEventListener interface defines the contract for listeners that handle events related to trace.
 */
public interface SpanEventListener {

    /**
     * Called when a span starts.
     *
     * @param span the span that has started
     * @param t    the thread associated with the span
     */
    void onSpanStart(Span span, Thread t);

    /**
     * Called when a span completes.
     *
     * @param span the span that has completed
     * @param t    the thread associated with the span
     */
    void onSpanComplete(Span span, Thread t);

    /**
     * Checks whether the listener is enabled for the given span.
     * TODO - replace with operation based flag
     * @param span the span to check
     * @return true if the listener is enabled for the span, false otherwise
     */
    boolean isEnabled(Span span);
}

