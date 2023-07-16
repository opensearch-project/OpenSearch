/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.listeners;

import org.opensearch.telemetry.tracing.Span;

/**
 * The RunnableEventListener interface defines the contract for listeners that handle events related to the execution
 * of runnables in a traced environment.
 */
public interface RunnableEventListener {

    /**
     * Called when a runnable starts executing.
     *
     * @param span the span associated with the execution context
     * @param t    the thread executing the runnable
     */
    void onRunnableStart(Span span, Thread t);

    /**
     * Called when a runnable completes execution.
     *
     * @param span the span associated with the execution context
     * @param t    the thread executing the runnable
     */
    void onRunnableComplete(Span span, Thread t);

    /**
     * Checks whether the listener is enabled for the given span.
     * TODO - replace with operation based flag
     * @param span the span associated with the execution context
     * @return true if the listener is enabled for the span, false otherwise
     */
    boolean isEnabled(Span span);
}

