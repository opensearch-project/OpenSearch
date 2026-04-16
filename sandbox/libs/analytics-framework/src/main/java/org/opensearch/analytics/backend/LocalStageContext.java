/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.opensearch.core.action.ActionListener;

import java.io.Closeable;

/**
 * Backend-provided execution context for a LOCAL stage. The walker obtains one
 * of these from {@code AnalyticsSearchBackendPlugin.createLocalStage} and uses
 * it to wire per-child sinks and to finalize the stage once all children have
 * completed.
 *
 * <p>Implementations own whatever backend-internal resources they need (engines,
 * input handles, drain threads) and expose only the sink + finalize contract to
 * the analytics-engine.
 *
 * @opensearch.internal
 */
public interface LocalStageContext extends Closeable {

    /**
     * Returns the per-child sink that the walker feeds with this child stage's
     * row output. Each child stage MUST have been declared in the request's
     * childSchemas map.
     *
     * @param childStageId the stage id of the child whose output this sink collects
     * @return the {@link ExchangeSink} for the given child stage
     * @throws IllegalArgumentException if no sink is registered for the given child stage id
     */
    ExchangeSink sinkFor(int childStageId);

    /**
     * Called by the walker after all children have completed successfully.
     * The implementation closes its inputs, completes any pending drain work,
     * forwards drained output into the downstream sink supplied in the request,
     * and signals the listener exactly once.
     *
     * <p>Non-blocking from the caller's POV; the listener is fired on whatever
     * thread the implementation chooses (typically a virtual thread).
     *
     * @param listener the listener to signal on completion or failure
     */
    void asyncFinalize(ActionListener<Void> listener);

    /**
     * Idempotent. Releases all backend resources (engine, handles, drain).
     * Called by the walker on any failure path.
     *
     * <p>If an {@link #asyncFinalize} drain is in flight, {@code close()} MUST
     * cause it to terminate promptly (either by natural next-iteration flag
     * check or by interrupting the drain thread). The drain's listener may fire
     * with {@code TaskCancelledException} as a result; callers must not rely on
     * {@code close()} fully completing before the listener has fired — use the
     * listener's terminal state as the synchronization point for any follow-on
     * action.
     */
    @Override
    void close();
}
