/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.backend;

import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.core.action.ActionListener;

import java.io.Closeable;

/**
 * Backend-provided execution context for a LOCAL (coordinator-side) stage.
 * Created by {@code AnalyticsSearchBackendPlugin.createLocalStage} and owned
 * by {@code LocalStageExecution} for the lifetime of the stage.
 *
 * <p>A local stage sits between its child stages (which produce Arrow batches
 * via shard dispatch) and a downstream output sink. The context provides:
 * <ul>
 *   <li>Per-child {@link ExchangeSink}s that child stage executions feed
 *       batches into — potentially from multiple shards concurrently.
 *       Implementations must be thread-safe since N shard callbacks may
 *       call {@code feed()} on the same sink in parallel.</li>
 *   <li>An {@link #asyncFinalize} entry point that the execution calls once
 *       all children have completed. The backend drains child inputs, runs
 *       the local plan fragment (e.g. final aggregation, sort, filter),
 *       writes results to the downstream sink, and signals completion.</li>
 * </ul>
 *
 * <p>Implementations own whatever backend-internal resources they need
 * (native engines, input handles, drain threads) and expose only this
 * contract to the analytics-engine.
 *
 * @opensearch.internal
 */
public interface LocalStageContext extends Closeable {

    /**
     * Returns the sink for a specific child stage. Child stage executions
     * feed Arrow batches into this sink as shard responses arrive. Multiple
     * shards within the same child stage may call {@code feed()} concurrently
     * — the returned sink must be thread-safe.
     *
     * <p>Each child stage id must have been declared in the
     * {@link LocalStageRequest#getChildSchemas()} map at construction time.
     *
     * @param childStageId the stage id of the child whose output this sink collects
     * @return the {@link ExchangeSink} for the given child stage
     * @throws IllegalArgumentException if no sink is registered for the given child stage id
     */
    ExchangeSink sinkFor(int childStageId);

    /**
     * Called once all children have completed successfully. The backend
     * drains child inputs, executes the local plan fragment, writes output
     * to the downstream sink (provided in the original
     * {@link LocalStageRequest}), and signals the listener exactly once.
     *
     * <p>Non-blocking from the caller's perspective. The implementation
     * typically runs the drain on a virtual thread or executor and fires
     * the listener on completion.
     *
     * @param listener signaled on success ({@code onResponse(null)}) or
     *                 failure ({@code onFailure(exception)})
     */
    void asyncFinalize(ActionListener<Void> listener);

    /**
     * Releases all backend resources. Idempotent. Called on failure or
     * cancellation paths — may be called before, during, or after
     * {@link #asyncFinalize}.
     *
     * <p>If a drain is in flight, {@code close()} must cause it to
     * terminate promptly. The drain's listener may subsequently fire with
     * a failure; callers should use the listener's terminal signal (not
     * {@code close()} returning) as the synchronization point.
     */
    @Override
    void close();
}
