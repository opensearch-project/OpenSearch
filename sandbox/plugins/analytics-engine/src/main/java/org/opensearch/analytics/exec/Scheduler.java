/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.core.action.ActionListener;

/**
 * Pluggable coordinator-side query control flow. Given a {@link QueryContext},
 * drives the query to completion and signals the caller via an
 * {@link ActionListener}.
 *
 * <p>Implementations own:
 * <ul>
 *   <li>how stages are started and how failures cascade</li>
 *   <li>how external cancellation (timeouts, task cancel) is wired</li>
 *   <li>per-query cleanup on the transport dispatcher + any owned resources</li>
 * </ul>
 *
 * <p>Today's only implementation is {@link QueryScheduler}, which builds an
 * {@link ExecutionGraph} via {@link ExecutionGraph#build} and drives it
 * through a {@link QueryExecution} using state listeners. Future
 * implementations (pipelined, bottom-up recursive, etc.) implement this
 * same contract.
 *
 * @opensearch.internal
 */
public interface Scheduler {

    /**
     * Drives the query described by {@code config} to completion. Never blocks.
     * Fires the listener exactly once — {@code onResponse} with the root output's
     * Arrow batches on success, {@code onFailure} with the captured cause on
     * failure or cancellation. The caller (e.g., {@code DefaultPlanExecutor})
     * is responsible for any row materialization needed at the external API and
     * for wrapping the listener with any caller-side cleanup hooks (task
     * unregister, etc.) — the scheduler's single-fire guarantee on the listener
     * makes {@link ActionListener#runAfter} the natural place to attach them.
     */
    void execute(QueryContext config, ActionListener<Iterable<VectorSchemaRoot>> listener);
}
