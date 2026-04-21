/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.stage.StageExecution;
import org.opensearch.analytics.exec.stage.StageExecutionBuilder;
import org.opensearch.core.action.ActionListener;

/**
 * Pluggable coordinator-side query control flow. Given a {@link QueryContext},
 * drives the query to completion and signals the caller via an
 * {@link ActionListener}.
 *
 * <p>Implementations own:
 * <ul>
 *   <li>how the execution graph is built from {@code QueryContext.dag()}</li>
 *   <li>how stages are started and how failures cascade</li>
 *   <li>how external cancellation (timeouts, task cancel) is wired</li>
 *   <li>per-query cleanup on the transport dispatcher + any owned resources</li>
 * </ul>
 *
 * <p>Today's only implementation is {@link QueryScheduler}, which walks
 * the DAG once up front, constructs all {@link StageExecution} instances via
 * {@link StageExecutionBuilder#buildExecution}, and drives them through a
 * {@link PlanWalker} using state listeners. Future implementations
 * (pipelined, bottom-up recursive, etc.) implement this same contract.
 *
 * @opensearch.internal
 */
public interface Scheduler {

    /**
     * Drives the query described by {@code config} to completion. Never blocks.
     * Fires the listener exactly once — {@code onResponse} with the root output's
     * Arrow batches on success, {@code onFailure} with the captured cause on
     * failure or cancellation. The caller (e.g., {@code DefaultPlanExecutor})
     * is responsible for any row materialization needed at the external API.
     */
    void execute(QueryContext config, ActionListener<Iterable<VectorSchemaRoot>> listener);
}
