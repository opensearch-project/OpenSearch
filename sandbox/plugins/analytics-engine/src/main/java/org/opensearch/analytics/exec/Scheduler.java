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
 * Coordinator-side query control flow. Given a {@link QueryContext}, drives the
 * query to completion and signals the caller via an {@link ActionListener}.
 *
 * <p>One implementation: {@link QueryScheduler}, which builds an
 * {@link ExecutionGraph} via {@link ExecutionGraph#build} and drives it through
 * a {@link QueryExecution} using state listeners. The interface stays so an
 * alternative orchestration strategy (e.g. one that wraps the existing scheduler
 * to layer retries at the query level) can plug in without touching call sites.
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
    void execute(QueryContext context, ActionListener<Iterable<VectorSchemaRoot>> listener);
}
