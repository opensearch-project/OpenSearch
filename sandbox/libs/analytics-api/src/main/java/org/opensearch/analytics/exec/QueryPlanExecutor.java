/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.profile.ProfiledResult;
import org.opensearch.core.action.ActionListener;

/**
 * Executes a logical query plan fragment against the underlying data store.
 *
 * @opensearch.internal
 */
public interface QueryPlanExecutor<LogicalPlan, Stream> {

    /**
     * Executes the given logical fragment and delivers the result stream (or a failure)
     * to {@code listener}.
     *
     * @param plan     the logical subtree to execute
     * @param queryCtx per-query snapshot ({@code null} → executor reads a fresh cluster state)
     * @param listener receives the produced stream on success, or the failure cause on error
     */
    void execute(LogicalPlan plan, QueryRequestContext queryCtx, ActionListener<Stream> listener);

    /**
     * Executes the given logical fragment with profiling enabled. Captures per-stage
     * timing from the coordinator's perspective and delivers a {@link ProfiledResult}
     * containing both the query results and the profile snapshot.
     *
     * @param plan     the logical subtree to execute
     * @param queryCtx per-query snapshot ({@code null} → executor reads a fresh cluster state)
     * @param listener receives the profiled result on success, or the failure cause on error
     */
    default void executeWithProfile(LogicalPlan plan, QueryRequestContext queryCtx, ActionListener<ProfiledResult> listener) {
        listener.onFailure(new UnsupportedOperationException(getClass().getSimpleName() + " does not support executeWithProfile"));
    }
}
