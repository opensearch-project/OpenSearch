/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

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
     * @param context  execution context (opaque Object to avoid server dependency)
     * @param listener receives the produced stream on success, or the failure cause on error
     */
    void execute(LogicalPlan plan, Object context, ActionListener<Stream> listener);

    /**
     * Executes the given logical fragment with profiling enabled. Captures per-stage
     * timing from the coordinator's perspective and delivers a {@link ProfiledResult}
     * containing both the query results and the profile snapshot.
     *
     * <p>Default implementation throws {@link UnsupportedOperationException}. Implementations
     * that support profiling (e.g. {@code DefaultPlanExecutor}) override this.
     *
     * @param plan     the logical subtree to execute
     * @param context  execution context (opaque Object to avoid server dependency)
     * @param listener receives the profiled result on success, or the failure cause on error
     */
    default void executeWithProfile(LogicalPlan plan, Object context, ActionListener<ProfiledResult> listener) {
        listener.onFailure(new UnsupportedOperationException(getClass().getSimpleName() + " does not support executeWithProfile"));
    }
}
