/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

/**
 * Executes a logical query plan fragment against the underlying data store.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface QueryPlanExecutor<LogicalPlan, Stream> {

    /**
     * Executes the given logical fragment and returns result rows.
     *
     * @param plan    the logical subtree to execute
     * @param context execution context (opaque Object to avoid server dependency)
     * @return rows produced by the engine
     */
    Stream execute(LogicalPlan plan, Object context);
}
