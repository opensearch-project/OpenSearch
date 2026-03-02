/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

/**
 * Executes a logical query fragment against the underlying data store.
 * Implementations translate the fragment into engine-specific operations
 * and return result rows.
 *
 * This interface lives in server/plugins to avoid Calcite dependencies.
 * The query-engine module implements it; the calcite plugin consumes it
 * via the DQE plugin architecture.
 *
 * @opensearch.internal
 */
public interface QueryPlanExecutor {

    /**
     * Executes the given logical fragment and returns result rows.
     *
     * @param plan the logical subtree to execute (opaque Object to avoid Calcite dependency)
     * @param context execution context (opaque Object to avoid Calcite dependency)
     * @return an Iterable of Object[] rows produced by the engine
     */
    Iterable<Object[]> execute(Object plan, Object context);
}
