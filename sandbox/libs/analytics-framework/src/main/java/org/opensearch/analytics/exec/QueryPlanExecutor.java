/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;

/**
 * Executes a logical query plan fragment against the underlying data store.
 *
 * @opensearch.internal
 */
public interface QueryPlanExecutor {

    /**
     * Executes the given logical fragment and returns result rows.
     *
     * @param plan    the logical subtree to execute
     * @param context execution context (opaque Object to avoid server dependency)
     * @return an Iterable of Object[] rows produced by the engine
     */
    Iterable<Object[]> execute(RelNode plan, Object context);
}
