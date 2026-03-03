/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.planner;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;

/**
 * Integration point for AnalyticsEnginePlugins to provide an execution path. The boundary node calls
 * {@link #execute(RelNode, DataContext)} in its {@code bind()} method,
 * passing the absorbed logical subtree and the Calcite DataContext.
 */
public interface PlanExecutor {

    /**
     * Executes the given logical fragment and returns the result rows.
     *
     * @param logicalFragment the absorbed logical subtree to execute
     * @param dataContext      the Calcite DataContext for the current execution
     * @return an Enumerable of rows produced by the engine
     */
    Enumerable<Object[]> execute(RelNode logicalFragment, DataContext dataContext);
}
