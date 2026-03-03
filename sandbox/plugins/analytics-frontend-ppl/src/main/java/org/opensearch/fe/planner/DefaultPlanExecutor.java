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
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.exec.QueryPlanExecutor;

/**
 * Default {@link PlanExecutor} implementation that wraps a {@link QueryPlanExecutor} provided
 * by the analytics engine framework.
 */
public class DefaultPlanExecutor implements PlanExecutor {

    private final QueryPlanExecutor queryPlanExecutor;

    public DefaultPlanExecutor(QueryPlanExecutor queryPlanExecutor) {
        this.queryPlanExecutor = queryPlanExecutor;
    }

    @Override
    public Enumerable<Object[]> execute(RelNode logicalFragment, DataContext dataContext) {
        Iterable<Object[]> result = queryPlanExecutor.execute(logicalFragment, dataContext);
        return Linq4j.asEnumerable(result);
    }
}
