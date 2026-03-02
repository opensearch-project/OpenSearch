/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl.planner;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.opensearch.plugins.QueryPlanExecutor;

/**
 * Default {@link EngineExecutor} implementation that wraps a {@link QueryPlanExecutor} provided
 * by server.
 */
public class DefaultEngineExecutor implements EngineExecutor {

    private final QueryPlanExecutor queryPlanExecutor;

    public DefaultEngineExecutor(QueryPlanExecutor queryPlanExecutor) {
        this.queryPlanExecutor = queryPlanExecutor;
    }

    @Override
    public Enumerable<Object[]> execute(RelNode logicalFragment, DataContext dataContext) {
        Iterable<Object[]> result = queryPlanExecutor.execute(logicalFragment, dataContext);
        return Linq4j.asEnumerable(result);
    }
}
