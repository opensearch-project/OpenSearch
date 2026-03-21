/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.RelShuttleImpl;
import org.opensearch.analytics.plan.QueryPlanningException;
import org.opensearch.analytics.plan.operators.AggMode;
import org.opensearch.analytics.plan.operators.OpenSearchAggregate;
import org.opensearch.analytics.plan.operators.OpenSearchFilter;
import org.opensearch.analytics.plan.operators.OpenSearchProject;
import org.opensearch.analytics.plan.operators.OpenSearchTableScan;

import java.util.List;

/**
 * Phase 3 of the query planning pipeline: converts standard Calcite logical operators
 * into OpenSearch custom operators, each stamped with {@code backendTag = "unresolved"}.
 *
 * <p>Uses {@link RelShuttleImpl} for bottom-up tree rewriting. Each {@code visit()} override
 * manually recurses into inputs before constructing the replacement node, ensuring children
 * are wrapped before parents.
 *
 * <p>The catch-all {@link #visit(RelNode)} override rejects any operator type not explicitly
 * handled, preventing unhandled nodes from reaching Phase 5 where they would fail the
 * {@code BackendTagged} cast with a less informative error.
 *
 * <p>Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6
 */
public final class OperatorWrapperVisitor extends RelShuttleImpl {

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalTableScan) {
            return new OpenSearchTableScan(scan.getCluster(), scan.getTraitSet(),
                                           scan.getTable(), "unresolved");
        }
        return scan;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        RelNode input = filter.getInput().accept(this);
        return new OpenSearchFilter(filter.getCluster(), filter.getTraitSet(),
                                    input, filter.getCondition(), "unresolved");
    }

    @Override
    public RelNode visit(LogicalAggregate agg) {
        RelNode input = agg.getInput().accept(this);
        return new OpenSearchAggregate(agg.getCluster(), agg.getTraitSet(), input,
                                       agg.getGroupSet(), agg.getGroupSets(),
                                       agg.getAggCallList(), "unresolved", AggMode.UNRESOLVED);
    }

    @Override
    public RelNode visit(LogicalProject project) {
        RelNode input = project.getInput().accept(this);
        return new OpenSearchProject(project.getCluster(), project.getTraitSet(), input,
                                     project.getProjects(), project.getRowType(), "unresolved");
    }

    @Override
    public RelNode visit(RelNode other) {
        throw new QueryPlanningException(List.of(
            "OperatorWrapperVisitor: unhandled operator type: "
            + other.getClass().getSimpleName()
            + ". Add a visit() override or ensure this operator is eliminated in Phase 2."));
    }
}
