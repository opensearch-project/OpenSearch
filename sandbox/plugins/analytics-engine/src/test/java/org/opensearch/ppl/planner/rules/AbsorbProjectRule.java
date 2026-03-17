/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;

/**
 * RelOptRule that absorbs a {@link LogicalProject} into an {@link OpenSearchBoundaryTableScan}.
 *
 * <p>Pattern: {@code LogicalProject} on top of {@code OpenSearchBoundaryTableScan}.
 *
 * <p>When the rule matches, it checks whether all functions in the project expressions
 * are supported by the back-end's {@link SqlOperatorTable}. If supported, the project
 * is absorbed into the boundary node's logical fragment.
 *
 * <p>This is NOT a ConverterRule — it transforms an already-converted boundary node
 * by growing its internal logical fragment.
 */
public class AbsorbProjectRule extends RelOptRule {

    private final SqlOperatorTable operatorTable;

    public static AbsorbProjectRule create(SqlOperatorTable operatorTable) {
        return new AbsorbProjectRule(operatorTable);
    }

    private AbsorbProjectRule(SqlOperatorTable operatorTable) {
        super(operand(LogicalProject.class, operand(OpenSearchBoundaryTableScan.class, none())), "AbsorbProjectRule");
        this.operatorTable = operatorTable;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        OpenSearchBoundaryTableScan boundary = call.rel(1);

        // Check that all functions in every project expression are supported
        for (RexNode expr : project.getProjects()) {
            if (!AbsorbRuleUtils.allFunctionsSupported(expr, operatorTable)) {
                return;
            }
        }

        // Wrap the existing logical fragment with the project to build the new absorbed subtree
        LogicalProject absorbedProject = project.copy(
            project.getTraitSet(),
            boundary.getLogicalFragment(),
            project.getProjects(),
            project.getRowType()
        );

        // Create a new boundary node with the expanded logical fragment
        OpenSearchBoundaryTableScan newBoundary = new OpenSearchBoundaryTableScan(
            boundary.getCluster(),
            boundary.getTraitSet(),
            boundary.getTable(),
            absorbedProject,
            boundary.getEngineExecutor()
        );

        call.transformTo(newBoundary);
    }
}
