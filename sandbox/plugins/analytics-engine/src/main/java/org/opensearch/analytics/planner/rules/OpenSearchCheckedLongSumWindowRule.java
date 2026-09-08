/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;

/**
 * Window counterpart to {@link OpenSearchCheckedLongSumRule}: rebinds
 * {@code CHECKED_LONG_SUM(...) OVER (...)} to Calcite's canonical {@code SUM}.
 *
 * @opensearch.internal
 */
public class OpenSearchCheckedLongSumWindowRule extends RelOptRule {

    public OpenSearchCheckedLongSumWindowRule() {
        super(operand(LogicalProject.class, any()), "OpenSearchCheckedLongSumWindowRule");
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        LogicalProject project = ruleCall.rel(0);
        return project.getProjects().stream().anyMatch(OpenSearchCheckedLongSumWindowRule::containsCheckedLongSum);
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        LogicalProject project = ruleCall.rel(0);
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        List<RexNode> rewritten = project.getProjects().stream().map(expr -> expr.accept(new Rewriter(rexBuilder))).toList();
        ruleCall.transformTo(project.copy(project.getTraitSet(), project.getInput(), rewritten, project.getRowType()));
    }

    private static boolean containsCheckedLongSum(RexNode expression) {
        boolean[] found = new boolean[1];
        expression.accept(new RexShuttle() {
            @Override
            public RexNode visitOver(RexOver over) {
                if (OpenSearchCheckedLongSumRule.isCheckedLongSum(over.getAggOperator())) {
                    found[0] = true;
                }
                return over;
            }
        });
        return found[0];
    }

    private static class Rewriter extends RexShuttle {
        private final RexBuilder rexBuilder;

        private Rewriter(RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitOver(RexOver over) {
            RexOver visited = (RexOver) super.visitOver(over);
            if (!OpenSearchCheckedLongSumRule.isCheckedLongSum(visited.getAggOperator())) {
                return visited;
            }
            RexWindow window = visited.getWindow();
            return rexBuilder.makeOver(
                visited.getType(),
                SqlStdOperatorTable.SUM,
                visited.getOperands(),
                window.partitionKeys,
                window.orderKeys,
                window.getLowerBound(),
                window.getUpperBound(),
                window.getExclude(),
                window.isRows(),
                true,
                false,
                visited.isDistinct(),
                visited.ignoreNulls()
            );
        }
    }
}
