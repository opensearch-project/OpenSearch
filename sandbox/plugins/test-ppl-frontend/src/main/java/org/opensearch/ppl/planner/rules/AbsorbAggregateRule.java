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
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;

/**
 * Absorbs a {@link LogicalAggregate} (and any intermediate nodes between it
 * and the boundary) into an {@link OpenSearchBoundaryTableScan}.
 *
 * <p>Checks that all aggregate functions are supported by the back-end's
 * {@link SqlOperatorTable} before absorbing.
 */
public class AbsorbAggregateRule extends RelOptRule {

    private final SqlOperatorTable operatorTable;

    public static AbsorbAggregateRule create(SqlOperatorTable operatorTable) {
        return new AbsorbAggregateRule(operatorTable);
    }

    private AbsorbAggregateRule(SqlOperatorTable operatorTable) {
        super(operand(LogicalAggregate.class, any()), "AbsorbAggregateRule");
        this.operatorTable = operatorTable;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggregate = call.rel(0);

        if (!AbsorbRuleUtils.allAggFunctionsSupported(aggregate.getAggCallList(), operatorTable)) {
            return;
        }

        OpenSearchBoundaryTableScan boundary = AbsorbRuleUtils.findBoundary(aggregate);
        if (boundary == null) {
            return;
        }

        call.transformTo(AbsorbRuleUtils.absorbIntoBoundary(aggregate, boundary));
    }
}
