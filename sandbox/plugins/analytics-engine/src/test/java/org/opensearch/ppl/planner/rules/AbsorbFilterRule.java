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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;

/**
 * RelOptRule that absorbs a {@link LogicalFilter} into an {@link OpenSearchBoundaryTableScan}.
 *
 * <p>Pattern: {@code LogicalFilter} on top of {@code OpenSearchBoundaryTableScan}.
 *
 * <p>When the rule matches, it checks whether all functions in the filter condition
 * are supported by the back-end's {@link SqlOperatorTable}. If supported, the filter
 * is absorbed into the boundary node's logical fragment.
 *
 * <p>This is NOT a ConverterRule — it transforms an already-converted boundary node
 * by growing its internal logical fragment.
 */
public class AbsorbFilterRule extends RelOptRule {

    private final SqlOperatorTable operatorTable;

    public static AbsorbFilterRule create(SqlOperatorTable operatorTable) {
        return new AbsorbFilterRule(operatorTable);
    }

    private AbsorbFilterRule(SqlOperatorTable operatorTable) {
        super(operand(LogicalFilter.class, operand(OpenSearchBoundaryTableScan.class, none())), "AbsorbFilterRule");
        this.operatorTable = operatorTable;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        OpenSearchBoundaryTableScan boundary = call.rel(1);

        if (!AbsorbRuleUtils.allFunctionsSupported(filter.getCondition(), operatorTable)) {
            return;
        }

        // Wrap the existing logical fragment with the filter to build the new absorbed subtree
        LogicalFilter absorbedFilter = filter.copy(filter.getTraitSet(), boundary.getLogicalFragment(), filter.getCondition());

        // Create a new boundary node with the expanded logical fragment
        OpenSearchBoundaryTableScan newBoundary = new OpenSearchBoundaryTableScan(
            boundary.getCluster(),
            boundary.getTraitSet(),
            boundary.getTable(),
            absorbedFilter,
            boundary.getEngineExecutor()
        );

        call.transformTo(newBoundary);
    }
}
