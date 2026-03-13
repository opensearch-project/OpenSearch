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
import org.apache.calcite.rel.logical.LogicalSort;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;

/**
 * Absorbs a {@link LogicalSort} (and any intermediate nodes between it
 * and the boundary) into an {@link OpenSearchBoundaryTableScan}.
 *
 * <p>Sort collations are field references and directions — no expression-level
 * capability checks are needed. Sort always absorbs if a boundary exists.
 */
public class AbsorbSortRule extends RelOptRule {

    public static AbsorbSortRule create() {
        return new AbsorbSortRule();
    }

    private AbsorbSortRule() {
        super(operand(LogicalSort.class, any()), "AbsorbSortRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);

        OpenSearchBoundaryTableScan boundary = AbsorbRuleUtils.findBoundary(sort);
        if (boundary == null) {
            return;
        }

        call.transformTo(AbsorbRuleUtils.absorbIntoBoundary(sort, boundary));
    }
}
