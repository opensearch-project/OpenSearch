/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.fe.ppl.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalSort;
import org.opensearch.fe.ppl.planner.EngineCapabilities;
import org.opensearch.fe.ppl.planner.rel.OpenSearchBoundaryTableScan;

/**
 * Absorbs a {@link LogicalSort} (and any intermediate nodes between it
 * and the boundary) into an {@link OpenSearchBoundaryTableScan}.
 *
 * <p>Sort collations are field references and directions — no expression-level
 * capability checks are needed beyond operator support.
 */
public class AbsorbSortRule extends RelOptRule {

    private final EngineCapabilities capabilities;

    public static AbsorbSortRule create(EngineCapabilities capabilities) {
        return new AbsorbSortRule(capabilities);
    }

    private AbsorbSortRule(EngineCapabilities capabilities) {
        super(operand(LogicalSort.class, any()), "AbsorbSortRule");
        this.capabilities = capabilities;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort sort = call.rel(0);

        if (!capabilities.supportsOperator(sort)) {
            return;
        }

        OpenSearchBoundaryTableScan boundary = AbsorbRuleUtils.findBoundary(sort);
        if (boundary == null) {
            return;
        }

        call.transformTo(AbsorbRuleUtils.absorbIntoBoundary(sort, boundary));
    }
}
