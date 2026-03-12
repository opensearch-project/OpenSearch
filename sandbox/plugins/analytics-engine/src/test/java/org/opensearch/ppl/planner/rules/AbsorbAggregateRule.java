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
import org.opensearch.analytics.backend.EngineCapabilities;
import org.opensearch.ppl.planner.rel.OpenSearchBoundaryTableScan;

/**
 * Absorbs a {@link LogicalAggregate} (and any intermediate nodes between it
 * and the boundary) into an {@link OpenSearchBoundaryTableScan}.
 *
 * <p>Checks both operator-level support ({@code supportsOperator}) and
 * expression-level support ({@code supportsAllAggFunctions}) before absorbing.
 */
public class AbsorbAggregateRule extends RelOptRule {

    private final EngineCapabilities capabilities;

    public static AbsorbAggregateRule create(EngineCapabilities capabilities) {
        return new AbsorbAggregateRule(capabilities);
    }

    private AbsorbAggregateRule(EngineCapabilities capabilities) {
        super(operand(LogicalAggregate.class, any()), "AbsorbAggregateRule");
        this.capabilities = capabilities;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggregate = call.rel(0);

        if (!capabilities.supportsOperator(aggregate)) {
            return;
        }
        if (!capabilities.supportsAllAggFunctions(aggregate.getAggCallList())) {
            return;
        }

        OpenSearchBoundaryTableScan boundary = AbsorbRuleUtils.findBoundary(aggregate);
        if (boundary == null) {
            return;
        }

        call.transformTo(AbsorbRuleUtils.absorbIntoBoundary(aggregate, boundary));
    }
}
