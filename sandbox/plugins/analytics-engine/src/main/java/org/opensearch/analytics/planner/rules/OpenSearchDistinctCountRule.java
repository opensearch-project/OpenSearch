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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites single-arg {@code COUNT(DISTINCT x)} → {@code APPROX_COUNT_DISTINCT(x)} before
 * the aggregate is marked by {@link OpenSearchAggregateRule}. Multi-arg distinct falls through
 * to coordinator-gather in {@link OpenSearchAggregateSplitRule}.
 *
 * @opensearch.internal
 */
public class OpenSearchDistinctCountRule extends RelOptRule {

    public OpenSearchDistinctCountRule() {
        super(operand(LogicalAggregate.class, any()), "OpenSearchDistinctCountRule");
    }

    @Override
    public boolean matches(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        return agg.getAggCallList().stream().anyMatch(c -> isSingleArgCountDistinct(c) || isUdfApproxCountDistinct(c));
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        List<AggregateCall> rewritten = new ArrayList<>(agg.getAggCallList().size());
        boolean changed = false;
        for (AggregateCall call : agg.getAggCallList()) {
            if (isSingleArgCountDistinct(call) || isUdfApproxCountDistinct(call)) {
                rewritten.add(rewriteToApprox(call, agg));
                changed = true;
            } else {
                rewritten.add(call);
            }
        }
        if (!changed) return;
        ruleCall.transformTo(agg.copy(agg.getTraitSet(), agg.getInput(), agg.getGroupSet(), agg.getGroupSets(), rewritten));
    }

    private static boolean isSingleArgCountDistinct(AggregateCall call) {
        return call.getAggregation().getKind() == SqlKind.COUNT && call.isDistinct() && call.getArgList().size() == 1;
    }

    /** Matches PPL's UDF-based APPROX_COUNT_DISTINCT that needs rewriting to the Calcite built-in. */
    private static boolean isUdfApproxCountDistinct(AggregateCall call) {
        return call.getAggregation().getKind() == SqlKind.OTHER_FUNCTION
            && "APPROX_COUNT_DISTINCT".equalsIgnoreCase(call.getAggregation().getName());
    }

    private static AggregateCall rewriteToApprox(AggregateCall call, LogicalAggregate agg) {
        return AggregateCall.create(
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            /* distinct */ false,
            /* approximate */ false,
            call.ignoreNulls(),
            call.rexList,
            call.getArgList(),
            call.filterArg,
            call.distinctKeys,
            call.collation,
            agg.getGroupSet().cardinality(),
            agg.getInput(),
            call.getType(),
            call.getName()
        );
    }
}
