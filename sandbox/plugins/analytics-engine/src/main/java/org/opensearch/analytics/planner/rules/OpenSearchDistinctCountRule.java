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
 * Rewrites single-arg {@code COUNT(DISTINCT x)} → {@code APPROX_COUNT_DISTINCT(x)} on plain
 * {@link LogicalAggregate} during the HEP rewrite phase, before {@link OpenSearchAggregateRule}
 * marks it.
 *
 * <p>Why: PPL {@code dc(x)} / {@code distinct_count(x)} are parsed by the SQL plugin's PPL
 * frontend as {@code COUNT(DISTINCT x)} (see {@code AstExpressionBuilder.visitDistinctCountFunctionCall}).
 * Distinctness can't be reduced additively across shards — summing per-shard distinct counts
 * over-counts any value present on more than one shard. Routing the call through Calcite's
 * standard {@link SqlStdOperatorTable#APPROX_COUNT_DISTINCT} engages the
 * {@link org.opensearch.analytics.spi.AggregateFunction#APPROX_COUNT_DISTINCT}
 * registration ({@code Type.APPROXIMATE} + {@code intermediateFields=[sketch:Binary, reducer==self]}),
 * which carries cross-shard HLL sketch merge semantics through the
 * {@link OpenSearchAggregateSplitRule} structural split,
 * {@link org.opensearch.analytics.planner.dag.DistributedAggregateRewriter#overrideExchangeType}
 * engine-native-merge retyping, and the DataFusion backend's prepare_partial/final_plan execution.
 *
 * <p>Multi-arg {@code COUNT(DISTINCT a, b)} doesn't match — falls through to the residual
 * {@code aggCall.isDistinct()} skip in {@link OpenSearchAggregateSplitRule}, which gathers to
 * the coordinator and runs the distinct count once.
 *
 * <p>The new {@link AggregateCall} is built via the {@code create} overload that takes
 * {@code (groupCount, input)} and a {@code null} explicit type — Calcite re-infers the return
 * type from {@code APPROX_COUNT_DISTINCT.inferReturnType(...)}, which avoids the
 * {@code typeMatchesInferred} mismatch that would surface if {@code COUNT}'s
 * {@code BIGINT NOT NULL} type were cloned onto an operator that infers force-nullable.
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
        return agg.getAggCallList().stream().anyMatch(OpenSearchDistinctCountRule::isSingleArgCountDistinct);
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        LogicalAggregate agg = ruleCall.rel(0);
        List<AggregateCall> rewritten = new ArrayList<>(agg.getAggCallList().size());
        boolean changed = false;
        for (AggregateCall call : agg.getAggCallList()) {
            if (isSingleArgCountDistinct(call)) {
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
            /* type */ null,
            call.getName()
        );
    }
}
