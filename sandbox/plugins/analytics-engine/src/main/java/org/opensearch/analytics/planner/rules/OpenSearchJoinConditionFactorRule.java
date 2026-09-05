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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.opensearch.analytics.planner.JoinKeyAnalysis;

/**
 * Factors conjuncts shared by every branch of an OR'd join condition up to the top level, turning
 * {@code (E AND A) OR (E AND B) OR (E AND C)} into {@code E AND (A OR B OR C)}.
 *
 * <p><b>Why this matters for distribution.</b> {@code JoinInfo.analyzeCondition} only recognises equi keys
 * among the TOP-LEVEL {@code AND} conjuncts of a join condition. When the shared equality hides inside an OR,
 * {@code leftKeys} comes back EMPTY and the join reads as PURE THETA — so every MPP split rule declines
 * (they all require a non-empty {@code leftKeys}) and the join is forced coordinator-centric, gathering both
 * inputs in full.
 *
 * <p>TPC-H q19 is exactly this shape: three OR'd branches that each repeat {@code p_partkey = l_partkey}
 * alongside a different brand/container/quantity/size filter. Measured at sf=10 it gathers
 * {@code lineitem ⋈ part} and dies with
 * {@code ReduceSizeExceededException} (~1.36 GB against a ~1.36 GB budget). Verified with a probe:
 * as written {@code leftKeys=[]}; after factoring, {@code leftKeys=[0]} and the condition becomes
 * {@code AND(=($0,$2), OR(...))} — precisely the equi-key-plus-residual shape
 * {@link OpenSearchHashJoinSplitRule} already supports (the TPC-H q14 case), so the join can hash-shuffle or
 * broadcast and the gather disappears.
 *
 * <p>Purely a predicate normalisation: {@link RexUtil#pullFactors} is semantics-preserving, so this cannot
 * change results — only which plans become available. Applied to any {@link Join} whose condition actually
 * changes shape, and gated on the rewrite exposing an equi key so it never churns the memo for nothing.
 *
 * @opensearch.internal
 */
public class OpenSearchJoinConditionFactorRule extends RelOptRule {

    public static final OpenSearchJoinConditionFactorRule INSTANCE = new OpenSearchJoinConditionFactorRule();

    private OpenSearchJoinConditionFactorRule() {
        super(operand(Join.class, any()), "OpenSearchJoinConditionFactorRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        RexNode condition = join.getCondition();
        if (condition == null || condition.isAlwaysTrue()) {
            return false;
        }
        // Only worth rewriting when the join currently exposes NO equi key but factoring would expose one.
        // Without this gate the rule re-fires on its own already-factored output.
        if (!JoinKeyAnalysis.forDistribution(join).leftKeys.isEmpty()) {
            return false;
        }
        RexNode factored = RexUtil.pullFactors(join.getCluster().getRexBuilder(), condition);
        return !factored.equals(condition) && exposesEquiKey(join, factored);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RexNode factored = RexUtil.pullFactors(join.getCluster().getRexBuilder(), join.getCondition());
        call.transformTo(
            join.copy(join.getTraitSet(), factored, join.getLeft(), join.getRight(), join.getJoinType(), join.isSemiJoinDone())
        );
    }

    /** True when {@code candidate} as a join condition yields at least one equi key. */
    private static boolean exposesEquiKey(Join join, RexNode candidate) {
        Join probe = join.copy(join.getTraitSet(), candidate, join.getLeft(), join.getRight(), join.getJoinType(), join.isSemiJoinDone());
        return !JoinKeyAnalysis.forDistribution(probe).leftKeys.isEmpty();
    }
}
