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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.JoinCapability;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * HEP marker rewriting {@link LogicalJoin} → {@link OpenSearchJoin}. Both inputs are
 * gathered to the coordinator (enforced by the join's cost gate, which only accepts
 * SINGLETON inputs — Volcano inserts an {@link OpenSearchExchangeReducer} per side).
 *
 * <p>Accepts INNER / LEFT / RIGHT / FULL / SEMI / ANTI joins that have at least one
 * equi conjunct (so a hash join is viable; any remaining non-equi conjuncts ride
 * along as a residual filter). Cross joins (no condition at all) also match. Pure
 * non-equi joins (e.g. {@code t1.a < t2.b} as the only condition) are still
 * rejected — DataFusion would need a non-equi NestedLoopJoin path we don't enable
 * yet, but a mixed condition like
 * {@code AND(<=($8, $cor0.__stream_seq__), =($11, $cor0.__seg_id__), =($7, $cor0.DEPTNO))}
 * — produced by RelDecorrelator on streamstats reset's directly-built
 * LogicalCorrelate — survives via the equi conjuncts.
 *
 * @opensearch.internal
 */
public class OpenSearchJoinRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchJoinRule(PlannerContext context) {
        super(operand(LogicalJoin.class, any()), "OpenSearchJoinRule");
        this.context = context;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        JoinRelType joinType = join.getJoinType();
        // Accept INNER / LEFT / RIGHT / FULL / SEMI / ANTI equi-joins. FULL is needed
        // by PPL's `appendcol` lowering (ROW_NUMBER pairing via a full outer join on the
        // row numbers). Pure non-equi joins are rejected below via JoinInfo.isEqui().
        if (joinType != JoinRelType.INNER
            && joinType != JoinRelType.LEFT
            && joinType != JoinRelType.RIGHT
            && joinType != JoinRelType.FULL
            && joinType != JoinRelType.SEMI
            && joinType != JoinRelType.ANTI) {
            return false;
        }
        // Accept equi-joins, cross joins, and mixed equi+non-equi conditions.
        //   * Equi-only joins → JoinInfo.isEqui() true, leftKeys non-empty.
        //   * Cross joins → condition is literal TRUE; JoinInfo.isEqui() true,
        //     leftKeys empty.
        //   * Mixed condition (e.g. AND(<=, =, =)) → isEqui() false because of the
        //     non-equi conjunct, but leftKeys still carries the equi pairs and
        //     nonEquiConditions carries the residual. A hash join on the equi keys
        //     plus a residual filter is viable.
        // Pure non-equi joins (no equi conjunct, no leftKey) are still rejected —
        // the runtime hash-join path needs at least one equi key. Surfaces in
        // RelDecorrelator output for streamstats reset's directly-built
        // LogicalCorrelate, which always carries at least one equi correlation
        // alongside its range correlation.
        JoinInfo info = join.analyzeCondition();
        if (!info.leftKeys.isEmpty()) {
            // Equi or mixed equi+non-equi (hash key + residual filter both viable).
            return true;
        }
        if (info.isEqui()) {
            // Cross join (literal-true condition).
            return true;
        }
        // JoinInfo classifies IS_NOT_DISTINCT_FROM into nonEquiConditions in some
        // call paths even though splitJoinCondition handles it as a null-safe equi
        // when filterNulls is non-null. Walk the condition's conjuncts ourselves —
        // if any one is a binary EQUALS / IS_NOT_DISTINCT_FROM between RexInputRefs
        // straddling the inputs, the join is hashable on that pair (DataFusion
        // builds a hash join key, residual conditions ride along as a filter).
        if (hasInputRefEquiOrNullSafeConjunct(join)) {
            return true;
        }
        return false;
    }

    /** Returns true if the join condition has at least one conjunct of the form
     *  {@code RexInputRef = RexInputRef} or
     *  {@code RexInputRef IS NOT DISTINCT FROM RexInputRef} where the two refs
     *  straddle the left / right side. The dispatch in {@code Join#analyzeCondition()}
     *  occasionally puts {@code IS_NOT_DISTINCT_FROM} into {@code nonEquiConditions}
     *  even though {@code splitJoinCondition} logic recognises it as a null-safe
     *  equi join key; this method is a robust fallback. */
    private static boolean hasInputRefEquiOrNullSafeConjunct(LogicalJoin join) {
        int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        int rightFieldCount = join.getRight().getRowType().getFieldCount();
        ImmutableBitSet leftRange = ImmutableBitSet.range(0, leftFieldCount);
        ImmutableBitSet rightRange = ImmutableBitSet.range(leftFieldCount, leftFieldCount + rightFieldCount);
        for (RexNode conjunct : RelOptUtil.conjunctions(join.getCondition())) {
            if (!(conjunct instanceof RexCall call)) continue;
            SqlKind kind = call.getKind();
            if (kind != SqlKind.EQUALS && kind != SqlKind.IS_NOT_DISTINCT_FROM) continue;
            if (call.getOperands().size() != 2) continue;
            ImmutableBitSet op0Refs = RelOptUtil.InputFinder.bits(call.getOperands().get(0));
            ImmutableBitSet op1Refs = RelOptUtil.InputFinder.bits(call.getOperands().get(1));
            // Each operand must reference exactly one side; the two sides must be different.
            // DataFusion's hash-join evaluates each operand expression to derive the key —
            // RexCall arithmetic (e.g. {@code uid = salary + 1000}) is acceptable.
            boolean op0Left = leftRange.contains(op0Refs);
            boolean op0Right = rightRange.contains(op0Refs);
            boolean op1Left = leftRange.contains(op1Refs);
            boolean op1Right = rightRange.contains(op1Refs);
            if ((op0Left && op1Right) || (op0Right && op1Left)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        // Viable backends = intersection of inputs' viable backends, narrowed to those whose
        // joinCapabilities declare the join's required JoinKind. Inputs are HepRelVertex-
        // wrapped marked nodes by the time this rule fires; bottom-up HEP traversal
        // guarantees they're already in OpenSearchConvention.
        List<String> viableBackends = computeViableBackends(join.getLeft(), join.getRight());
        List<String> candidateBackends = List.copyOf(viableBackends);
        JoinCapability.JoinKind requiredKind = JoinCapability.JoinKind.fromCalcite(join.getJoinType());
        viableBackends.removeIf(backend -> {
            var caps = context.getCapabilityRegistry().getBackend(backend).getCapabilityProvider();
            for (JoinCapability cap : caps.joinCapabilities()) {
                if (cap.kinds().contains(requiredKind)) return false;
            }
            return true;
        });
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException(
                "No backend supports join kind [" + requiredKind + "] among viable backends " + candidateBackends
            );
        }
        // HEP marking only — no ER insertion. OpenSearchJoin's cost gate (SINGLETON input
        // required) drives Volcano to insert ERs on each input via TraitDef.convert.
        OpenSearchDistributionTraitDef distTraitDef = context.getDistributionTraitDef();
        RelNode leftUnwrapped = RelNodeUtils.unwrapHep(join.getLeft());
        RelNode rightUnwrapped = RelNodeUtils.unwrapHep(join.getRight());
        RelTraitSet joinTraits = leftUnwrapped.getTraitSet().replace(distTraitDef.coordSingleton());
        OpenSearchJoin osJoin = new OpenSearchJoin(
            join.getCluster(),
            joinTraits,
            leftUnwrapped,
            rightUnwrapped,
            join.getCondition(),
            join.getJoinType(),
            viableBackends
        );
        call.transformTo(osJoin);
    }

    /** Intersection of viable backends from left and right children. Children may be
     *  {@link HepRelVertex}-wrapped — unwrap to read viableBackends if it's an
     *  {@link OpenSearchRelNode}. onMatch then narrows to backends whose
     *  {@link JoinCapability} declares the join's required kind. */
    private static List<String> computeViableBackends(RelNode left, RelNode right) {
        List<String> leftBackends = viableBackendsOf(left);
        List<String> rightBackends = viableBackendsOf(right);

        Set<String> intersection = new LinkedHashSet<>(leftBackends);
        intersection.retainAll(rightBackends);
        return new ArrayList<>(intersection);
    }

    private static List<String> viableBackendsOf(RelNode rel) {
        if (RelNodeUtils.unwrapHep(rel) instanceof OpenSearchRelNode osNode) {
            return osNode.getViableBackends();
        }
        // Not yet marked — empty list forces the fallback path above.
        return List.of();
    }
}
