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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
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
 * <p>Accepts INNER / LEFT / RIGHT / FULL / SEMI / ANTI equi-joins. Cross joins match
 * via {@link JoinInfo#isEqui()}. Pure non-equi predicates are rejected.
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
        // Accept equi-joins and cross joins (both satisfy JoinInfo.isEqui() — empty
        // nonEquiConditions). A pure non-equi predicate (e.g. t1.a < t2.b) yields
        // isEqui()=false and stays rejected — DataFusion would need a non-equi
        // NestedLoopJoin path we don't enable yet.
        JoinInfo info = join.analyzeCondition();
        return info.isEqui();
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
