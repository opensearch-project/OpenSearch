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
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

import java.util.List;

/**
 * Coord-centric split rule for {@link OpenSearchJoin}. Produces the COORDINATOR_CENTRIC
 * strategy alternative — both inputs gather to the coordinator; the join runs there.
 *
 * <p><b>Firing contract</b> (from M2's strategy table):
 * <ul>
 *   <li>Theta (non-equi) joins: ALWAYS. Theta has no broadcast or hash alternative; this rule
 *       is the only legal path.</li>
 *   <li>Equi joins with {@code analytics.mpp.enabled=false}: ALWAYS. The kill switch forces
 *       coord-centric for all joins.</li>
 *   <li>Equi joins with {@code analytics.mpp.enabled=true}: NEVER. CBO must pick between
 *       BROADCAST and HASH_SHUFFLE only — coord-centric is not a competitor in this case.
 *       The {@link OpenSearchBroadcastJoinSplitRule} and {@link OpenSearchHashJoinSplitRule}
 *       cover this path.</li>
 * </ul>
 *
 * <p><b>Co-location fast path.</b> When both sides are SHARD+SINGLETON scans with
 * {@code shardCount=1} and the same {@code tableId} (self-join on a 1-shard table),
 * the join runs at the shard node without any ER. Output preserves that trait so a
 * downstream operator (or the root) can insert a single gather ER above it.
 *
 * <p><b>General path.</b> Otherwise, request {@code COORDINATOR+SINGLETON} on each
 * side. Volcano materializes an ER on any non-{@code COORDINATOR+SINGLETON} input via
 * {@link OpenSearchDistributionTraitDef#convert}.
 *
 * @opensearch.internal
 */
public class OpenSearchJoinSplitRule extends RelOptRule {

    private final PlannerContext context;
    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchJoinSplitRule(PlannerContext context) {
        super(operand(OpenSearchJoin.class, any()), "OpenSearchJoinSplitRule");
        this.context = context;
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);
        if (joinAlreadyResolved(join)) return false;
        // Contract: this rule produces COORDINATOR_CENTRIC. Per M2's strategy table, that
        // strategy is legal when (a) the join is theta (no MPP alternative exists),
        // (b) MPP is killed cluster-wide, or (c) the structural shape disqualifies the MPP
        // rules — broadcast and hash both require multi-shard SHARD-distributed inputs on
        // both sides. Inputs like {@code Project(Filter(Scan))} where the underlying scan
        // is 1-shard, or non-scan inputs like {@code Values} / {@code Aggregate} sub-trees,
        // can't satisfy that. Without (c), {@code CannotPlanException} fires because no
        // rule produces a plan for those shapes under mpp.enabled=true. The check unwraps
        // RelSubsets via {@code best/original} so transient demanded-trait subsets don't
        // accidentally flip the answer mid-CBO.
        boolean mppEnabled = AnalyticsSettings.MPP_ENABLED.get(context.getSettings());
        JoinInfo info = join.analyzeCondition();
        boolean isEqui = info.isEqui();
        if (mppEnabled && isEqui && bothInputsCouldBeMppShardScans(join)) {
            return false;
        }
        return true;
    }

    /** True when both inputs originate from SHARD-distributed scans with shardCount > 1
     *  (the situation the broadcast/hash rules need to fire — single-shard scans can't
     *  benefit from broadcast/hash and would fail the broadcast cost gate, which requires
     *  RANDOM+SHARD on the probe). Unlike the rules' own check that looks at {@code
     *  rel.getTraitSet()}, this version unwraps RelSubsets via best/original so a transient
     *  demanded-trait subset doesn't make us think the inputs are non-SHARD. */
    private static boolean bothInputsCouldBeMppShardScans(OpenSearchJoin join) {
        OpenSearchDistribution leftDist = originalDistribution(join.getLeft());
        OpenSearchDistribution rightDist = originalDistribution(join.getRight());
        return isMultiShard(leftDist) && isMultiShard(rightDist);
    }

    private static boolean isMultiShard(OpenSearchDistribution dist) {
        if (dist == null) return false;
        if (dist.getLocality() != OpenSearchDistribution.Locality.SHARD) return false;
        Integer shardCount = dist.getShardCount();
        // shardCount can be null on synthetic subtrees; treat that as "not multi-shard" so
        // we route through coord-centric rather than risking a CannotPlan.
        return shardCount != null && shardCount > 1;
    }

    private static OpenSearchDistribution originalDistribution(RelNode rel) {
        if (rel instanceof org.apache.calcite.plan.volcano.RelSubset subset) {
            RelNode best = subset.getBestOrOriginal();
            if (best != rel) return originalDistribution(best);
        }
        return distributionOf(rel);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);

        Integer commonTableId = commonColocatedTableId(List.of(join.getLeft(), join.getRight()));
        if (commonTableId != null) {
            // Co-location applies: both sides are 1-shard scans of the same table. Build
            // the Join at SHARD with no ER on either input, then call convert(shardJoin,
            // COORDINATOR) so a parent demanding COORDINATOR sees a single gather ER above
            // (one transport instead of two).
            RelTraitSet shardTraits = join.getTraitSet().replace(distTraitDef.shardSingleton(commonTableId, 1));
            RelNode shardJoin = join.copy(
                shardTraits,
                join.getCondition(),
                join.getLeft(),
                join.getRight(),
                join.getJoinType(),
                join.isSemiJoinDone()
            );
            RelTraitSet coordTraits = join.getTraitSet().replace(distTraitDef.coordSingleton());
            convert(shardJoin, coordTraits);
            call.transformTo(shardJoin);
            return;
        }

        // Not co-located: one side originates from a different table or shard layout, so
        // per-side ERs are unavoidable. Demand COORDINATOR+SINGLETON on each side.
        RelTraitSet coordTraits = join.getTraitSet().replace(distTraitDef.coordSingleton());
        RelNode gatheredLeft = convert(join.getLeft(), coordTraits);
        RelNode gatheredRight = convert(join.getRight(), coordTraits);
        call.transformTo(
            join.copy(coordTraits, join.getCondition(), gatheredLeft, gatheredRight, join.getJoinType(), join.isSemiJoinDone())
        );
    }

    private static Integer commonColocatedTableId(List<RelNode> inputs) {
        Integer commonId = null;
        for (RelNode input : inputs) {
            OpenSearchDistribution dist = distributionOf(input);
            if (dist == null) return null;
            if (dist.getLocality() != OpenSearchDistribution.Locality.SHARD) return null;
            if (dist.getType() != RelDistribution.Type.SINGLETON) return null;
            if (!Integer.valueOf(1).equals(dist.getShardCount())) return null;
            Integer tid = dist.getTableId();
            if (tid == null) return null;
            if (commonId == null) commonId = tid;
            else if (!commonId.equals(tid)) return null;
        }
        return commonId;
    }

    private static boolean joinAlreadyResolved(OpenSearchJoin join) {
        OpenSearchDistribution joinDist = distributionOf(join);
        if (joinDist == null) return false;
        if (joinDist.getLocality() == OpenSearchDistribution.Locality.COORDINATOR && joinDist.getType() == RelDistribution.Type.SINGLETON) {
            OpenSearchDistribution ld = distributionOf(join.getLeft());
            OpenSearchDistribution rd = distributionOf(join.getRight());
            return ld != null
                && ld.getLocality() == OpenSearchDistribution.Locality.COORDINATOR
                && ld.getType() == RelDistribution.Type.SINGLETON
                && rd != null
                && rd.getLocality() == OpenSearchDistribution.Locality.COORDINATOR
                && rd.getType() == RelDistribution.Type.SINGLETON;
        }
        if (joinDist.getLocality() == OpenSearchDistribution.Locality.SHARD && joinDist.getType() == RelDistribution.Type.SINGLETON) {
            return true;
        }
        return false;
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }
}
