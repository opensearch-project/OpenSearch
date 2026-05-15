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
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

import java.util.List;

/**
 * Drives per-side distribution for {@link OpenSearchJoin}.
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

    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchJoinSplitRule(PlannerContext context) {
        super(operand(OpenSearchJoin.class, any()), "OpenSearchJoinSplitRule");
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);
        if (joinAlreadyResolved(join)) return false;
        return true;
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
