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
import org.opensearch.analytics.exec.join.MppShufflePartitions;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

/**
 * Hash-shuffle split rule for {@link OpenSearchJoin} (M2). Sibling of
 * {@link OpenSearchJoinSplitRule}. Both fire on the same operand; this one emits a
 * HASH-localized alternative that {@code OpenSearchJoin}'s cost gate accepts when both inputs
 * deliver matching {@code WORKER+HASH(keys, partitionCount)} traits. The result is a join
 * that runs on data-node workers in parallel, with Volcano materializing
 * {@link org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange} on each input via
 * the trait converter.
 *
 * <p>Gates:
 * <ul>
 *   <li>{@code analytics.mpp.shuffle_enabled} must be true (cluster setting; default true).</li>
 *   <li>{@code JoinInfo.isEqui()} must be true. Hash partitioning requires equality predicates;
 *       theta joins flow through {@link OpenSearchJoinSplitRule}'s coordinator-centric path.</li>
 *   <li>The resolved partition count (cluster setting → engine default) must be ≥ 2. A count
 *       of 1 means no viable backend supports shuffle (or the cluster has only one node), in
 *       which case BROADCAST or COORDINATOR_CENTRIC is the right strategy instead.</li>
 *   <li>{@code joinAlreadyResolvedAsHash(...)} must be false to avoid re-firing on the rule's
 *       own output.</li>
 * </ul>
 *
 * <p>The rule competes with {@link OpenSearchJoinSplitRule} via Volcano's cost model. The
 * advisor (post-CBO) inspects which alternative survived and tags stages accordingly; today
 * the cost difference is structural (HASH+HASH vs SINGLETON+SINGLETON), and the strategy
 * selector at {@link org.opensearch.analytics.exec.join.JoinStrategyAdvisor} time decides
 * whether to dispatch the HASH plan based on row counts.
 *
 * @opensearch.internal
 */
public class OpenSearchHashJoinSplitRule extends RelOptRule {

    private final PlannerContext context;
    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchHashJoinSplitRule(PlannerContext context) {
        super(operand(OpenSearchJoin.class, any()), "OpenSearchHashJoinSplitRule");
        this.context = context;
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // The master MPP kill switch must be on — shuffle is a sub-feature of MPP. With MPP
        // disabled cluster-wide, the join routes through OpenSearchJoinSplitRule's
        // coord-centric path; this rule must stay out of that decision.
        if (!AnalyticsSettings.MPP_ENABLED.get(context.getSettings())) {
            return false;
        }
        if (!AnalyticsSettings.MPP_SHUFFLE_ENABLED.get(context.getSettings())) {
            return false;
        }
        OpenSearchJoin join = call.rel(0);
        JoinInfo info = join.analyzeCondition();
        if (!info.isEqui()) {
            return false;
        }
        if (joinAlreadyResolvedAsHash(join)) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);
        JoinInfo info = join.analyzeCondition();
        int partitionCount = MppShufflePartitions.resolve(
            context.getSettings(),
            context.getClusterState(),
            context.getCapabilityRegistry(),
            join.getViableBackends()
        );
        // No backend opts in (or single-node cluster); leave the alternative space to
        // OpenSearchJoinSplitRule. The advisor will route this query through BROADCAST or
        // COORDINATOR_CENTRIC as appropriate.
        if (partitionCount <= 1) {
            return;
        }

        // Demand a HASH partitioning on the per-side keys. Volcano's trait converter
        // materializes an OpenSearchShuffleExchange on any input not already so distributed.
        OpenSearchDistribution leftHash = distTraitDef.hash(info.leftKeys, partitionCount);
        OpenSearchDistribution rightHash = distTraitDef.hash(info.rightKeys, partitionCount);
        RelTraitSet leftTraits = join.getLeft().getTraitSet().replace(leftHash);
        RelTraitSet rightTraits = join.getRight().getTraitSet().replace(rightHash);
        RelNode shuffledLeft = convert(join.getLeft(), leftTraits);
        RelNode shuffledRight = convert(join.getRight(), rightTraits);

        // The hash-join itself runs at WORKER+HASH(leftKeys, N). Convention: use the left
        // keys as the join's own hash key marker (the cost gate validates each input's HASH
        // independently — it doesn't compare the join's keys to inputs').
        OpenSearchDistribution joinHash = distTraitDef.hash(info.leftKeys, partitionCount);
        RelTraitSet joinTraits = join.getTraitSet().replace(joinHash);
        RelNode workerJoin = join.copy(joinTraits, join.getCondition(), shuffledLeft, shuffledRight, join.getJoinType(), join.isSemiJoinDone());

        // Coord still needs the result; convert WORKER → COORDINATOR registers a final gather
        // ER above. The transformTo target carries WORKER traits; Volcano's enforcement will
        // wrap with an ER when a SINGLETON consumer demands it.
        RelTraitSet coordTraits = join.getTraitSet().replace(distTraitDef.coordSingleton());
        convert(workerJoin, coordTraits);
        call.transformTo(workerJoin);
    }

    /** True when this join already carries a WORKER+HASH trait — i.e. this rule has already
     *  fired on it and the alternative is registered. Prevents a memo loop. */
    private static boolean joinAlreadyResolvedAsHash(OpenSearchJoin join) {
        OpenSearchDistribution joinDist = distributionOf(join);
        if (joinDist == null) return false;
        if (joinDist.getType() != RelDistribution.Type.HASH_DISTRIBUTED) return false;
        if (joinDist.getLocality() != OpenSearchDistribution.Locality.WORKER) return false;
        OpenSearchDistribution ld = distributionOf(join.getLeft());
        OpenSearchDistribution rd = distributionOf(join.getRight());
        return ld != null
            && ld.getType() == RelDistribution.Type.HASH_DISTRIBUTED
            && ld.getLocality() == OpenSearchDistribution.Locality.WORKER
            && rd != null
            && rd.getType() == RelDistribution.Type.HASH_DISTRIBUTED
            && rd.getLocality() == OpenSearchDistribution.Locality.WORKER;
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }
}
