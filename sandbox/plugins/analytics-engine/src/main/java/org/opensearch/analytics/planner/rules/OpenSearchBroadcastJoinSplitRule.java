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
import org.apache.calcite.rel.core.JoinRelType;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

/**
 * Broadcast-join split rule (M2). Sibling of {@link OpenSearchJoinSplitRule} (coord-centric)
 * and {@link OpenSearchHashJoinSplitRule}. Fires on {@link OpenSearchJoin}; emits a
 * broadcast-shape alternative where one side (the build) is replicated to every probe-side
 * worker via {@link org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange} and
 * the other side (the probe) keeps its SHARD+RANDOM trait. The join itself runs at
 * SHARD+RANDOM alongside the probe scan; CBO ranks broadcast vs hash via the cost model
 * (broadcast cost = build_rows × probe_nodes; hash cost = total_rows + setup × N).
 *
 * <p>For INNER joins, emits two alternatives (left-as-build, right-as-build) and lets cost
 * decide which side to broadcast. For LEFT/RIGHT/FULL outer joins, only the row-preserved
 * side can be probe; the other must be build. SEMI/ANTI: build is always the right side.
 *
 * <p>Gates:
 * <ul>
 *   <li>{@code analytics.mpp.enabled} must be true (master MPP kill switch).</li>
 *   <li>{@code JoinInfo.isEqui()} must be true. Theta routes through the coord-centric path.</li>
 *   <li>Both inputs must already carry SHARD+RANDOM (i.e. the planner has stamped scan
 *       traits but no exchange has wrapped them yet). Without this gate the rule re-fires
 *       on the alternatives it produced.</li>
 *   <li>Probe-node estimate (from cluster setting / data-node count) must be ≥ 1.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class OpenSearchBroadcastJoinSplitRule extends RelOptRule {

    private final PlannerContext context;
    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchBroadcastJoinSplitRule(PlannerContext context) {
        super(operand(OpenSearchJoin.class, any()), "OpenSearchBroadcastJoinSplitRule");
        this.context = context;
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!AnalyticsSettings.MPP_ENABLED.get(context.getSettings())) {
            return false;
        }
        OpenSearchJoin join = call.rel(0);
        JoinInfo info = join.analyzeCondition();
        if (!info.isEqui()) {
            return false;
        }
        // Refuse to fire on already-resolved alternatives. Both inputs must be vanilla
        // SHARD-distributed scans (untouched by an exchange).
        return isShardScan(join.getLeft()) && isShardScan(join.getRight());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchJoin join = call.rel(0);
        int probeNodes = resolveProbeNodeEstimate(join.getLeft());
        if (probeNodes <= 1) {
            // Broadcast offers no parallelism gain over coord-centric on a 1-node cluster
            // (or in test fixtures with a stubbed ClusterState). Leave alternative space to
            // OpenSearchJoinSplitRule (coord-centric); the strategy advisor will record
            // COORDINATOR_CENTRIC for telemetry.
            return;
        }

        JoinRelType joinType = join.getJoinType();
        // Decide which sides are eligible as the build:
        //   INNER, FULL? — both sides eligible (full not handled in M2 spike; left/right could
        //                   broadcast either side but neither preserves rows, so emit both).
        //   LEFT          — left rows must be preserved → left is probe → build = right.
        //   RIGHT         — right rows preserved → build = left.
        //   SEMI / ANTI   — build = right (M0/M1 contract).
        //   FULL          — neither side can be duplicated; broadcast doesn't apply.
        boolean leftAsBuildEligible = joinType == JoinRelType.INNER || joinType == JoinRelType.RIGHT;
        boolean rightAsBuildEligible = joinType == JoinRelType.INNER
            || joinType == JoinRelType.LEFT
            || joinType == JoinRelType.SEMI
            || joinType == JoinRelType.ANTI;
        if (!leftAsBuildEligible && !rightAsBuildEligible) {
            return;
        }

        if (leftAsBuildEligible) {
            emitBroadcastAlternative(call, join, /* buildSide = */ true, probeNodes);
        }
        if (rightAsBuildEligible) {
            emitBroadcastAlternative(call, join, /* buildSide = */ false, probeNodes);
        }
    }

    /**
     * Emits one broadcast alternative: the chosen build side is wrapped in a broadcast
     * exchange (via trait conversion); the other side (probe) keeps its SHARD trait. The
     * join's own trait is the probe side's SHARD+RANDOM, which propagates the probe's
     * tableId so the cost gate can validate.
     *
     * @param leftIsBuild true if the left side is broadcast (probe = right); false otherwise.
     */
    private void emitBroadcastAlternative(RelOptRuleCall call, OpenSearchJoin join, boolean leftIsBuild, int probeNodes) {
        RelNode buildSide = leftIsBuild ? join.getLeft() : join.getRight();
        RelNode probeSide = leftIsBuild ? join.getRight() : join.getLeft();
        OpenSearchDistribution probeDist = distributionOf(probeSide);
        if (probeDist == null) {
            return;
        }

        // Demand BROADCAST+REPLICATED on the build side. Volcano materializes an
        // OpenSearchBroadcastExchange via OpenSearchDistributionTraitDef.convert.
        OpenSearchDistribution broadcastDist = distTraitDef.broadcast(probeNodes);
        RelTraitSet buildTraits = buildSide.getTraitSet().replace(broadcastDist);
        RelNode broadcastBuild = convert(buildSide, buildTraits);

        // Probe side keeps its SHARD+RANDOM trait — no exchange. The join sits at the same
        // trait so it runs alongside the probe scan on probe-side data nodes. The
        // distribution copy preserves probe's tableId / shardCount (used by the cost gate).
        RelTraitSet joinTraits = join.getTraitSet().replace(distTraitDef.from(probeDist));
        RelNode workerJoin;
        if (leftIsBuild) {
            workerJoin = join.copy(joinTraits, join.getCondition(), broadcastBuild, probeSide, join.getJoinType(), join.isSemiJoinDone());
        } else {
            workerJoin = join.copy(joinTraits, join.getCondition(), probeSide, broadcastBuild, join.getJoinType(), join.isSemiJoinDone());
        }

        // Coord still gathers the joined output. Register the gather alternative in the memo
        // so a parent demanding COORDINATOR+SINGLETON can find it.
        RelTraitSet coordTraits = join.getTraitSet().replace(distTraitDef.coordSingleton());
        convert(workerJoin, coordTraits);
        call.transformTo(workerJoin);
    }

    /** Resolves the probe-node estimate from the cluster setting, defaulting to the cluster's
     *  data-node count when the setting is unset (-1). The {@code anyChild} parameter lets us
     *  keep the API forward-compatible if we ever want per-side estimates. Tolerant of test
     *  fixtures where ClusterState mocks don't stub {@code nodes()}: returns 1 (no broadcast
     *  benefit) which causes the rule to bail. */
    private int resolveProbeNodeEstimate(RelNode anyChild) {
        Integer override = AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE.get(context.getSettings());
        if (override != null && override > 0) {
            return override;
        }
        // Default: cluster's data-node count. Defensive against partially-mocked ClusterState
        // (test fixtures often stub metadata() but not nodes()).
        org.opensearch.cluster.ClusterState state = context.getClusterState();
        if (state == null || state.nodes() == null) {
            return 1;
        }
        return Math.max(state.nodes().getDataNodes().size(), 1);
    }

    /** True when {@code rel}'s OpenSearchDistribution is SHARD-localized — i.e. the rel is an
     *  unwrapped scan (or scan-shaped subtree) without an exchange already on top. */
    private static boolean isShardScan(RelNode rel) {
        OpenSearchDistribution dist = distributionOf(rel);
        if (dist == null) return false;
        return dist.getLocality() == OpenSearchDistribution.Locality.SHARD;
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }

    @SuppressWarnings("unused")
    private static boolean joinAlreadyResolvedAsBroadcast(OpenSearchJoin join) {
        OpenSearchDistribution joinDist = distributionOf(join);
        if (joinDist == null) return false;
        if (joinDist.getType() != RelDistribution.Type.RANDOM_DISTRIBUTED) return false;
        if (joinDist.getLocality() != OpenSearchDistribution.Locality.SHARD) return false;
        // Already a broadcast-shape join — at least one input should be REPLICATED.
        OpenSearchDistribution ld = distributionOf(join.getLeft());
        OpenSearchDistribution rd = distributionOf(join.getRight());
        return (ld != null && ld.getLocality() == OpenSearchDistribution.Locality.REPLICATED)
            || (rd != null && rd.getLocality() == OpenSearchDistribution.Locality.REPLICATED);
    }
}
