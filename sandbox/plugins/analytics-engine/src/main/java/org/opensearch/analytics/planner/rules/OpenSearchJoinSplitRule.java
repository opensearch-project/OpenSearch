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
import org.opensearch.analytics.exec.join.MppShufflePartitions;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

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
 *   <li>Equi joins with {@code analytics.mpp.enabled=true}: only when neither MPP rule will
 *       fire. Specifically, coord-centric SUPPRESSES itself iff broadcast OR hash would emit
 *       a viable alternative — meaning {@code probeNodes > 1} and the join type has an
 *       eligible build side (broadcast), OR partition count {@literal > 1} (hash). When
 *       neither MPP rule can produce (single-node cluster, FULL OUTER equi join, single-shard
 *       inputs), coord-centric must still fire so Volcano has a plan to satisfy the root
 *       SINGLETON demand.</li>
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
        // Contract: this rule produces COORDINATOR_CENTRIC. Suppress it only when at least
        // one MPP rule (broadcast or hash) will actually produce a viable alternative for
        // this join — otherwise Volcano has no plan to satisfy the root SINGLETON demand
        // and throws CannotPlanException.
        //
        // Concretely we suppress coord only if:
        // - mpp.enabled=true (gate on broadcast + hash rules)
        // - join is equi (theta is structurally ineligible for both MPP rules)
        // - AND at least one of broadcast/hash will fire and produce a non-empty alt:
        // broadcast: probeNodes > 1 AND joinType not FULL OUTER (FULL has no eligible
        // build side; broadcast emits zero alternatives)
        // hash: partitionCount > 1
        // - both inputs are multi-shard SHARD scans (the structural check both MPP rules
        // impose; otherwise neither fires)
        //
        // Single-node clusters (probeNodes=1) still get coord-centric for all equi joins:
        // broadcast bails on probeNodes <= 1, and hash bails on partitionCount <= 1 (a
        // single-node cluster's defaultShuffleParallelism is 1). With neither MPP rule
        // viable, coord-rule suppression doesn't kick in.
        if (!shouldSuppressCoord(join)) {
            return true;
        }
        return false;
    }

    /** True iff at least one MPP rule (broadcast or hash) will produce a viable alternative
     *  for this join. Coord rule suppresses itself only when this returns true; otherwise it
     *  fires so Volcano has a plan. */
    private boolean shouldSuppressCoord(OpenSearchJoin join) {
        if (!AnalyticsSettings.MPP_ENABLED.get(context.getSettings())) {
            return false;
        }
        JoinInfo info = join.analyzeCondition();
        // Mirror the MPP rules' eligibility EXACTLY: they require at least one equi key (non-empty
        // leftKeys) but tolerate a residual non-equi predicate (they no longer require info.isEqui()
        // — e.g. TPC-H q14: l_partkey=p_partkey AND l_shipdate BETWEEN …). So coord suppresses itself
        // whenever there is ≥1 equi key (then a broadcast/hash alternative will be produced). With NO
        // equi key (pure theta / cross), neither MPP rule fires, so coord must stay enabled or Volcano
        // can't satisfy the root SINGLETON demand.
        if (info.leftKeys.isEmpty()) {
            return false;
        }
        // Both MPP rules need multi-shard SHARD scans on both sides. Single-shard or non-
        // scan inputs (Values, Aggregate, Project(Aggregate(...))) make both rules dormant.
        if (!bothInputsCouldBeMppShardScans(join)) {
            return false;
        }
        // OK, the structural shape can support an MPP alternative. Now check whether at
        // least one of broadcast or hash will actually fire.
        return broadcastWillFire(join) || hashWillFire(join);
    }

    /** Broadcast emits at least one alternative when probeNodes > 1 AND the join type has at
     *  least one eligible build side. FULL OUTER has no eligible side (broadcast can't
     *  replicate either side without breaking row-preservation semantics). */
    private boolean broadcastWillFire(OpenSearchJoin join) {
        // Resolve probe-node estimate the same way OpenSearchBroadcastJoinSplitRule does:
        // setting override (positive) wins; otherwise data-node count.
        Integer override = AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE.get(context.getSettings());
        int probeNodes;
        if (override != null && override > 0) {
            probeNodes = override;
        } else {
            org.opensearch.cluster.ClusterState state = context.getClusterState();
            if (state == null || state.nodes() == null) {
                probeNodes = 1;
            } else {
                probeNodes = Math.max(state.nodes().getDataNodes().size(), 1);
            }
        }
        if (probeNodes <= 1) {
            return false;
        }
        JoinRelType jt = join.getJoinType();
        // Mirror OpenSearchBroadcastJoinSplitRule.onMatch: at least one of left-as-build or
        // right-as-build must be eligible. INNER, LEFT, RIGHT, SEMI, ANTI all have an
        // eligible side; FULL has none.
        boolean leftAsBuild = jt == JoinRelType.INNER || jt == JoinRelType.RIGHT;
        boolean rightAsBuild = jt == JoinRelType.INNER || jt == JoinRelType.LEFT || jt == JoinRelType.SEMI || jt == JoinRelType.ANTI;
        return leftAsBuild || rightAsBuild;
    }

    /** Hash-shuffle emits an alternative when a viable backend reports a partition count > 1. */
    private boolean hashWillFire(OpenSearchJoin join) {
        // Resolve partition count via the same helper OpenSearchHashJoinSplitRule uses.
        // Need viableBackends from the OpenSearchJoin to feed into the resolver.
        if (!(join instanceof OpenSearchRelNode osRel)) {
            return false;
        }
        int partitionCount = MppShufflePartitions.resolve(
            context.getSettings(),
            context.getClusterState(),
            context.getCapabilityRegistry(),
            osRel.getViableBackends()
        );
        return partitionCount > 1;
    }

    /** True when both inputs originate from SHARD-distributed scans with shardCount > 1 AND
     *  the input subtree is a "pure" shard scan shape (no Aggregate/Join between the input
     *  and the underlying TableScan). Both broadcast and hash split rules require this:
     *  the producer-side wiring for shuffle and the build-side capture for broadcast both
     *  assume SHARD-fragment dispatch, not coordinator-reduce. If an input is post-aggregate
     *  (e.g. {@code stats by str0 | join …}), neither MPP rule fires, so coord-rule must
     *  not suppress itself or Volcano can't plan. */
    private static boolean bothInputsCouldBeMppShardScans(OpenSearchJoin join) {
        return isPureShardScanInput(join.getLeft()) && isPureShardScanInput(join.getRight());
    }

    /** Combines the multi-shard distribution check with the pure-shard-scan-shape walk that
     *  matches what {@link OpenSearchHashJoinSplitRule} and
     *  {@link OpenSearchBroadcastJoinSplitRule} actually require. */
    private static boolean isPureShardScanInput(RelNode rel) {
        OpenSearchDistribution dist = originalDistribution(rel);
        if (!isMultiShard(dist)) return false;
        return isPureShardScanShape(rel);
    }

    /** Walks the subtree to make sure no Aggregate / Join sits between this rel and its
     *  underlying TableScan. Project / Filter are allowed (they run on the shard). */
    private static boolean isPureShardScanShape(RelNode rel) {
        if (rel instanceof org.apache.calcite.plan.volcano.RelSubset subset) {
            RelNode best = subset.getBestOrOriginal();
            if (best == null || best == rel) return false;
            return isPureShardScanShape(best);
        }
        if (rel instanceof OpenSearchTableScan) return true;
        if (rel instanceof OpenSearchAggregate) return false;
        if (rel instanceof OpenSearchJoin) return false;
        for (RelNode input : rel.getInputs()) {
            if (!isPureShardScanShape(input)) return false;
        }
        return !rel.getInputs().isEmpty();
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
