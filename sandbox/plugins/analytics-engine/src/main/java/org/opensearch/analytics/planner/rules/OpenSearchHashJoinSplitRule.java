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
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.exec.join.MppShufflePartitions;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

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
 *   <li>{@code analytics.mpp.enabled} must be true (the master kill switch covers all MPP
 *       strategies; with MPP disabled this rule must stay out of CBO).</li>
 *   <li>{@code JoinInfo.isEqui()} must be true. Hash partitioning requires equality predicates;
 *       theta joins flow through {@link OpenSearchJoinSplitRule}'s coordinator-centric path.</li>
 *   <li>The resolved partition count (cluster setting → engine default) must be ≥ 2. A count
 *       of 1 means no viable backend supports shuffle (or the cluster has only one node), in
 *       which case BROADCAST or COORDINATOR_CENTRIC is the right strategy instead.</li>
 *   <li>{@code joinAlreadyResolvedAsHash(...)} must be false to avoid re-firing on the rule's
 *       own output.</li>
 * </ul>
 *
 * <p>The rule competes with {@link OpenSearchJoinSplitRule} via Volcano's cost model. The chosen
 * alternative becomes a {@code OpenSearchShuffleExchange} the post-CBO distribution-enforcement pass
 * ({@code DistributionEnforcementPass}) promotes into a worker tier; below the size floor the join
 * stays coordinator-centric.
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
        // The master MPP kill switch must be on. With MPP disabled cluster-wide, the join routes
        // through OpenSearchJoinSplitRule's coord-centric path; this rule must stay out of that.
        if (!AnalyticsSettings.MPP_ENABLED.get(context.getSettings())) {
            return false;
        }
        OpenSearchJoin join = call.rel(0);
        JoinInfo info = join.analyzeCondition();
        // Hash-shuffle requires at least one EQUI key to define the partitioning expression — without
        // it, distTraitDef.hash() would receive an empty key list and produce a partitioning that
        // doesn't actually partition. We do NOT require info.isEqui(): a join may carry equi keys AND a
        // residual non-equi predicate (e.g. TPC-H q14: l_partkey=p_partkey AND l_shipdate BETWEEN …).
        // We hash-partition on info.leftKeys only; onMatch copies the FULL join condition (equi +
        // residual) onto the worker join, so DataFusion's HashJoinExec applies the residual as a join
        // filter after the equi-partition match. A PURE-theta join (no equi key) still bails here
        // (empty leftKeys) and routes coord-centric, since it can't be hash-partitioned.
        if (info.leftKeys.isEmpty()) {
            return false;
        }
        if (joinAlreadyResolvedAsHash(join)) {
            return false;
        }
        // Both inputs must be vanilla SHARD-distributed scans. Without this gate the rule fires
        // on shapes like Join(Scan, Values) — Values has no shard distribution, so the resulting
        // OpenSearchShuffleExchange wraps a non-scan input, and DAGBuilder.cutShuffle's
        // ShardTargetResolver can't find a TableScan and bails out.
        return isShardScan(join.getLeft()) && isShardScan(join.getRight());
    }

    /** True when {@code rel}'s OpenSearchDistribution is SHARD-localized AND its subtree is a
     *  pure shard-scan shape (no Aggregate, no Join, no other reducer between the shuffle
     *  exchange we'd insert and the underlying TableScan). The producer-side wiring for
     *  hash-shuffle ({@link org.opensearch.analytics.exec.AnalyticsSearchService#startFragment})
     *  builds the framework {@code ShuffleSender} only on shard-fragment dispatches, not on
     *  coordinator-reduce stages. If a hash-shuffle plan tried to put a {@code ShuffleExchange}
     *  above an {@code Aggregate(FINAL)} that itself sits above an {@code ExchangeReducer}, the
     *  producer would be a coordinator-reduce stage with no partitioned-sink hookup — the
     *  worker waits forever on senders that never ship. */
    private static boolean isShardScan(RelNode rel) {
        OpenSearchDistribution dist = distributionOf(rel);
        if (dist == null) return false;
        if (dist.getLocality() != OpenSearchDistribution.Locality.SHARD) return false;
        return isPureShardScanShape(rel);
    }

    /** Walks the subtree to make sure no Aggregate / Join sits between this rel and its
     *  underlying TableScan. Project / Filter are allowed (they run on the shard). Anything
     *  else (or no scan reachable) returns false. */
    private static boolean isPureShardScanShape(RelNode rel) {
        if (rel instanceof RelSubset subset) {
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
        RelNode workerJoin = join.copy(
            joinTraits,
            join.getCondition(),
            shuffledLeft,
            shuffledRight,
            join.getJoinType(),
            join.isSemiJoinDone()
        );

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
