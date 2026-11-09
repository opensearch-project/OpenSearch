/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Join rel carrying viable backends. Both sides are gathered SINGLETON to the
 * coordinator (enforced by {@link #computeSelfCost}). {@code right} is always the
 * build side (matches substrait {@code JoinRel.right}).
 *
 * <p>Implements {@link DistributionAware}: under the post-CBO distribution-enforcement pass
 * ({@code DistributionEnforcementPass}), an INNER/LEFT/RIGHT/FULL/SEMI/ANTI equi-join can co-partition
 * on its equi keys — it requires {@code WORKER+HASH(leftKeys,N)} on the left input and
 * {@code WORKER+HASH(rightKeys,N)} on the right, and outputs {@code WORKER+HASH(leftKeys,N)}. That lets a
 * parent join/aggregate keyed on the same column consume the output with no further exchange, so the
 * multi-tier cascade emerges for any chain depth. A pure-theta join (no equi key) imposes no requirement
 * (stays coordinator-gathered).
 *
 * @opensearch.internal
 */
public class OpenSearchJoin extends Join implements OpenSearchRelNode, DistributionAware {

    private final List<String> viableBackends;

    public OpenSearchJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        JoinRelType joinType,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, List.of(), left, right, condition, Set.of(), joinType);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /**
     * Output field storage is the concatenation of left and right input storage —
     * matches Calcite's join row type ordering (left fields first, then right).
     *
     * <p>SEMI / ANTI joins project only the left side — Calcite's {@code Join#getRowType}
     * exposes left fields only in those cases, so our storage metadata must mirror that or
     * downstream walkers (e.g. {@code OpenSearchJoinRule.collectStorageFormats} on a wrapping
     * outer join) index past the row and pick up phantom formats from the right.
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        List<FieldStorageInfo> result = new ArrayList<>();
        appendChildStorage(getLeft(), result);
        if (getJoinType().projectsRight()) {
            appendChildStorage(getRight(), result);
        }
        return result;
    }

    private static void appendChildStorage(RelNode child, List<FieldStorageInfo> out) {
        RelNode unwrapped = RelNodeUtils.unwrapHep(child);
        if (unwrapped instanceof OpenSearchRelNode os) {
            out.addAll(os.getOutputFieldStorage());
        }
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new OpenSearchJoin(getCluster(), traitSet, left, right, conditionExpr, joinType, viableBackends);
    }

    /**
     * Cost gate. The join's locality must match its inputs' locality:
     * <ul>
     *   <li>If the join is at {@code COORDINATOR+SINGLETON}, every input must also be
     *       {@code COORDINATOR+SINGLETON}. {@code OpenSearchJoinSplitRule} drives this
     *       by calling {@code convert(input, COORDINATOR+SINGLETON)} which inserts an ER
     *       wherever the input doesn't already deliver that.</li>
     *   <li>If the join is at {@code SHARD+SINGLETON} (co-location fast path), every input
     *       must also be {@code SHARD+SINGLETON} with the same {@code tableId} and
     *       {@code shardCount=1}. Anything else is infinite cost.</li>
     *   <li>If the join is at {@code WORKER+HASH(keys, N)} (post-shuffle hash join), every
     *       input must also be {@code WORKER+HASH(keys, N)} with the same key set and the
     *       same partition count. {@code OpenSearchHashJoinSplitRule} drives this by
     *       demanding the appropriate per-side HASH on each input; Volcano materializes
     *       an {@link OpenSearchShuffleExchange} on any input not already so distributed.</li>
     * </ul>
     *
     * <p>TODO(trait-propagation): exchange PLACEMENT is already a trait algebra — see
     * {@link DistributionAware#requiredInputDistribution}/{@link DistributionAware#deriveOutputDistribution}
     * on this class, which the post-CBO {@code DistributionEnforcementPass} consults (the
     * {@code passThroughTraits}/{@code deriveTraits} logic expressed as plain methods). Join-ALGORITHM
     * selection (broadcast/shuffle/coord) still rides this cost gate because Volcano runs bottom-up. A
     * future migration to top-down mode ({@code setTopDownOpt} + Calcite {@code PhysicalNode} hooks)
     * would fold this derivation into the trait machinery and let this override shrink; deferred as a
     * separate refactor, not a correctness blocker.
     */
    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(
        org.apache.calcite.plan.RelOptPlanner planner,
        org.apache.calcite.rel.metadata.RelMetadataQuery mq
    ) {
        OpenSearchDistribution selfDist = distributionOf(this);
        if (selfDist == null) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        org.apache.calcite.rel.RelDistribution.Type selfType = selfDist.getType();
        OpenSearchDistribution.Locality selfLocality = selfDist.getLocality();
        // Three legal join shapes:
        // 1. SINGLETON: COORDINATOR+SINGLETON (coord-centric) or SHARD+SINGLETON (1-shard
        // co-location). Inputs match self exactly.
        // 2. HASH+WORKER: hash-shuffle. Inputs are both HASH+WORKER with the same N.
        // 3. RANDOM+SHARD: broadcast. Inputs are one BROADCAST+REPLICATED (build) and one
        // SHARD-localized (probe); the join runs alongside the probe scan.
        boolean isSingleton = selfType == org.apache.calcite.rel.RelDistribution.Type.SINGLETON;
        boolean isHashWorker = selfType == org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED
            && selfLocality == OpenSearchDistribution.Locality.WORKER;
        boolean isBroadcastShape = selfType == org.apache.calcite.rel.RelDistribution.Type.RANDOM_DISTRIBUTED
            && selfLocality == OpenSearchDistribution.Locality.SHARD;
        if (!isSingleton && !isHashWorker && !isBroadcastShape) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        // For broadcast shape, exactly one input must be BROADCAST+REPLICATED (the build) and
        // the other must be SHARD-localized matching the join's own SHARD+tableId.
        int broadcastBuildSeen = 0;
        int probeShardSeen = 0;
        for (RelNode input : getInputs()) {
            OpenSearchDistribution inputDist = distributionOf(input);
            if (inputDist == null) continue;
            if (inputDist.getType() == org.apache.calcite.rel.RelDistribution.Type.ANY) continue;

            if (isBroadcastShape) {
                if (inputDist.getType() == org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED
                    && inputDist.getLocality() == OpenSearchDistribution.Locality.REPLICATED) {
                    broadcastBuildSeen++;
                    continue;
                }
                if (inputDist.getType() == org.apache.calcite.rel.RelDistribution.Type.RANDOM_DISTRIBUTED
                    && inputDist.getLocality() == OpenSearchDistribution.Locality.SHARD
                    && selfDist.getTableId() != null
                    && selfDist.getTableId().equals(inputDist.getTableId())) {
                    probeShardSeen++;
                    continue;
                }
                return planner.getCostFactory().makeInfiniteCost();
            }

            // Non-broadcast shapes: inputs must match join's distribution type.
            if (inputDist.getType() != selfType) {
                return planner.getCostFactory().makeInfiniteCost();
            }
            if (selfDist.getLocality() != inputDist.getLocality()) {
                return planner.getCostFactory().makeInfiniteCost();
            }
            if (isSingleton) {
                if (selfDist.getLocality() == OpenSearchDistribution.Locality.SHARD) {
                    if (selfDist.getTableId() == null || !selfDist.getTableId().equals(inputDist.getTableId())) {
                        return planner.getCostFactory().makeInfiniteCost();
                    }
                    if (!Integer.valueOf(1).equals(inputDist.getShardCount())) {
                        return planner.getCostFactory().makeInfiniteCost();
                    }
                }
            } else {
                // HASH+WORKER: partitionCount must agree on each input. Per-input keys may
                // differ (left.k1 = right.k2), so we don't compare keys here — that's the
                // exchange's job at trait conversion.
                if (!Integer.valueOf(selfDist.getPartitionCount() == null ? -1 : selfDist.getPartitionCount())
                    .equals(inputDist.getPartitionCount())) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
            }
        }
        if (isBroadcastShape && (broadcastBuildSeen != 1 || probeShardSeen != 1)) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        return planner.getCostFactory().makeTinyCost();
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            org.apache.calcite.plan.RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }

    // ---- DistributionAware (Option B post-CBO enforcement pass) ----

    /**
     * An equi-join co-partitions on its equi keys: input 0 (left) must deliver
     * {@code WORKER+HASH(leftKeys, N)}, input 1 (right) {@code WORKER+HASH(rightKeys, N)}. A pure-theta
     * join (empty {@code leftKeys}) returns {@code null} — no key to hash-partition on, so it stays
     * coordinator-gathered. Co-partitioning is sound for all of INNER/LEFT/RIGHT/FULL/SEMI/ANTI: a
     * hash-partitioned outer/semi/anti join's null-fill / existence test is partition-local because rows
     * with the same key land in the same partition (standard Spark/Presto). The per-row null semantics
     * live in the worker join operator, not the distribution.
     */
    @Override
    public OpenSearchDistribution requiredInputDistribution(int inputIndex, int partitionCount, OpenSearchDistributionTraitDef traitDef) {
        JoinInfo info = analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            return null;
        }
        if (inputIndex == 0) {
            return traitDef.hash(info.leftKeys, partitionCount);
        }
        if (inputIndex == 1) {
            return traitDef.hash(info.rightKeys, partitionCount);
        }
        return null;
    }

    /**
     * When the left input is hash-partitioned on this join's left equi keys, the join output is
     * {@code WORKER+HASH(leftKeys, N)} — left key columns keep their output positions (left fields come
     * first in the join row type), so a parent keyed on the same column consumes it without a re-shuffle.
     * Anchored on the LEFT side only (the engine convention used by {@code OpenSearchHashJoinSplitRule} and
     * the cost gate). Returns {@code null} (output not co-partitionable) when the left input is not
     * hash-partitioned on exactly the left equi keys, or for a pure-theta join.
     */
    @Override
    public OpenSearchDistribution deriveOutputDistribution(
        List<OpenSearchDistribution> childDistributions,
        OpenSearchDistributionTraitDef traitDef
    ) {
        if (childDistributions.size() != 2) {
            return null;
        }
        OpenSearchDistribution leftDist = childDistributions.get(0);
        if (leftDist == null || leftDist.getType() != org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED) {
            return null;
        }
        JoinInfo info = analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            return null;
        }
        // Left input must be hash-partitioned on exactly this join's left equi keys (order-sensitive)
        // for the output-is-left-keys derivation to be sound.
        if (!leftDist.getKeys().equals(info.leftKeys)) {
            return null;
        }
        Integer n = leftDist.getPartitionCount();
        if (n == null) {
            return null;
        }
        return traitDef.hash(info.leftKeys, n);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchJoin(
            getCluster(),
            getTraitSet(),
            children.get(0),
            children.get(1),
            getCondition(),
            getJoinType(),
            List.of(backend)
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalJoin.create(
            strippedChildren.get(0),
            strippedChildren.get(1),
            List.of(),
            getCondition(),
            Set.<CorrelationId>of(),
            getJoinType()
        );
    }
}
