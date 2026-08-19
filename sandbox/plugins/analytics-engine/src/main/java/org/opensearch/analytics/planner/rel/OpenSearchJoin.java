/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rules.OpenSearchBroadcastJoinSplitRule;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.cluster.ClusterState;

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
        // Beyond the legality gate above, charge the join for the work it does — DIVIDED by the
        // parallelism its distribution buys. This is what makes a distributed plan win on its own merit:
        // the same rows are processed, but across N workers instead of one coordinator.
        //
        // Previously every legal shape returned makeTinyCost(), so parallelism was not modelled ANYWHERE
        // and a coordinator join looked exactly as cheap as an N-way distributed one — only the exchanges
        // differed. Under bottom-up Volcano that was masked (the coordinator alternative was only
        // reachable via a split rule that self-suppresses when an MPP rule fires), but top-down can
        // synthesize the coordinator plan directly, so the missing credit made it win every time.
        //
        // Parallelism comes from real cluster facts, not a tuned constant: the shuffle partition count
        // for a worker-tier hash join, or the probe-node estimate for a broadcast (the same value the
        // broadcast exchange's own cost model scales by, carried in the REPLICATED input's
        // partitionCount slot).
        double inputRows = mq.getRowCount(getLeft()) + mq.getRowCount(getRight());
        int parallelism = 1;
        if (isHashWorker && selfDist.getPartitionCount() != null) {
            parallelism = Math.max(1, selfDist.getPartitionCount());
        } else if (isBroadcastShape) {
            for (RelNode input : getInputs()) {
                OpenSearchDistribution inputDist = distributionOf(input);
                if (inputDist != null
                    && inputDist.getType() == org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED
                    && inputDist.getPartitionCount() != null) {
                    parallelism = Math.max(1, inputDist.getPartitionCount());
                    break;
                }
            }
        }
        double executionCost = inputRows / parallelism;
        return planner.getCostFactory().makeCost(executionCost, executionCost, 0);
    }

    // ---- PhysicalNode (top-down trait propagation) ----

    /**
     * A SINGLETON demand is satisfied by gathering BOTH inputs to the coordinator — the coord-centric
     * shape, and legal shape #1 in {@link #computeSelfCost}. Any other demand is declined here and left
     * to {@link #deriveTraits}: a hash or broadcast shape is discovered bottom-up from what an input can
     * actually deliver, not requested top-down, because whether a given side is shuffleable or
     * broadcastable depends on the input subtree rather than on the parent's wish.
     */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        OpenSearchDistribution requiredDistribution = OpenSearchRelNode.distributionOf(required);
        if (requiredDistribution == null || requiredDistribution.getType() != RelDistribution.Type.SINGLETON) {
            return null;
        }
        OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
        OpenSearchDistribution singleton = traitDef.coordSingleton();
        return Pair.of(
            getTraitSet().replace(singleton),
            List.of(getLeft().getTraitSet().replace(singleton), getRight().getTraitSet().replace(singleton))
        );
    }

    /**
     * Derives the join's output from ONE input's distribution, emitting only the shapes
     * {@link #computeSelfCost} accepts:
     * <ul>
     *   <li>{@code COORDINATOR+SINGLETON} on a child → gather the sibling too (shape #1).</li>
     *   <li>{@code WORKER+HASH} on a child whose keys match that side's equi keys → demand the SAME
     *       partition count on the sibling, keyed on ITS equi keys, and output the left keys' hash
     *       (shape #2). Declining on a key mismatch is essential: shuffling on the wrong column is
     *       type-correct but silently produces wrong results.</li>
     * </ul>
     * Everything else (broadcast, single-shard co-location) is left to the existing split rules and the
     * post-CBO enforcement pass, which own the extra context those shapes need — the probe's shard
     * identity and the runtime broadcast-size gate.
     */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        if (childId != 0 && childId != 1) {
            return null;
        }
        OpenSearchDistribution childDistribution = OpenSearchRelNode.distributionOf(childTraits);
        if (childDistribution == null || childDistribution.getType() == RelDistribution.Type.ANY) {
            return null;
        }
        OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) childDistribution.getTraitDef();

        if (childDistribution.getType() == RelDistribution.Type.SINGLETON
            && childDistribution.getLocality() == OpenSearchDistribution.Locality.COORDINATOR) {
            OpenSearchDistribution singleton = traitDef.coordSingleton();
            List<RelTraitSet> inputs = new ArrayList<>(
                List.of(getLeft().getTraitSet().replace(singleton), getRight().getTraitSet().replace(singleton))
            );
            inputs.set(childId, childTraits);
            return Pair.of(getTraitSet().replace(singleton), inputs);
        }

        JoinInfo info = analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            // Pure theta / cross join: no key to partition on, so no distributed shape exists.
            return null;
        }
        if (childDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED
            && childDistribution.getLocality() == OpenSearchDistribution.Locality.WORKER
            && childDistribution.getPartitionCount() != null) {
            List<Integer> expectedKeys = childId == 0 ? info.leftKeys : info.rightKeys;
            if (!childDistribution.getKeys().equals(expectedKeys)) {
                return null;
            }
            int partitionCount = childDistribution.getPartitionCount();
            OpenSearchDistribution leftHash = traitDef.hash(info.leftKeys, partitionCount);
            OpenSearchDistribution rightHash = traitDef.hash(info.rightKeys, partitionCount);
            List<RelTraitSet> inputs = new ArrayList<>(
                List.of(getLeft().getTraitSet().replace(leftHash), getRight().getTraitSet().replace(rightHash))
            );
            inputs.set(childId, childTraits);
            // Both inputs may be co-partitioned on their equi keys for ANY join type — that is what makes
            // the distributed join legal. But only some join types may ADVERTISE the output as still
            // hash-partitioned on the LEFT keys. A RIGHT or FULL outer join emits null-extended rows for
            // unmatched right rows: those rows never passed through the left-key hash, so their left-key
            // columns are NULL and they sit in whichever partition their right key landed in. Claiming
            // HASH(leftKeys) would let a parent join/aggregate keyed on the same column skip its exchange
            // and silently miss matches. Report "unknown" instead and let the parent demand its own
            // exchange. (Songkan's design note calls this out; neither implementation had the gate.)
            OpenSearchDistribution output = advertisesLeftKeyHash() ? leftHash : null;
            if (output == null) {
                return null;
            }
            return Pair.of(getTraitSet().replace(output), inputs);
        }
        // BROADCAST shape: one input is replicated to every probe node, the other stays SHARD-local, and the
        // join runs alongside the probe scan (output RANDOM+SHARD, carrying the probe's tableId / shardCount
        // — the identity computeSelfCost's broadcast gate checks). Without this case a broadcast-shaped child
        // derived NOTHING, so the only distributed alternative CBO could form above a scan was hash-shuffle.
        //
        // Derived from the PROBE side (childId is the probe): the build's BROADCAST+REPLICATED trait says
        // nothing about where the join runs. Note the derived alternative still COMPETES on cost with the
        // shuffle/coord ones — this only makes the broadcast shape reachable, it does not force it.
        if (childDistribution.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED
            && childDistribution.getLocality() == OpenSearchDistribution.Locality.SHARD) {
            if (!broadcastDeriveEnabled(traitDef)) {
                return null;
            }
            // Only the sides the split rule considers build-eligible may be broadcast: the build is
            // duplicated to every probe node, so a join type that must preserve the build side's own rows
            // cannot use it as the build. Same predicate as the rule, so formation paths agree.
            int buildId = 1 - childId;
            if (!broadcastBuildEligible(buildId)) {
                return null;
            }
            int probeNodes = probeNodeEstimate(traitDef);
            if (probeNodes <= 1) {
                // No parallelism to gain over coord-centric (single-node cluster / unstubbed test fixture).
                return null;
            }
            // Same pre-flight size gate the split rule applies: a build whose estimated bytes exceed
            // analytics.mpp.broadcast.max_bytes can never be broadcast at runtime (the capture sink would
            // reject it), so the alternative must not be formed here either — otherwise lowering the cap
            // would stop suppressing broadcast, which is exactly how operators (and
            // testEnforcementPass_filteredScanJoinInputStaysShardProducer) force the shuffle path.
            long maxBytes = AnalyticsSettings.BROADCAST_MAX_BYTES.get(traitDef.getPlannerContext().getSettings()).getBytes();
            if (!OpenSearchBroadcastJoinSplitRule.buildSideFitsBroadcast(getInput(buildId), getCluster().getMetadataQuery(), maxBytes)) {
                return null;
            }
            OpenSearchDistribution probeDist = traitDef.from(childDistribution);
            List<RelTraitSet> inputs = new ArrayList<>(List.of(getLeft().getTraitSet(), getRight().getTraitSet()));
            inputs.set(childId, childTraits);
            inputs.set(buildId, getInput(buildId).getTraitSet().replace(traitDef.broadcast(probeNodes)));
            return Pair.of(getTraitSet().replace(probeDist), inputs);
        }
        return null;
    }

    /**
     * True when input {@code buildId} may serve as a broadcast BUILD side, mirroring
     * {@code OpenSearchBroadcastJoinSplitRule}'s eligibility: the build is replicated to every probe node, so
     * a join type that must preserve the build side's own rows cannot broadcast it. LEFT preserves left rows
     * → only the right may be the build; RIGHT is the mirror; SEMI/ANTI test existence of the right side →
     * build = right; FULL preserves both → neither.
     */
    private boolean broadcastBuildEligible(int buildId) {
        return switch (getJoinType()) {
            case INNER -> true;
            case LEFT, SEMI, ANTI -> buildId == 1;
            case RIGHT -> buildId == 0;
            case FULL, ASOF, LEFT_ASOF -> false;
        };
    }

    /** Whether the broadcast derive is allowed: MPP must be on, and broadcast must not have been made
     *  ineligible for this planning attempt (the re-plan after a runtime broadcast-size overflow). A trait
     *  hook has no {@code matches()}, so the gate lives here — the same conditions the split rule checks. */
    private static boolean broadcastDeriveEnabled(OpenSearchDistributionTraitDef traitDef) {
        PlannerContext context = traitDef.getPlannerContext();
        if (context == null) {
            return false;
        }
        return AnalyticsSettings.MPP_ENABLED.get(context.getSettings()) && context.isBroadcastEligible();
    }

    /** Probe-node estimate for the derived broadcast, resolved exactly as the split rule resolves it: the
     *  {@code analytics.mpp.broadcast.probe_estimate} override, else the cluster's data-node count. */
    private static int probeNodeEstimate(OpenSearchDistributionTraitDef traitDef) {
        PlannerContext context = traitDef.getPlannerContext();
        Integer override = AnalyticsSettings.MPP_BROADCAST_PROBE_ESTIMATE.get(context.getSettings());
        if (override != null && override > 0) {
            return override;
        }
        ClusterState state = context.getClusterState();
        if (state == null || state.nodes() == null) {
            return 1;
        }
        return Math.max(state.nodes().getDataNodes().size(), 1);
    }

    /**
     * True when this join's output really is partitioned by its LEFT equi keys, so a parent may consume it
     * co-partitioned. False for RIGHT/FULL, whose null-extended rows carry NULL left keys and therefore do
     * not obey the left-key hash. SEMI/ANTI project only the left side and emit no null-extension, so their
     * output remains left-key partitioned.
     */
    private boolean advertisesLeftKeyHash() {
        return switch (getJoinType()) {
            case INNER, LEFT, SEMI, ANTI -> true;
            // RIGHT/FULL: null-extended rows carry NULL left keys. ASOF/LEFT_ASOF are temporal
            // nearest-match joins whose output ordering/partitioning we do not model — decline rather
            // than guess, so a parent always demands its own exchange.
            case RIGHT, FULL, ASOF, LEFT_ASOF -> false;
        };
    }

    /** Derive from EITHER input: a join is co-partitionable when either side supplies a usable hash. */
    @Override
    public DeriveMode getDeriveMode() {
        return DeriveMode.BOTH;
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
        // Same RIGHT/FULL restriction as the top-down deriveTraits path: a null-extended row has NULL
        // left keys and does not obey the left-key hash, so such a join must not advertise a
        // co-partitionable output. Without this the cascade in DistributionEnforcementPass can reuse the
        // partitioning and skip a required re-shuffle, silently dropping matches.
        return advertisesLeftKeyHash() ? traitDef.hash(info.leftKeys, n) : null;
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
