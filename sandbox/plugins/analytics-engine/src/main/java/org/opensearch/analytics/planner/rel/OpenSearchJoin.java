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
import org.opensearch.analytics.planner.JoinKeyAnalysis;
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
 * <p>Distribution is a search dimension, not a fixed shape: an INNER/LEFT/RIGHT/FULL/SEMI/ANTI equi-join
 * can co-partition on its equi keys, requiring {@code WORKER+HASH(leftKeys,N)} on the left input and
 * {@code WORKER+HASH(rightKeys,N)} on the right. That lets a parent join/aggregate keyed on the same
 * column consume the output with no further exchange, so the multi-tier cascade emerges for any chain
 * depth. A pure-theta join (no equi key) has no key to partition on and stays coordinator-gathered.
 * {@link #passThroughTraits} and {@link #deriveTraits} express this to Calcite's top-down planner; only
 * RIGHT and FULL decline to ADVERTISE the hash output, because their null-extended rows carry NULL left
 * keys and never passed through the left-key hash.
 *
 * @opensearch.internal
 */
public class OpenSearchJoin extends Join implements OpenSearchRelNode {

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
     * <p>These are ASSERTED, not priced — see {@link #assertPlacementIsLegal}. Only two things remain real
     * costs here: an input whose placement is still UNRESOLVED (no defined cost, and the marking phase
     * legitimately registers such seeds), and the join's own execution cost divided by the parallelism its
     * distribution buys.
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
        // The marking phase's UNRESOLVED seed. It claims no placement, so there is nothing to assert and no
        // parallelism to price; it must not be consumable, which infinite cost secures. Its concrete
        // alternatives come from passThroughTraits / deriveTraits and the split rules.
        if (selfDist.getType() == org.apache.calcite.rel.RelDistribution.Type.ANY) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        // An UNRESOLVED INPUT cannot be consumed: its placement is undecided, so nothing above it has a
        // defined cost or a defined correctness. This ONE invariant does the work the shape-by-shape legality
        // table used to, and it cannot become an assertion for the reason above — seeds are legal nodes.
        if (hasUnresolvedInput()) {
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
        boolean isHashWorker = selfType == org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED
            && selfLocality == OpenSearchDistribution.Locality.WORKER;
        boolean isBroadcastShape = selfType == org.apache.calcite.rel.RelDistribution.Type.RANDOM_DISTRIBUTED
            && selfLocality == OpenSearchDistribution.Locality.SHARD;
        assert assertPlacementIsLegal(selfDist, isHashWorker, isBroadcastShape);
        // Charge the join for its work DIVIDED by the parallelism its distribution buys — without this a
        // coordinator join prices the same as an N-way distributed one and always wins. The divisor comes
        // from cluster facts: the shuffle partition count for a worker-tier hash join, or the probe-node
        // estimate for a broadcast (carried in the REPLICATED input's partitionCount slot).
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

    /**
     * The (self, left, right) placements a join can correctly execute at. ASSERTED, not priced — these were
     * ten {@code makeInfiniteCost()} branches, i.e. legality expressed through the channel that exists for
     * ranking. Each is now unreachable because every builder states self and input traits TOGETHER:
     * {@link #passThroughTraits} declines any demand but SINGLETON, {@link #deriveTraits} emits only the
     * three shapes, and the split rules construct each shape explicitly alongside the
     * {@code convert(input, …)} that makes the inputs match. The marking rule no longer claims a placement at
     * all, which is what removed the last builder able to register an illegal pair.
     *
     * <p>Asserted rather than deleted because a violation is a silently WRONG RESULT, not a slow plan — a
     * hash join whose sides carry different partition counts joins rows that never co-locate, and a broadcast
     * shape with no probe has no execution location. Assertions are on in the suites
     * ({@code OpenSearchJoinCostGateTests} covers every branch), so a builder that breaks one fails there.
     *
     * @return always {@code true}, so this reads as {@code assert assertPlacementIsLegal(...)}
     * @throws IllegalStateException when the join meets inputs it cannot correctly consume
     */
    private boolean assertPlacementIsLegal(OpenSearchDistribution selfDist, boolean isHashWorker, boolean isBroadcastShape) {
        org.apache.calcite.rel.RelDistribution.Type selfType = selfDist.getType();
        boolean isSingleton = selfType == org.apache.calcite.rel.RelDistribution.Type.SINGLETON;
        if (!isSingleton && !isHashWorker && !isBroadcastShape) {
            throw new IllegalStateException("Join at a shape it cannot execute at [" + selfDist + "]");
        }
        // For broadcast shape, exactly one input must be BROADCAST+REPLICATED (the build) and
        // the other must be SHARD-localized matching the join's own SHARD+tableId.
        int broadcastBuildSeen = 0;
        int probeShardSeen = 0;
        for (RelNode input : getInputs()) {
            OpenSearchDistribution inputDist = distributionOf(input);
            if (inputDist == null) continue;

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
                throw new IllegalStateException(
                    "Broadcast join input is neither a REPLICATED build nor a matching SHARD probe [" + inputDist + "]"
                );
            }

            // Non-broadcast shapes: inputs must match join's distribution type.
            if (inputDist.getType() != selfType) {
                throw new IllegalStateException("Join at [" + selfDist + "] over input of a different type [" + inputDist + "]");
            }
            if (selfDist.getLocality() != inputDist.getLocality()) {
                throw new IllegalStateException("Join at [" + selfDist + "] over input of a different locality [" + inputDist + "]");
            }
            if (isSingleton) {
                if (selfDist.getLocality() == OpenSearchDistribution.Locality.SHARD) {
                    if (selfDist.getTableId() == null || !selfDist.getTableId().equals(inputDist.getTableId())) {
                        throw new IllegalStateException("Co-located join over an input from another table [" + inputDist + "]");
                    }
                    if (!Integer.valueOf(1).equals(inputDist.getShardCount())) {
                        throw new IllegalStateException("Co-located join over a multi-shard input [" + inputDist + "]");
                    }
                }
            } else {
                // HASH+WORKER: partitionCount must agree on each input. Per-input keys may
                // differ (left.k1 = right.k2), so we don't compare keys here — that's the
                // exchange's job at trait conversion.
                if (!Integer.valueOf(selfDist.getPartitionCount() == null ? -1 : selfDist.getPartitionCount())
                    .equals(inputDist.getPartitionCount())) {
                    throw new IllegalStateException(
                        "Hash join whose partition count disagrees with its input's [" + selfDist + "] vs [" + inputDist + "]"
                    );
                }
            }
        }
        if (isBroadcastShape && (broadcastBuildSeen != 1 || probeShardSeen != 1)) {
            throw new IllegalStateException(
                "Broadcast join needs exactly one REPLICATED build and one SHARD probe, saw "
                    + broadcastBuildSeen
                    + " and "
                    + probeShardSeen
            );
        }
        return true;
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
        // Answers a SINGLETON demand with the COORDINATOR shape deliberately, even for a locality-agnostic
        // demand that two co-located 1-shard inputs could satisfy where they sit. Passing the demand through
        // saves one gather on a single-shard join, but the join then never sees a coordinator-gathered pair,
        // and its broadcast alternative loses to a plain shuffle on much larger inputs — a bad trade.
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

        JoinInfo info = JoinKeyAnalysis.forDistribution(this);
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
