/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexOver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule;

import java.util.ArrayList;
import java.util.List;

/**
 * Post-CBO RelNode rewrite that turns a multi-way join's coordinator-gathered inputs into
 * hash-shuffle inputs, so {@code DAGBuilder} cuts a cascaded shuffle DAG instead of leaving the
 * outer join coordinator-centric.
 *
 * <p><b>Why.</b> CBO emits the bottom join with {@code OpenSearchShuffleExchange} inputs (both are
 * pure shard scans, which {@code OpenSearchHashJoinSplitRule} accepts), but the outer join with
 * {@code OpenSearchExchangeReducer} inputs — the split rule rejects a nested-join input, and CBO
 * can't form a re-shuffle of a worker result in the Volcano memo (verified). The execution layer's
 * shuffle is a post-CBO DAG rewrite anyway (see {@code HashShuffleDAGRewriter}), so we complete the
 * picture here: rewrite each {@code OpenSearchJoin}'s {@code ExchangeReducer} inputs into
 * {@code ShuffleExchange} inputs keyed by that side's equi-join keys. DAGBuilder's {@code cutShuffle}
 * then cuts proper shuffle producer stages at every level.
 *
 * <p>Only INNER equi-joins are rewritten (the {@code HashShuffleDispatch} cascade handles those);
 * a join with no equi keys, or a non-reducer input, is left untouched so coordinator-centric /
 * the existing single-level shuffle still apply.
 *
 * @opensearch.internal
 */
public final class CascadeShufflePlanRewriter {

    private static final Logger LOGGER = LogManager.getLogger(CascadeShufflePlanRewriter.class);

    private CascadeShufflePlanRewriter() {}

    /**
     * Returns {@code plan} with every {@link OpenSearchJoin}'s {@link OpenSearchExchangeReducer}
     * inputs replaced by {@link OpenSearchShuffleExchange} inputs (hash-keyed on the join's per-side
     * equi keys, {@code partitionCount} partitions). Recurses first so nested joins are rewritten
     * bottom-up. A new tree is returned only where something changed; unchanged subtrees are shared.
     */
    public static RelNode rewrite(RelNode plan, int partitionCount) {
        if (partitionCount <= 1) {
            return plan;
        }
        return rewriteNode(plan, partitionCount);
    }

    /**
     * Seed pass for the agg-over-join shape (TPC-H q2/q11): when a decomposable aggregate sits above a
     * partition-preserving INNER-join chain whose BOTTOM join CBO kept coordinator-centric (both inputs
     * {@link OpenSearchExchangeReducer}), convert that bottom join's reducer inputs to
     * {@link OpenSearchShuffleExchange}s so the join becomes a liftable cascade level. The downstream
     * machinery ({@link #rewrite} extend, {@link CascadeShuffleDAGRewriter#isCascade},
     * {@link DistributedAggOverJoinRewriter}) then lifts the join onto a worker, broadcasts the
     * dimensions, and pushes a {@code PARTIAL} aggregate below the coordinator gather — exactly the
     * proven q5/q10 path.
     *
     * <p><b>Why a separate seed.</b> CBO natively shuffles only a join over two pure shard scans it
     * judged large×large (q5's {@code customer ⋈ orders} bottom). q2/q11's bottom join is large×small
     * ({@code partsupp(8M) ⋈ supplier(100K)}): even with a corrected join cardinality, coordinator-centric
     * wins the cost race because the 8M join output must gather to SINGLETON for the parent dimension
     * join either way — so broadcasting the small dimension does not avoid the 8M coordinator gather. The
     * decisive win comes only from pushing the PARTIAL aggregate below that gather, which requires the
     * bottom join to be shuffle-shaped first. This seed supplies that shape; without an aggregate above
     * (no PARTIAL to push) it does nothing, so it never force-shuffles a bare large×small join CBO
     * deliberately kept coordinator-centric.
     *
     * <p><b>Safety.</b> Only fires when {@link #findDecomposableAggOverInnerChainBottomJoin} matches the
     * full q5/q10-class shape (decomposable {@code Aggregate(SINGLE)} with a non-empty group over a
     * partition-preserving {@code Project}/{@code Filter}/INNER-{@code Join} chain bottoming out at a
     * single INNER equi-join whose larger side exceeds {@code minBottomJoinRows}). Each side is shuffled
     * via {@link #scanSubtreeToShuffle}, which enforces the same partition-preserving-subtree gate as the
     * cascade extend ({@link #isPartitionPreservingCascadeInput}) and bails unless BOTH sides convert. A
     * too-small bottom join (below the row floor) is left coordinator-centric — the gather is bounded, so
     * there is no OOM motivation to distribute.
     *
     * @param minBottomJoinRows row floor: only seed when the bottom join's larger input exceeds this
     *     (keeps small joins coordinator-centric). {@code <= 0} disables the floor (seed whenever shaped).
     */
    public static RelNode seedAggOverJoinBottomShuffle(RelNode plan, int partitionCount, long minBottomJoinRows) {
        if (partitionCount <= 1) {
            return plan;
        }
        return seedAggOverJoinBottomShuffleNode(plan, partitionCount, minBottomJoinRows);
    }

    private static RelNode seedAggOverJoinBottomShuffleNode(RelNode node, int partitionCount, long minBottomJoinRows) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        List<RelNode> newInputs = new ArrayList<>(n.getInputs().size());
        boolean changed = false;
        for (RelNode input : n.getInputs()) {
            RelNode rewritten = seedAggOverJoinBottomShuffleNode(input, partitionCount, minBottomJoinRows);
            newInputs.add(rewritten);
            if (rewritten != RelNodeUtils.unwrapHep(input) && rewritten != input) {
                changed = true;
            }
        }
        RelNode rebuilt = changed ? n.copy(n.getTraitSet(), newInputs) : n;

        OpenSearchJoin bottom = findDecomposableAggOverInnerChainBottomJoin(rebuilt, minBottomJoinRows);
        if (bottom == null) {
            return rebuilt;
        }
        JoinInfo info = bottom.analyzeCondition();
        // Force BOTH inputs to a hash shuffle on this join's per-side equi keys, unwrapping whatever
        // distribution CBO chose for each side (a BROADCAST of the small side, a SINGLETON reducer, or a
        // bare scan) down to the underlying data subtree. This produces the liftable
        // {@code Join(ShuffleExchange, ShuffleExchange)} cascade shape the q5/q10 machinery recognizes.
        RelNode newLeft = scanSubtreeToShuffle(bottom.getLeft(), info.leftKeys, partitionCount);
        RelNode newRight = scanSubtreeToShuffle(bottom.getRight(), info.rightKeys, partitionCount);
        if (newLeft == null || newRight == null) {
            LOGGER.debug(
                "agg-over-join seed: bottom join {} not shuffleable (left={}, right={})",
                bottom.getId(),
                newLeft != null,
                newRight != null
            );
            return rebuilt;
        }
        if (newLeft == bottom.getLeft() && newRight == bottom.getRight()) {
            return rebuilt;
        }
        LOGGER.debug("agg-over-join seed: shuffling bottom join {} (both sides) for PARTIAL push", bottom.getId());
        OpenSearchJoin seeded = (OpenSearchJoin) bottom.copy(bottom.getTraitSet(), List.of(newLeft, newRight));
        return replaceNode(rebuilt, bottom, seeded);
    }

    /**
     * TPC-H q2/q11 scalar-subquery split: when the coordinator root is
     * {@code Sort?/Project?/Filter? → theta Join → [agg-over-join, agg-over-join]}, wrap both
     * aggregate inputs in SINGLETON {@link OpenSearchExchangeReducer}s. {@code DAGBuilder} then cuts
     * each aggregate subtree into its own child stage while the theta join remains coordinator-only
     * over two {@code StageInputScan} leaves.
     *
     * <p>The qualification of each theta input intentionally reuses
     * {@link #findDecomposableAggOverInnerChainBottomJoin}: the same decomposable aggregate gate and
     * deepest INNER equi-join path validation used by the q2/q11 seed must hold before we introduce a
     * synthetic cut. Pure theta joins without two qualifying aggregate inputs are left unchanged, so
     * the generic theta-join contract stays coordinator-centric and never shuffled.
     */
    public static RelNode splitThetaJoinOverAggInputs(RelNode plan) {
        return splitThetaJoinOverAggInputsNode(plan);
    }

    private static RelNode splitThetaJoinOverAggInputsNode(RelNode node) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        RelNode splitAtRoot = splitThetaJoinOverAggInputsAtRoot(n);
        if (splitAtRoot != n) {
            return splitAtRoot;
        }

        List<RelNode> newInputs = new ArrayList<>(n.getInputs().size());
        boolean changed = false;
        for (RelNode input : n.getInputs()) {
            RelNode rewritten = splitThetaJoinOverAggInputsNode(input);
            newInputs.add(rewritten);
            if (rewritten != RelNodeUtils.unwrapHep(input) && rewritten != input) {
                changed = true;
            }
        }
        return changed ? n.copy(n.getTraitSet(), newInputs) : n;
    }

    private static RelNode splitThetaJoinOverAggInputsAtRoot(RelNode root) {
        List<RelNode> aboveJoin = new ArrayList<>();
        RelNode n = root;
        while (n instanceof OpenSearchSort
            || (n instanceof OpenSearchProject p && !RexOver.containsOver(p.getProjects(), null))
            || n instanceof OpenSearchFilter) {
            if (n.getInputs().size() != 1) {
                return root;
            }
            aboveJoin.add(n);
            n = RelNodeUtils.unwrapHep(n.getInput(0));
        }
        if (!(n instanceof OpenSearchJoin join) || join.getJoinType() != JoinRelType.INNER) {
            return root;
        }
        JoinInfo info = join.analyzeCondition();
        if (info.leftKeys.isEmpty() == false) {
            return root;
        }
        RelNode left = RelNodeUtils.unwrapHep(join.getLeft());
        RelNode right = RelNodeUtils.unwrapHep(join.getRight());
        if (!isSplittableAggOverJoinInput(left) || !isSplittableAggOverJoinInput(right)) {
            return root;
        }

        RelNode newLeft = wrapSingletonReducerIfNeeded(left);
        RelNode newRight = wrapSingletonReducerIfNeeded(right);
        if (newLeft == left && newRight == right) {
            return root;
        }
        RelNode rebuilt = join.copy(join.getTraitSet(), List.of(newLeft, newRight));
        for (int i = aboveJoin.size() - 1; i >= 0; i--) {
            RelNode op = aboveJoin.get(i);
            rebuilt = op.copy(op.getTraitSet(), List.of(rebuilt));
        }
        LOGGER.debug("theta agg-over-join split: cut both inputs of theta join {}", join.getId());
        return rebuilt;
    }

    private static boolean isSplittableAggOverJoinInput(RelNode input) {
        RelNode n = RelNodeUtils.unwrapHep(input);
        if (n instanceof OpenSearchExchangeReducer reducer) {
            n = RelNodeUtils.unwrapHep(reducer.getInput());
        }
        return findDecomposableAggOverInnerChainBottomJoin(n, /* minRows */ 0L) != null;
    }

    private static RelNode wrapSingletonReducerIfNeeded(RelNode input) {
        RelNode n = RelNodeUtils.unwrapHep(input);
        if (n instanceof OpenSearchExchangeReducer) {
            return n;
        }
        List<String> viableBackends = ((OpenSearchRelNode) n).getViableBackends();
        return new OpenSearchExchangeReducer(n.getCluster(), n.getTraitSet(), n, viableBackends);
    }

    /**
     * Wraps a bottom-join input in an {@link OpenSearchShuffleExchange} keyed on {@code keys}, after
     * peeling the exchange CBO placed on it ({@link OpenSearchBroadcastExchange} of the small side, an
     * {@link OpenSearchExchangeReducer}, or none). The peeled subtree must be partition-preserving and
     * row-wise (the same {@link #isPartitionPreservingCascadeInput} gate the cascade extend uses), else
     * returns {@code null} (caller bails). The shuffled subtree runs as a per-partition producer.
     */
    private static RelNode scanSubtreeToShuffle(RelNode input, List<Integer> keys, int partitionCount) {
        RelNode n = RelNodeUtils.unwrapHep(input);
        // Peel CBO's chosen exchange to reach the data subtree.
        if (n instanceof OpenSearchBroadcastExchange broadcast) {
            n = RelNodeUtils.unwrapHep(broadcast.getInput());
        } else if (n instanceof OpenSearchExchangeReducer reducer) {
            n = RelNodeUtils.unwrapHep(reducer.getInput());
        }
        if (n instanceof OpenSearchShuffleExchange) {
            // CBO already shuffled this side (the q5 native case) — leave it.
            return n;
        }
        if (!isPartitionPreservingCascadeInput(n)) {
            return null;
        }
        List<String> viableBackends = ((OpenSearchRelNode) n).getViableBackends();
        return new OpenSearchShuffleExchange(n.getCluster(), n.getTraitSet(), n, keys, partitionCount, viableBackends);
    }

    /**
     * Finds the bottom INNER equi-join of a decomposable-aggregate-over-INNER-join-chain plan whose two
     * inputs are both {@link OpenSearchExchangeReducer} (CBO kept it coordinator-centric) and whose larger
     * input exceeds {@code minRows}. Returns {@code null} when the plan is not that shape.
     *
     * <p>The shape mirrors {@link DistributedAggOverJoinRewriter#analyze} but at the RelNode level (pre-DAG):
     * an optional chain of {@code Sort}/{@code Project}(no window)/{@code Filter}, then an
     * {@code Aggregate(SINGLE)} that is decomposable ({@link OpenSearchAggregateSplitRule#shouldSkipPartialFinalSplit}
     * false) with a non-empty group set, then a partition-preserving chain of {@code Project}(no window)/{@code Filter}/
     * INNER-{@code Join} down to a single reducer-fed INNER equi-join. The aggregate gates ensure a PARTIAL can
     * actually be pushed; without it the seed would shuffle a join that stays coordinator-centric anyway.
     */
    static OpenSearchJoin findDecomposableAggOverInnerChainBottomJoin(RelNode plan, long minRows) {
        RelNode n = RelNodeUtils.unwrapHep(plan);
        // Peel the coordinator ops above the aggregate (Sort / Project-without-window / Filter), single-input only.
        while (n instanceof OpenSearchSort
            || (n instanceof OpenSearchProject p && !RexOver.containsOver(p.getProjects(), null))
            || n instanceof OpenSearchFilter) {
            if (n.getInputs().size() != 1) {
                return null;
            }
            n = RelNodeUtils.unwrapHep(n.getInput(0));
        }
        if (!(n instanceof OpenSearchAggregate aggregate) || aggregate.getMode() != AggregateMode.SINGLE) {
            return null;
        }
        // Decomposable aggregate (same gate DistributedAggOverJoinRewriter.analyze applies). Empty-group
        // is ALLOWED here: although the aggregate output is one row, the JOIN output feeding it can be
        // huge (the q2/q11 scalar subquery: empty-group SUM/MIN over an 8M-row join), so distributing the
        // join + pushing a PARTIAL still caps the coordinator gather. The rewriter closes the empty-group
        // COUNT→SUM nullability gap with wrapWithCastIfNeeded.
        if (OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit(aggregate)) {
            return null;
        }
        // Find the deepest (bottom) join — the one CBO kept coordinator-centric / broadcast — and seed
        // it. The path from the aggregate down to it must be partition-preserving (INNER joins /
        // Project-without-window / Filter, possibly wrapped in reducers/broadcasts CBO inserted).
        OpenSearchJoin bottom = deepestInnerEquiJoin(RelNodeUtils.unwrapHep(aggregate.getInput()));
        if (bottom == null) {
            return null;
        }
        // Size floor: only seed a genuinely large bottom join (small ones gather safely → keep
        // coord-centric). Measured on the larger input's scan subtree.
        long maxRows = Math.max(subtreeMaxScanRows(bottom.getInput(0)), subtreeMaxScanRows(bottom.getInput(1)));
        if (minRows > 0 && maxRows < minRows) {
            LOGGER.debug(
                "agg-over-join seed: bottom join {} below row floor ({} < {}), keeping coord-centric",
                bottom.getId(),
                maxRows,
                minRows
            );
            return null;
        }
        return bottom;
    }

    /**
     * Walks the partition-preserving coordinator path from {@code node} down to the bottom INNER equi-join
     * and returns that join, or {@code null} if the path or the bottom join is unsafe to seed.
     *
     * <p><b>The path must be fully partition-preserving.</b> Only {@link OpenSearchProject} (no window),
     * {@link OpenSearchFilter}, and INNER {@link OpenSearchJoin} (each dimension join over the probe chain)
     * are descended; a LEFT/RIGHT/FULL/SEMI/ANTI join, or an intervening {@code Aggregate}/{@code Sort}/
     * {@code Union}, makes the whole shape unsafe — running the lifted bottom join per-partition under such
     * an operator would silently change results (and {@code DistributedAggOverJoinRewriter.canPushPartial}
     * would reject it anyway, leaving an already-shuffled bottom join that the generic HashShuffleDispatch
     * could then mis-lift). Validating the FULL path here — not just the bottom join — mirrors the
     * {@code DistributedAggOverJoinRewriter.collectBroadcastableDims} gate the rewriter applies downstream,
     * so the seed only fires on a shape the rewriter will actually accept. (codex review finding #1)
     *
     * <p>At each dimension join exactly one input must contain the bottom join (the probe side, descended)
     * and the other must be a dimension that contains NO join (a leaf/reducer/broadcast subtree). The
     * bottom join itself must be an INNER equi-join with ≥1 equi key.
     */
    private static OpenSearchJoin deepestInnerEquiJoin(RelNode node) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        // Descend the partition-preserving chain. Exchange wrappers CBO inserted (ExchangeReducer of a
        // gathered probe subtree, Broadcast/Shuffle of a side) are transparent to the join-structure walk
        // — peel them. Project(no-window) / Filter are row-wise. Anything else (Aggregate / Sort / Union /
        // Limit) makes the path unsafe.
        if (n instanceof OpenSearchExchangeReducer || n instanceof OpenSearchBroadcastExchange || n instanceof OpenSearchShuffleExchange) {
            return deepestInnerEquiJoin(n.getInput(0));
        }
        if (n instanceof OpenSearchProject project) {
            if (RexOver.containsOver(project.getProjects(), null)) {
                return null;
            }
            return deepestInnerEquiJoin(project.getInput(0));
        }
        if (n instanceof OpenSearchFilter) {
            return deepestInnerEquiJoin(n.getInput(0));
        }
        if (!(n instanceof OpenSearchJoin join) || join.getJoinType() != JoinRelType.INNER) {
            return null;
        }
        JoinInfo info = join.analyzeCondition();
        if (!info.isEqui() || info.leftKeys.isEmpty()) {
            return null;
        }
        RelNode left = RelNodeUtils.unwrapHep(join.getInput(0));
        RelNode right = RelNodeUtils.unwrapHep(join.getInput(1));
        boolean leftHasJoin = subtreeContainsJoin(left);
        boolean rightHasJoin = subtreeContainsJoin(right);
        // Bottom join: neither input contains a deeper join. Seed it.
        if (!leftHasJoin && !rightHasJoin) {
            return join;
        }
        // Dimension join: EXACTLY one side contains the deeper (probe) join; the other must be a
        // join-free dimension. A bushy shape (both sides contain joins) is not the left-deep q5/q11
        // pattern — bail. Descend the probe side only.
        if (leftHasJoin == rightHasJoin) {
            return null;
        }
        return deepestInnerEquiJoin(leftHasJoin ? left : right);
    }

    /** True iff {@code node}'s subtree contains an {@link OpenSearchJoin}. */
    private static boolean subtreeContainsJoin(RelNode node) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        if (n instanceof OpenSearchJoin) {
            return true;
        }
        for (RelNode input : n.getInputs()) {
            if (subtreeContainsJoin(input)) {
                return true;
            }
        }
        return false;
    }

    /** Largest {@link OpenSearchTableScan} row count in {@code node}'s subtree (0 when no scan / unknown). */
    private static long subtreeMaxScanRows(RelNode node) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        if (n instanceof OpenSearchTableScan scan) {
            return Math.max(0L, (long) scan.getTable().getRowCount());
        }
        long max = 0L;
        for (RelNode input : n.getInputs()) {
            max = Math.max(max, subtreeMaxScanRows(input));
        }
        return max;
    }

    /** Returns {@code root} with the single node {@code target} (by reference identity) replaced by
     *  {@code replacement}, rebuilding only the ancestors on the path. */
    private static RelNode replaceNode(RelNode root, RelNode target, RelNode replacement) {
        RelNode r = RelNodeUtils.unwrapHep(root);
        if (r == target) {
            return replacement;
        }
        List<RelNode> newInputs = new ArrayList<>(r.getInputs().size());
        boolean changed = false;
        for (RelNode input : r.getInputs()) {
            RelNode rewritten = replaceNode(input, target, replacement);
            newInputs.add(rewritten);
            if (rewritten != RelNodeUtils.unwrapHep(input)) {
                changed = true;
            }
        }
        return changed ? r.copy(r.getTraitSet(), newInputs) : r;
    }

    private static RelNode rewriteNode(RelNode node, int partitionCount) {
        // Rewrite children first (bottom-up), then this node.
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteNode(input, partitionCount);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        RelNode rebuilt = changed ? node.copy(node.getTraitSet(), newInputs) : node;

        if (rebuilt instanceof OpenSearchJoin join) {
            return shuffleJoinInputs(join, partitionCount);
        }
        return rebuilt;
    }

    /**
     * If {@code join} is an INNER equi-join whose inputs arrive via {@link OpenSearchExchangeReducer},
     * swaps each reducer for a {@link OpenSearchShuffleExchange} keyed on that side's join keys.
     * Returns the join unchanged when it isn't an equi-join or its inputs aren't reducers.
     */
    private static RelNode shuffleJoinInputs(OpenSearchJoin join, int partitionCount) {
        JoinInfo info = join.analyzeCondition();
        if (!info.isEqui() || info.leftKeys.isEmpty()) {
            return join;
        }
        // INNER only. The cascade dispatch + hash partitioning is correct for INNER equi joins;
        // LEFT/RIGHT/FULL/SEMI/ANTI need null-side / build-side-preservation semantics the cascade
        // worker does not yet implement, so they stay on their CBO-chosen path (coord-centric).
        // FOLLOW-UP: extend the cascade to outer/semi/anti multi-way joins — TPC-H q13 (LEFT outer
        // customer⋈orders) will hit this and otherwise stays coord-centric → coordinator OOM at scale.
        if (join.getJoinType() != JoinRelType.INNER) {
            return join;
        }
        // Only cascade-shuffle a join that sits over an already-shuffled subtree. CBO emits a
        // shuffle only for a join it judged large (both inputs are sizable shard scans); a join
        // whose child already shuffles is therefore itself large and would overflow the
        // coordinator if left COORDINATOR_CENTRIC. A join with no shuffle below (small inputs,
        // or a broadcast/coord-centric subtree) is left untouched so the "small probe → coord
        // -centric" cost decision still holds. This is the heuristic that keeps the rewrite from
        // force-shuffling joins CBO deliberately kept on the coordinator.
        if (!subtreeContainsShuffle(join.getLeft()) && !subtreeContainsShuffle(join.getRight())) {
            return join;
        }
        // PARTITION-PRESERVING shape required (enforced per-side inside maybeReducerToShuffle):
        // converting a reducer to a shuffle makes its input run as a per-partition producer
        // (hash-partitioned by THIS join's keys). That is only correct when the operators between
        // this join and the lower shuffle are partition-preserving — Project/Filter and the inner
        // INNER-join. A non-preserving operator (Aggregate, Sort, Limit, Union) between join levels
        // would run PER PARTITION (keyed by the join key, not its own group/sort key) and silently
        // produce wrong global results; maybeReducerToShuffle leaves such a side as a reducer, and
        // the both-sides-shuffled check below then bails the whole join to its CBO-chosen path.
        // Mirrors OpenSearchHashJoinSplitRule.isPureShardScanShape. See codex review R3 blocker.
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelNode newLeft = maybeReducerToShuffle(left, info.leftKeys, partitionCount);
        RelNode newRight = maybeReducerToShuffle(right, info.rightKeys, partitionCount);
        if (newLeft == left && newRight == right) {
            return join;
        }
        // Both inputs must end up hash-shuffled — a cascaded worker join reads two partitioned
        // streams. If only one side converted (the other was not a reducer — e.g. a broadcast or
        // bare-scan input), the result would be a mixed shuffle/non-shuffle join that DAGBuilder
        // can't cut into a valid worker tier. Bail to the original join in that case.
        if (!(newLeft instanceof OpenSearchShuffleExchange leftShuffle) || !(newRight instanceof OpenSearchShuffleExchange rightShuffle)) {
            return join;
        }
        // Hash-key correctness: each side must be partitioned on THIS join's equi keys (in that
        // side's index space). Freshly-created shuffles use info.left/rightKeys by construction; a
        // PRE-EXISTING shuffle input (the newX == X case — e.g. CBO already shuffled the bottom
        // join) must be partitioned on the SAME keys, else the worker join would hash-match rows on
        // the wrong columns. If a side carries a stale/different key set, bail to coord-centric
        // rather than silently joining on mis-partitioned data. (codex review R5 should-fix)
        if (!leftShuffle.getHashKeys().equals(info.leftKeys) || !rightShuffle.getHashKeys().equals(info.rightKeys)) {
            return join;
        }
        return join.copy(join.getTraitSet(), List.of(newLeft, newRight));
    }

    /** True iff {@code node}'s subtree already contains an {@link OpenSearchShuffleExchange} —
     *  i.e. a lower join level was costed into a hash shuffle by CBO. */
    private static boolean subtreeContainsShuffle(RelNode node) {
        if (node instanceof OpenSearchShuffleExchange) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (subtreeContainsShuffle(input)) {
                return true;
            }
        }
        return false;
    }

    /** Replaces an {@link OpenSearchExchangeReducer} with an {@link OpenSearchShuffleExchange} over
     *  the SAME input, hash-keyed on {@code keys}. Non-reducer inputs (already a ShuffleExchange, a
     *  bare scan, etc.) pass through unchanged. A reducer whose subtree is NOT a partition-preserving
     *  join chain (e.g. it has an Aggregate / Sort / Limit between this level and the inner join) is
     *  also left unchanged — hash-partitioning across workers would run that operator per-partition
     *  and silently change global aggregate/sort/limit semantics. */
    private static RelNode maybeReducerToShuffle(RelNode input, List<Integer> keys, int partitionCount) {
        if (!(input instanceof OpenSearchExchangeReducer reducer)) {
            return input;
        }
        RelNode reducerInput = reducer.getInput();
        // Only cascade-shuffle when everything between this reducer and the data source is
        // partition-preserving and row-wise (Project / Filter / the inner Join / Shuffle / Scan).
        // An Aggregate / Sort / Limit / Union here is NOT safe to run per-partition.
        if (!isPartitionPreservingCascadeInput(reducerInput)) {
            return input;
        }
        List<String> viableBackends = ((OpenSearchRelNode) reducer).getViableBackends();
        return new OpenSearchShuffleExchange(
            reducer.getCluster(),
            reducer.getTraitSet(),
            reducerInput,
            keys,
            partitionCount,
            viableBackends
        );
    }

    /**
     * True iff {@code node}'s subtree is a CASCADE-SAFE chain: only partition-preserving, row-wise
     * operators ({@link OpenSearchProject}, {@link OpenSearchFilter}) sit above the data source,
     * which must be an inner {@link OpenSearchJoin} (already shuffle-shaped — the lower cascade
     * level), an {@link OpenSearchShuffleExchange}, or a bare {@link OpenSearchTableScan}.
     *
     * <p>Rejects {@link org.opensearch.analytics.planner.rel.OpenSearchAggregate},
     * {@link org.opensearch.analytics.planner.rel.OpenSearchSort} (carries Limit/fetch), and
     * {@link org.opensearch.analytics.planner.rel.OpenSearchUnion}: running any of those once
     * per hash partition (instead of globally) yields per-partition partial groups / per-partition
     * top-K, silently wrong. Such a join input keeps its CBO-chosen coordinator-centric path.
     */
    static boolean isPartitionPreservingCascadeInput(RelNode node) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        if (n instanceof OpenSearchShuffleExchange || n instanceof OpenSearchTableScan) {
            return true;
        }
        if (n instanceof OpenSearchJoin join) {
            // A nested join is cascade-safe ONLY when it is itself a VALIDATED cascade level: an
            // INNER join whose two inputs are already OpenSearchShuffleExchange. Returning true for
            // any join (codex R4 blocker #1) let shapes like Join(Join(Aggregate(Join(A,B)),D),E)
            // through — the middle join was never converted (its Aggregate side blocked it), yet the
            // outer guard saw "an OpenSearchJoin" and cascaded, leaving an unlifted coordinator-reduce
            // stage that the dispatcher would enrich as a producer that never ships partitions (hang).
            if (join.getJoinType() != JoinRelType.INNER) {
                return false;
            }
            return RelNodeUtils.unwrapHep(join.getInput(0)) instanceof OpenSearchShuffleExchange
                && RelNodeUtils.unwrapHep(join.getInput(1)) instanceof OpenSearchShuffleExchange;
        }
        if (n instanceof OpenSearchProject project) {
            // Project is row-wise ONLY when it has no window functions. A RexOver (window) needs the
            // full, gathered input — running it per hash partition corrupts the window (codex R4
            // blocker #2). Reject any window-bearing project; it keeps its coord-centric path.
            if (RexOver.containsOver(project.getProjects(), null)) {
                return false;
            }
            return isPartitionPreservingCascadeInput(n.getInput(0));
        }
        if (n instanceof OpenSearchFilter) {
            // Single-input, row-wise: recurse into the one input.
            return isPartitionPreservingCascadeInput(n.getInput(0));
        }
        // Aggregate / Sort / Limit / Union / Values / anything else → not cascade-safe.
        return false;
    }
}
