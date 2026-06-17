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
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

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
