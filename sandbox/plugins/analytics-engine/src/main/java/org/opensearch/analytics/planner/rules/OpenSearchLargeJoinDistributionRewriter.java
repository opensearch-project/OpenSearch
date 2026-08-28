/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.exec.join.MppShufflePartitions;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Post-CBO rewrite: distributes a join that CBO chose to GATHER, when its inputs are large enough to be worth
 * a worker tier. Turns {@code Join / {ER, ER}} into {@code ER / Join@WORKER+HASH / {SHUFFLE, SHUFFLE}}.
 *
 * <p>Peer of {@link OpenSearchPartialAggregatePushdownRewriter}, and runs immediately BEFORE it so an
 * aggregate above a promoted join can then be split across the new gather.
 *
 * <p><b>This is a POLICY, not enforcement — and it is explicitly a stopgap.</b> CBO already forms the
 * distributed alternative for a two-way join over shard scans; what it will not do is CHOOSE it for an upper
 * join of a multi-way tree, for two independent reasons:
 * <ol>
 *   <li>{@code OpenSearchHashJoinSplitRule.isPureShardScanShape} rejects an input whose subtree contains a
 *       Join, so the upper join never gets a {@code WORKER+HASH} alternative at all; and</li>
 *   <li>even where the alternative exists, cost prefers gathering — coord-centric ships the inputs once
 *       while distributing ships the inputs AND the (larger) result back. That accounting is correct on DATA
 *       MOVEMENT; distribution's payoff is CPU parallelism and per-node MEMORY, and the cost model has no
 *       memory dimension. A cost model that cannot represent "this will not fit" cannot prefer the plan
 *       that fits.</li>
 * </ol>
 *
 * <p><b>Measured worth (analytics-bench sf=10, one variable):</b> without this promotion q3/q5/q7/q11/q21 all
 * fail with {@code ReduceSizeExceededException} — the coordinator gathering raw upper-join input at ~1.3 GB —
 * and the sweep scores 14/22 instead of 18/22.
 *
 * <p><b>Exit condition.</b> Delete this class once exchange cost carries a memory/spill term (so reason 2
 * disappears) and the hash-join input gate is relaxed (so reason 1 disappears). Both are CBO-side changes;
 * this rewrite exists only because neither has landed. It deliberately does NOT thread demands, peel CBO's
 * exchanges, or re-decide placement anywhere else — the single decision it makes is "promote this gathered
 * join", which is why it is ~150 lines rather than the 840 of the enforcement pass it replaces.
 *
 * @opensearch.internal
 */
public final class OpenSearchLargeJoinDistributionRewriter {

    private static final Logger LOGGER = LogManager.getLogger(OpenSearchLargeJoinDistributionRewriter.class);

    private OpenSearchLargeJoinDistributionRewriter() {}

    /** Promotes every eligible gathered join in {@code plan}; empty when nothing matched. */
    public static Optional<RelNode> rewrite(RelNode plan, PlannerContext context) {
        if (!AnalyticsSettings.MPP_ENABLED.get(context.getSettings())) {
            return Optional.empty();
        }
        OpenSearchDistributionTraitDef traitDef = context.getDistributionTraitDef();
        if (traitDef == null) {
            return Optional.empty();
        }
        long minRows = AnalyticsSettings.MPP_DISTRIBUTE_MIN_ROWS.get(context.getSettings());
        boolean[] changed = new boolean[1];
        RelNode out = visit(plan, context, traitDef, minRows, changed);
        return changed[0] ? Optional.of(out) : Optional.empty();
    }

    private static RelNode visit(
        RelNode node,
        PlannerContext context,
        OpenSearchDistributionTraitDef traitDef,
        long minRows,
        boolean[] changed
    ) {
        RelNode n = RelNodeUtils.unwrapHep(node);

        List<RelNode> newInputs = new ArrayList<>(n.getInputs().size());
        boolean inputChanged = false;
        for (RelNode input : n.getInputs()) {
            RelNode rewritten = visit(input, context, traitDef, minRows, changed);
            inputChanged |= rewritten != RelNodeUtils.unwrapHep(input);
            newInputs.add(rewritten);
        }
        if (inputChanged) {
            n = n.copy(n.getTraitSet(), newInputs);
        }
        if (!(n instanceof OpenSearchJoin join)) {
            return n;
        }
        return promote(join, context, traitDef, minRows, changed);
    }

    /** Replaces a gathered join's two reducers with shuffles and re-gathers above it; identity if ineligible. */
    private static RelNode promote(
        OpenSearchJoin join,
        PlannerContext context,
        OpenSearchDistributionTraitDef traitDef,
        long minRows,
        boolean[] changed
    ) {
        // Only a COORDINATOR-gathered join is a candidate. A join already at WORKER+HASH was distributed by
        // CBO, and one at RANDOM+SHARD is a broadcast probe — neither must be touched.
        OpenSearchDistribution selfDist = OpenSearchRelNode.distributionOf(join.getTraitSet());
        if (selfDist == null || selfDist.getLocality() != OpenSearchDistribution.Locality.COORDINATOR) {
            return join;
        }
        // Which join types may be promoted. The requirement is only that hash-partitioning EACH INPUT on its
        // own equi keys co-locates every row that could match — true for INNER, LEFT, RIGHT, SEMI and ANTI, and
        // exactly what OpenSearchHashJoinSplitRule already does for a two-way join (it carries no join-type
        // filter, and HashShuffleJoinIT covers the LEFT/RIGHT cases end-to-end). SEMI/ANTI project only the
        // left side and emit no null-extension, so they are safe here too — needed for the decorrelated
        // subquery shapes, e.g. TPC-H q21, which otherwise gathers and trips ReduceSizeExceededException.
        //
        // FULL is included too. The usual objection — its null-extended rows carry NULL keys on BOTH sides, so a
        // parent cannot rely on the output partitioning (why OpenSearchJoin.advertisesLeftKeyHash declines
        // RIGHT/FULL) — does not apply HERE, because promote() always wraps the result in buildReducer(): the
        // join's partitioning is re-gathered immediately and never exposed to a parent. What matters for the
        // join itself is only that each input is hash-partitioned on its own equi keys, so every pair that
        // could match co-locates and unmatched rows are null-extended by whichever partition holds them.
        // HashShuffleJoinIT covers FULL end-to-end.
        //
        // ASOF/LEFT_ASOF stay out: they are temporal nearest-match joins whose matching is not equi on the
        // hash keys, so co-partitioning does not guarantee a match lands in the same partition.
        switch (join.getJoinType()) {
            case INNER, LEFT, RIGHT, FULL, SEMI, ANTI -> {
            }
            default -> {
                return join;
            }
        }
        JoinInfo info = join.analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            return join;
        }
        // Take each input's CONTENT: the payload under a plain gather, or the input itself when CBO placed no
        // exchange (which it does not when the child is already coordinator-local — e.g. a lower join it also
        // gathered). Requiring an ER on BOTH sides matched almost nothing: measured rejects were
        // left=Project / left=Aggregate / left=Join against right=ExchangeReducer.
        RelNode leftContent = gatherContent(RelNodeUtils.unwrapHep(join.getInput(0)));
        RelNode rightContent = gatherContent(RelNodeUtils.unwrapHep(join.getInput(1)));
        if (leftContent == null || rightContent == null) {
            return join;
        }
        if (!canProduceShuffle(leftContent) || !canProduceShuffle(rightContent)) {
            return join;
        }
        // Size floor: a shuffle round-trip only pays off at scale. 0 means UNKNOWN (no reachable scan), which
        // must not veto.
        long estimated = Math.max(RelNodeUtils.subtreeMaxScanRows(leftContent), RelNodeUtils.subtreeMaxScanRows(rightContent));
        if (minRows > 0 && estimated > 0 && estimated < minRows) {
            return join;
        }
        // Second floor, on what would ACTUALLY be shipped. The scan floor above deliberately digs past every
        // Filter/Join to the base table, so a query that scans a large table but ships a small FILTERED result
        // clears it and then pays a full shuffle round-trip for nothing — the promotion's whole justification is
        // the size of what crosses the gather, not the size of the tables underneath. Estimating the input
        // EXPRESSION sees the reduction that the scan walk cannot.
        //
        // Layered as an EXTRA veto rather than a replacement, because the two estimators fail in opposite
        // directions: a missing index statistic leaves a scan at Calcite's default row count, which the scan
        // floor already reads as small, whereas replacing the scan floor outright would make an un-estimatable
        // plan look small and silently stop distributing the queries this rewrite exists for.
        if (minRows > 0 && estimated > 0 && shippedRowsBelow(join, leftContent, rightContent, minRows)) {
            return join;
        }
        int partitionCount = MppShufflePartitions.resolve(
            context.getSettings(),
            context.getClusterState(),
            context.getCapabilityRegistry(),
            join.getViableBackends()
        );
        if (partitionCount <= 1) {
            return join;
        }
        // buildShuffleExchange / buildReducer FORCE their exchange rather than going through the
        // satisfies()-gated buildEnforcer: the content's traitSet still carries CBO's coordSingleton, so a
        // gated enforcer would insert NOTHING and DAGBuilder would never cut the worker boundary.
        RelNode shuffledLeft = traitDef.buildShuffleExchange(leftContent, traitDef.hash(info.leftKeys, partitionCount));
        RelNode shuffledRight = traitDef.buildShuffleExchange(rightContent, traitDef.hash(info.rightKeys, partitionCount));
        OpenSearchDistribution joinHash = traitDef.hash(info.leftKeys, partitionCount);
        RelNode workerJoin = join.copy(
            join.getTraitSet().replace(joinHash),
            join.getCondition(),
            shuffledLeft,
            shuffledRight,
            join.getJoinType(),
            join.isSemiJoinDone()
        );
        changed[0] = true;
        // Re-gather so every consumer above still sees the SINGLETON it was planned against. When that
        // consumer is an aggregate, OpenSearchPartialAggregatePushdownRewriter then splits it across this
        // gather, which is what shrinks the coordinator's input from raw join output to group count.
        return traitDef.buildReducer(workerJoin);
    }

    /**
     * The payload to shuffle for a join input: the child of a plain {@link OpenSearchExchangeReducer}, or the
     * node itself when there is no exchange to replace. {@code null} for a QTF reducer, whose declared
     * coord-side {@code ___ugsi} column only survives if the reducer stays intact.
     */
    private static RelNode gatherContent(RelNode input) {
        if (input instanceof OpenSearchExchangeReducer er) {
            return er.hasOverrideRowType() ? null : RelNodeUtils.unwrapHep(er.getInput(0));
        }
        return input;
    }

    /**
     * True when BOTH inputs are estimated to ship fewer than {@code minRows} rows, i.e. the shuffle would move
     * less data than the floor considers worth a round-trip.
     *
     * <p>Reads the cluster's {@link RelMetadataQuery}, which is the OpenSearch subclass that corrects Calcite's
     * no-statistics equi-join estimate, so a join input is estimated from its own keys rather than as a near
     * cartesian product. Uses {@code max} of the two sides to mirror the scan floor: one large side is enough
     * to justify distributing, since both sides get shuffled either way.
     *
     * <p>Declines to veto whenever the estimate is unusable — absent, non-finite, or non-positive. An estimator
     * that cannot answer must never be read as "small", or a metadata gap would silently switch distribution
     * off for the plans that need it most.
     *
     * <p><b>Why this reads the estimate as-is.</b> The estimate cannot see a filter applied to a join's SMALLER
     * side, because the FK correction in {@code OpenSearchRelMetadataQuery} is {@code max(left, right)} and a
     * selective dimension predicate never moves the max. Two attempts to re-apply that missing factor both
     * regressed a multi-way query: correcting it inside the metadata query fed the broadcast split rule's
     * pre-flight byte gate and let an over-sized build through, and correcting it locally here had to compound
     * survival ratios up a long join chain, which collapsed the estimate and vetoed a promotion that query
     * needed. A single row floor cannot separate "scans much, gathers little" from "scans much, gathers much
     * through a deep chain" — that needs the memory/spill term in exchange cost named in this class's exit
     * condition, not a better row guess.
     */
    private static boolean shippedRowsBelow(OpenSearchJoin join, RelNode leftContent, RelNode rightContent, long minRows) {
        RelMetadataQuery mq = join.getCluster().getMetadataQuery();
        if (mq == null) {
            return false;
        }
        Double left = safeRowCount(mq, leftContent);
        Double right = safeRowCount(mq, rightContent);
        LOGGER.debug(
            "[LargeJoinDistribution] shipped-row estimate for {} join: left={} right={} minRows={} scanFloorLeft={} scanFloorRight={}",
            join.getJoinType(),
            left,
            right,
            minRows,
            RelNodeUtils.subtreeMaxScanRows(leftContent),
            RelNodeUtils.subtreeMaxScanRows(rightContent)
        );
        if (left == null || right == null) {
            return false;
        }
        return Math.max(left, right) < minRows;
    }

    /** {@code mq.getRowCount} guarded against a null/NaN/infinite/non-positive answer and metadata failures. */
    private static Double safeRowCount(RelMetadataQuery mq, RelNode rel) {
        try {
            Double rows = mq.getRowCount(rel);
            if (rows == null || rows.isNaN() || rows.isInfinite() || rows <= 0.0) {
                return null;
            }
            return rows;
        } catch (RuntimeException e) {
            // A metadata handler can throw on an unusual node (a cyclic-metadata guard, a missing handler for a
            // custom rel). Treat that as "no estimate" rather than letting it fail the whole query, since this
            // is only a cost heuristic.
            LOGGER.debug("[LargeJoinDistribution] row-count estimate unavailable, not vetoing promotion", e);
            return null;
        }
    }

    /**
     * True when the shuffle producer cut below this content has a partitioned-sink hookup: a shard-scan-shaped
     * fragment, a join that becomes its own worker producer stage, or an aggregate whose coordinator-reduce
     * stage resolves an owned producer sink from its instruction chain.
     */
    private static boolean canProduceShuffle(RelNode rel) {
        RelNode n = RelNodeUtils.unwrapHep(rel);
        if (n instanceof OpenSearchTableScan) return true;
        // An AGGREGATE below the cut makes the producer a coordinator-reduce stage. That is allowed:
        // ReduceStageExecutionFactory resolves a partitioned sink from the stage's OWN instruction chain when it
        // carries a ShuffleProducerOutputState, so a gathered sub-stage can ship partitions. The wiring is
        // instruction-driven and ungated, and it fails LOUDLY (IllegalStateException "its partitions would never
        // be shipped and the consuming worker would hang") rather than hanging if the sender deps are missing.
        // Rejecting this shape is what kept every join above a decorrelated subquery coordinator-centric —
        // measured: TPC-H q21 (exists/not-exists over lineitem) then gathers and trips
        // ReduceSizeExceededException at sf=10.
        if (n instanceof OpenSearchJoin || n instanceof OpenSearchAggregate) {
            return RelNodeUtils.subtreeMaxScanRows(n) > 0;
        }
        if (n.getInputs().isEmpty()) return false;
        for (RelNode input : n.getInputs()) {
            if (!canProduceShuffle(input)) return false;
        }
        return true;
    }
}
