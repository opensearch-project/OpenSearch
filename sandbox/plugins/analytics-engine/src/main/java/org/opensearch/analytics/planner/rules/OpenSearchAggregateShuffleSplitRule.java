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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.exec.join.MppShufflePartitions;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Hash-shuffle split rule for {@link OpenSearchAggregate} (M3). Sibling of
 * {@link OpenSearchAggregateSplitRule}. Both fire on {@code mode=SINGLE}; this one emits a
 * HASH-distributed alternative analogous to {@link OpenSearchHashJoinSplitRule} for joins:
 * PARTIAL on shards → hash-shuffle on group keys → FINAL at WORKER+HASH → final gather.
 *
 * <p>The result is a final aggregate that runs on data-node workers in parallel — N workers
 * each merge {@code partial_rows / N} buffers concurrently, instead of the coordinator
 * merging all {@code partial_rows} serially. High-cardinality {@code GROUP BY} (e.g.
 * {@code stats … by user_id} over millions of users) becomes parallel.
 *
 * <p>Gates:
 * <ul>
 *   <li>{@code analytics.mpp.enabled} must be true. With MPP disabled, the aggregate routes
 *       through {@link OpenSearchAggregateSplitRule}'s coord-centric path.</li>
 *   <li>{@code analytics.mpp.shuffle.aggregate.enabled} must be true (default). A targeted
 *       sub-toggle to disable just hash-shuffle aggregation while leaving MPP joins enabled;
 *       when false the aggregate routes through the same coord-centric path as MPP-off.</li>
 *   <li>{@code groupSet} must be non-empty. Without group keys there is no shuffle key, so
 *       coord-centric is the only viable shape.</li>
 *   <li>The resolved partition count (cluster setting → engine default) must be ≥ 2. A count
 *       of 1 means no viable backend supports shuffle (or single-node cluster); fall back to
 *       coord-centric or the existing PARTIAL+gather+FINAL alternative.</li>
 *   <li>{@link OpenSearchAggregateSplitRule#shouldSkipPartialFinalSplit} must return false —
 *       the same correctness gates that block the coord-centric PARTIAL+FINAL split (e.g.
 *       {@code percentile_approx}, cross-family non-prefix groupSet) also block the shuffle
 *       split. Sharing the helper guarantees the two rules agree.</li>
 *   <li>{@code aggregateAlreadyResolvedAsHash(...)} must be false to avoid re-firing on this
 *       rule's own output.</li>
 * </ul>
 *
 * <p>Cost competition with {@link OpenSearchAggregateSplitRule}'s alternatives is driven by
 * the per-exchange cost functions: {@code OpenSearchExchangeReducer.computeSelfCost = SETUP +
 * rows} for the gather-then-coord-final path; {@code OpenSearchShuffleExchange.computeSelfCost
 * = rows + SETUP_PER_PARTITION × N} for the shuffle path. With high-cardinality grouping the
 * shuffle path wins because Calcite's {@code RelMdRowCount} reports {@code partial_rows / N}
 * per worker partition — the parallelism is captured automatically without a custom cost.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregateShuffleSplitRule extends RelOptRule {

    private final PlannerContext context;
    private final OpenSearchDistributionTraitDef distTraitDef;

    public OpenSearchAggregateShuffleSplitRule(PlannerContext context) {
        super(operand(OpenSearchAggregate.class, operand(RelNode.class, any())), "OpenSearchAggregateShuffleSplitRule");
        this.context = context;
        this.distTraitDef = context.getDistributionTraitDef();
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // The master MPP kill switch must be on. With MPP disabled cluster-wide, the aggregate
        // routes through OpenSearchAggregateSplitRule's coord-centric path; this rule must
        // stay out of that.
        if (!AnalyticsSettings.MPP_ENABLED.get(context.getSettings())) {
            return false;
        }
        // Per-strategy sub-toggle: disabling hash-shuffle aggregation routes aggregates through the
        // coord-centric path (same as MPP-off) while leaving MPP joins enabled.
        if (!AnalyticsSettings.MPP_SHUFFLE_AGGREGATE_ENABLED.get(context.getSettings())) {
            return false;
        }
        OpenSearchAggregate aggregate = call.rel(0);
        if (aggregate.getMode() != AggregateMode.SINGLE) {
            return false;
        }
        // No group keys → no shuffle key → falls through to coord-centric path. Aggregates with
        // empty groupSet produce a single output row; parallelizing the merge gains nothing.
        if (aggregate.getGroupSet().isEmpty()) {
            return false;
        }
        if (aggregateAlreadyResolvedAsHash(aggregate)) {
            return false;
        }
        // Same correctness gates as the coord-centric split rule. Sharing the helper guarantees
        // both rules agree on which shapes are unsafe to split.
        if (OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit(aggregate)) {
            return false;
        }
        // Leaf-shard-producer gate: the shuffle producer is only valid when the aggregate's child
        // is a single shard-scan fragment with no exchange below it — exactly the shape
        // DAGBuilder.cutShuffle can cut into one producer stage with a working ShardTargetResolver.
        // Checking only the top-level SHARD locality is NOT enough: a broadcast-join probe fragment
        // Join(OpenSearchBroadcastExchange(build), probeScan) still carries Locality.SHARD (the
        // broadcast split rule copies the probe's SHARD trait onto the join — see
        // OpenSearchBroadcastJoinSplitRule#emitBroadcastAlternative). Cutting a shuffle over that
        // shape is doubly broken: (1) cutShuffle's sever() cuts the nested broadcast exchange into a
        // grandchild stage, which forces targetResolver=null (no shard targeting on the producer),
        // and (2) HashShuffleAggregateDispatch only wires ShuffleProducer/ShuffleScan instructions
        // and never injects the broadcast build-side data, so the join inside the producer hangs
        // waiting for input. Likewise a co-located shard-join child (two scans, no exchange) trips
        // the one-scan-per-stage constraint ShardTargetResolver relies on. Require: SHARD locality
        // AND no exchange node in the subtree AND exactly one table scan. Non-leaf shapes fall back
        // to OpenSearchAggregateSplitRule's coord-centric path.
        if (!isLeafShardProducer(call.rel(1))) {
            return false;
        }
        return true;
    }

    /** True iff {@code rel} is a leaf shard producer: SHARD-localized, with no exchange node in
     *  its subtree and exactly one {@link org.opensearch.analytics.planner.rel.OpenSearchTableScan}.
     *  This is the precise shape {@code DAGBuilder.cutShuffle} can cut into a single producer stage
     *  with a real {@code ShardTargetResolver} (a nested exchange yields a null resolver; a second
     *  scan makes the resolver's first-scan targeting drop rows). Unwraps Volcano {@code RelSubset}
     *  placeholders to their resolved best so the gate reads real nodes during memo expansion. */
    private static boolean isLeafShardProducer(RelNode rel) {
        OpenSearchDistribution dist = originalDistribution(rel);
        if (dist == null || dist.getLocality() != OpenSearchDistribution.Locality.SHARD) {
            return false;
        }
        return !subtreeContainsExchange(rel) && countTableScans(rel) == 1;
    }

    /** True iff {@code rel}'s subtree contains an
     *  {@link org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer},
     *  {@link org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange}, or
     *  {@link org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange} — the three node
     *  types {@code DAGBuilder.sever} cuts into child stages. Their presence means the producer
     *  would have grandchild stages and {@code cutShuffle} would build it with no
     *  {@code ShardTargetResolver}. Unwraps {@code RelSubset} to the resolved best. */
    private static boolean subtreeContainsExchange(RelNode rel) {
        if (rel == null) return false;
        if (rel instanceof org.apache.calcite.plan.volcano.RelSubset subset) {
            RelNode best = subset.getBestOrOriginal();
            return best != rel && subtreeContainsExchange(best);
        }
        if (rel instanceof org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer
            || rel instanceof org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange
            || rel instanceof org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange) {
            return true;
        }
        for (RelNode input : rel.getInputs()) {
            if (subtreeContainsExchange(input)) return true;
        }
        return false;
    }

    /** Counts {@link org.opensearch.analytics.planner.rel.OpenSearchTableScan} nodes in
     *  {@code rel}'s subtree, unwrapping {@code RelSubset} to the resolved best. */
    private static int countTableScans(RelNode rel) {
        if (rel == null) return 0;
        if (rel instanceof org.apache.calcite.plan.volcano.RelSubset subset) {
            RelNode best = subset.getBestOrOriginal();
            return best != rel ? countTableScans(best) : 0;
        }
        int count = rel instanceof org.opensearch.analytics.planner.rel.OpenSearchTableScan ? 1 : 0;
        for (RelNode input : rel.getInputs()) {
            count += countTableScans(input);
        }
        return count;
    }

    private static OpenSearchDistribution originalDistribution(RelNode rel) {
        if (rel == null) return null;
        if (rel instanceof org.apache.calcite.plan.volcano.RelSubset subset) {
            RelNode best = subset.getBestOrOriginal();
            if (best != rel) return originalDistribution(best);
            return null;
        }
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        OpenSearchAggregate aggregate = call.rel(0);
        RelNode child = call.rel(1);

        int partitionCount = MppShufflePartitions.resolve(
            context.getSettings(),
            context.getClusterState(),
            context.getCapabilityRegistry(),
            aggregate.getViableBackends()
        );
        // No backend opts in (or single-node cluster); leave the alternative space to
        // OpenSearchAggregateSplitRule.
        if (partitionCount <= 1) {
            return;
        }

        // PARTIAL stays at the child's locality (typically SHARD). Mirror the lossy-return-type
        // repair the coord-centric split rule does so PARTIAL's output column types match what
        // DataFusion's array_agg actually produces. FINAL keeps the original aggCall list so
        // Volcano's parent row-type check on transformTo passes.
        List<AggregateCall> partialAggCalls = OpenSearchAggregateSplitRule.repairLossyReturnTypes(aggregate.getAggCallList(), child);
        RelTraitSet partialTraits = child.getTraitSet().replace(OpenSearchConvention.INSTANCE);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            aggregate.getCluster(),
            partialTraits,
            child,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            partialAggCalls,
            AggregateMode.PARTIAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations()
        );

        // Demand HASH partitioning on the group keys. Volcano's trait converter materializes
        // an OpenSearchShuffleExchange on the partial-side output. Group keys land at indices
        // [0..groupCount) on PARTIAL's output (it projects group keys then agg outputs)
        // regardless of the original input ordinals — for a non-prefix groupSet like
        // {input col 2}, PARTIAL still emits the group key at output index 0. Hash on the
        // PARTIAL-output positions, NOT the original input ordinals, otherwise the producer
        // would partition on agg-state columns and rows for the same group could be routed to
        // different workers (split / duplicate groups in the FINAL output).
        int groupCount = aggregate.getGroupSet().cardinality();
        List<Integer> hashKeys = new ArrayList<>(groupCount);
        for (int i = 0; i < groupCount; i++) {
            hashKeys.add(i);
        }
        OpenSearchDistribution hashTrait = distTraitDef.hash(hashKeys, partitionCount);
        RelTraitSet shuffledTraits = partial.getTraitSet().replace(hashTrait);
        RelNode shuffled = convert(partial, shuffledTraits);

        // FINAL runs at WORKER+HASH(groupKeys, N). Cost gate accepts this combo (see
        // OpenSearchAggregate.computeSelfCost). Capture state-expanding aggregates' literal
        // args so the FINAL handler can re-attach them — same pattern as the coord path.
        Map<Integer, List<RexLiteral>> finalExtraLiterals = OpenSearchAggregateSplitRule.captureLiteralArgsForFinal(
            aggregate.getAggCallList(),
            child
        );

        // Classify intermediate fields once and rebuild FINAL's aggCalls against the shuffled
        // input so Volcano's typeMatchesInferred passes AND COUNT is decomposed to SUM (over the
        // partial-count state column at index groupCount + i). Mirrors the coord-centric split
        // rule — without this the worker FINAL would run COUNT() over already-counted rows and
        // double-count across shards.
        List<org.opensearch.analytics.spi.AggregateFunction.IntermediateField> intermediateFields =
            org.opensearch.analytics.planner.dag.DistributedAggregateRewriter.FinalAggCallBuilder.classify(aggregate.getAggCallList());
        List<AggregateCall> finalAggCalls = org.opensearch.analytics.planner.dag.DistributedAggregateRewriter.FinalAggCallBuilder
            .buildFinalCalls(
                aggregate.getAggCallList(),
                intermediateFields,
                aggregate.getGroupSet().cardinality(),
                shuffled,
                aggregate.getGroupSet().isEmpty()
            );

        RelTraitSet finalTraits = aggregate.getTraitSet().replace(hashTrait);
        OpenSearchAggregate finalAggregate = new OpenSearchAggregate(
            aggregate.getCluster(),
            finalTraits,
            shuffled,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            finalAggCalls,
            AggregateMode.FINAL,
            aggregate.getViableBackends(),
            aggregate.getCallAnnotations(),
            finalExtraLiterals,
            intermediateFields
        );

        // Coord still needs the result. Convert WORKER+HASH → COORDINATOR+SINGLETON; Volcano
        // materializes a final gather ER above. The transformTo target carries WORKER+HASH; the
        // ER is produced when a SINGLETON consumer demands it.
        RelTraitSet coordTraits = aggregate.getTraitSet().replace(distTraitDef.coordSingleton());
        convert(finalAggregate, coordTraits);
        call.transformTo(finalAggregate);
    }

    /** True when this aggregate already carries a WORKER+HASH trait — i.e. this rule has
     *  already fired on it. Prevents a memo loop. */
    private static boolean aggregateAlreadyResolvedAsHash(OpenSearchAggregate aggregate) {
        OpenSearchDistribution dist = distributionOf(aggregate);
        if (dist == null) return false;
        return dist.getType() == RelDistribution.Type.HASH_DISTRIBUTED && dist.getLocality() == OpenSearchDistribution.Locality.WORKER;
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            RelTrait trait = rel.getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution dist) return dist;
        }
        return null;
    }
}
