/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.opensearch.analytics.AnalyticsSettings;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.DistributedAggregateRewriter.FinalAggCallBuilder;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Post-CBO rewrite: pushes a PARTIAL aggregate BELOW a coordinator gather whose input is partitioned,
 * turning {@code Aggregate(SINGLE) / ExchangeReducer / <partitioned>} into
 * {@code Aggregate(FINAL) / ExchangeReducer / Aggregate(PARTIAL) / <partitioned>}.
 *
 * <p>Peer of {@link OpenSearchTopKRewriter} and {@link OpenSearchSortPushdownRewriter} — a targeted plan
 * rewrite applied to CBO's output, NOT a re-planner. It does not move, add or remove a single exchange:
 * the gather stays exactly where CBO put it, and only the aggregate is split across it.
 *
 * <p><b>Why this cannot live in CBO (and why it is the ONLY thing left of the old enforcement pass).</b>
 * A PARTIAL aggregate's requirement on its input is genuinely "any partitioning, I don't care" — it is
 * correct over shard-, worker- or hash-partitioned input. Cascades needs a CONCRETE required property to
 * drive {@code convert()}, and {@code OpenSearchDistribution} has no "partitioned, specification
 * irrelevant" value ({@code Type.ANY} means UNRESOLVED, not "anything acceptable" — the role Orca fills
 * with {@code CDistributionSpecAny}). So {@code OpenSearchAggregateSplitRule} can only build its PARTIAL at
 * {@code child.getTraitSet()}, which for an aggregate over a join is {@code SINGLETON(COORDINATOR)} — a
 * PARTIAL over already-gathered input, correctly priced at infinity by
 * {@code OpenSearchAggregate.computeSelfCost}. The alternative therefore never survives, even though
 * {@code deriveTraits} does derive {@code PARTIAL@HASH(WORKER)} into the memo.
 *
 * <p>Post-CBO the plan is CONCRETE, so this rewrite simply reads the gather's input distribution and builds
 * the PARTIAL at exactly that trait. That is the whole reason a post-CBO step is still needed here, and it
 * is a modelling gap in the distribution lattice — not a limitation of top-down search. Adding a
 * "partitioned-unspecified" required distribution would let {@code OpenSearchAggregateSplitRule} form this
 * during search and make this rewriter deletable.
 *
 * <p><b>Measured worth (analytics-bench, sf=10, one variable):</b> with this split 18/22; without it 14/22,
 * where q3/q5/q7/q11/q21 all fail with {@code ReduceSizeExceededException} because the coordinator gathers
 * RAW join output instead of aggregated groups.
 *
 * @opensearch.internal
 */
public final class OpenSearchPartialAggregatePushdownRewriter {

    private OpenSearchPartialAggregatePushdownRewriter() {}

    /**
     * Rewrites every eligible {@code Aggregate(SINGLE) / ExchangeReducer / <partitioned>} triple in
     * {@code plan}. Returns empty when nothing matched, so the caller keeps CBO's plan untouched.
     */
    public static Optional<RelNode> rewrite(RelNode plan, PlannerContext context) {
        // Scoped to MPP, per analytics.mpp.shuffle.aggregate.enabled's contract: with MPP off the ordinary
        // shard-PARTIAL / coordinator-FINAL split that OpenSearchAggregateSplitRule already forms during CBO
        // is the only path, and must not be disturbed.
        if (!AnalyticsSettings.MPP_ENABLED.get(context.getSettings())
            || !AnalyticsSettings.MPP_SHUFFLE_AGGREGATE_ENABLED.get(context.getSettings())) {
            return Optional.empty();
        }
        OpenSearchDistributionTraitDef traitDef = context.getDistributionTraitDef();
        if (traitDef == null) {
            return Optional.empty();
        }
        boolean[] changed = new boolean[1];
        RelNode rewritten = visit(plan, traitDef, changed);
        return changed[0] ? Optional.of(rewritten) : Optional.empty();
    }

    private static RelNode visit(RelNode node, OpenSearchDistributionTraitDef traitDef, boolean[] changed) {
        RelNode n = RelNodeUtils.unwrapHep(node);

        List<RelNode> newInputs = new ArrayList<>(n.getInputs().size());
        boolean inputChanged = false;
        for (RelNode input : n.getInputs()) {
            RelNode rewrittenInput = visit(input, traitDef, changed);
            inputChanged |= rewrittenInput != RelNodeUtils.unwrapHep(input);
            newInputs.add(rewrittenInput);
        }
        if (inputChanged) {
            n = n.copy(n.getTraitSet(), newInputs);
        }

        if (!(n instanceof OpenSearchAggregate agg) || agg.getMode() != AggregateMode.SINGLE) {
            return n;
        }
        // Look through row-transparent Projects between the aggregate and the gather. Requiring the reducer
        // to be the aggregate's DIRECT child is too strict: when an aggregate's argument is a computed
        // expression, a Project computing it sits in between, and declining there leaves the aggregate SINGLE
        // so the coordinator's peak scales with input ROWS rather than GROUP COUNT.
        List<RelNode> passThrough = new ArrayList<>();
        RelNode belowAgg = RelNodeUtils.unwrapHep(n.getInput(0));
        while (belowAgg instanceof OpenSearchProject ptp && !ptp.containsOver() && ptp.getInputs().size() == 1) {
            passThrough.add(belowAgg);
            belowAgg = RelNodeUtils.unwrapHep(belowAgg.getInput(0));
        }
        if (!(belowAgg instanceof OpenSearchExchangeReducer reducer)) {
            return n;
        }
        // A QTF reducer DECLARES a coord-side ___ugsi column plus matching FieldStorageInfo that
        // ShardFragmentStageExecution appends at runtime. Rebuilding it drops that declaration and
        // DatafusionReduceSink then rejects the batch on schema validation, so leave those alone.
        if (reducer.hasOverrideRowType()) {
            return n;
        }
        RelNode gatherInput = RelNodeUtils.unwrapHep(reducer.getInput(0));
        if (!isPartitioned(OpenSearchRelNode.distributionOf(gatherInput.getTraitSet()))) {
            return n;
        }
        // Shared correctness gates with the CBO-side split: STATE_EXPANDING / DISTINCT / cross-family
        // non-prefix group sets cannot be decomposed into PARTIAL+FINAL and must stay single-stage.
        if (OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit(agg)) {
            return n;
        }
        changed[0] = true;
        // Rebuild the looked-through Projects below the gather, innermost first. Each keeps its OWN traitSet,
        // which still carries the gathered distribution it was planned at — so the PARTIAL built on top of
        // them inherits that trait rather than the partitioned one it actually runs at. That is inert:
        // staging cuts on the reducer NODE and worker promotion reads the join's shuffle leaves, neither
        // consults this trait. Re-stamping the child's distribution here would be more faithful but changes
        // what the nodes compare equal to, so it is left alone until something reads it.
        RelNode partialInput = gatherInput;
        for (int i = passThrough.size() - 1; i >= 0; i--) {
            RelNode pt = passThrough.get(i);
            partialInput = pt.copy(pt.getTraitSet(), List.of(partialInput));
        }
        return split(agg, partialInput, traitDef);
    }

    /** Builds {@code FINAL(ExchangeReducer(PARTIAL(partitionedInput)))} for {@code agg}. */
    private static RelNode split(OpenSearchAggregate agg, RelNode partitionedInput, OpenSearchDistributionTraitDef traitDef) {
        List<AggregateCall> partialCalls = OpenSearchAggregateSplitRule.repairLossyReturnTypes(agg.getAggCallList(), partitionedInput);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            agg.getCluster(),
            partitionedInput.getTraitSet().replace(OpenSearchConvention.INSTANCE),
            partitionedInput,
            agg.getGroupSet(),
            agg.getGroupSets(),
            partialCalls,
            AggregateMode.PARTIAL,
            agg.getViableBackends(),
            agg.getCallAnnotations()
        );
        // buildReducer, not buildEnforcer: the PARTIAL sits on a partitioned input whose traitSet may still
        // carry CBO's coordSingleton, and the satisfies()-gated enforcer would then insert NOTHING — fusing
        // PARTIAL and FINAL into one stage so DAGBuilder never cuts the worker boundary.
        RelNode gathered = traitDef.buildReducer(partial);

        Map<Integer, List<RexLiteral>> finalExtraLiterals = OpenSearchAggregateSplitRule.captureLiteralArgsForFinal(
            agg.getAggCallList(),
            partitionedInput
        );
        List<IntermediateField> intermediateFields = FinalAggCallBuilder.classify(agg.getAggCallList());
        List<AggregateCall> finalCalls = FinalAggCallBuilder.buildFinalCalls(
            agg.getAggCallList(),
            intermediateFields,
            agg.getGroupSet().cardinality(),
            gathered,
            agg.getGroupSet().isEmpty()
        );
        OpenSearchAggregate finalAgg = new OpenSearchAggregate(
            agg.getCluster(),
            gathered.getTraitSet().replace(traitDef.coordSingleton()),
            gathered,
            agg.getGroupSet(),
            agg.getGroupSets(),
            finalCalls,
            AggregateMode.FINAL,
            agg.getViableBackends(),
            agg.getCallAnnotations(),
            finalExtraLiterals,
            intermediateFields
        );
        // Empty-group nullability gap (COUNT→SUM swap): wrap FINAL so its row type matches SINGLE's.
        return OpenSearchAggregateSplitRule.wrapWithCastIfNeeded(finalAgg, agg);
    }

    /** True when {@code dist} describes data spread across shards or worker partitions. */
    private static boolean isPartitioned(OpenSearchDistribution dist) {
        if (dist == null) {
            return false;
        }
        return dist.getType() == RelDistribution.Type.HASH_DISTRIBUTED || dist.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED;
    }
}
