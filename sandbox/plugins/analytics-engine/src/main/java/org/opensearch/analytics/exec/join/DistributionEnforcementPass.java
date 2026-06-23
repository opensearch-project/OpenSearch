/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.DistributedAggregateRewriter.FinalAggCallBuilder;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.DistributionAware;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastExchange;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * General post-CBO distribution-enforcement pass (Option B — see {@code MPP-GENERAL-SCHEDULING-DESIGN.md}).
 * Replaces the enumerated shape-matchers ({@code CascadeShufflePlanRewriter},
 * {@code DistributedAggOverJoinRewriter}, …) with ONE pass that places exchanges generically, the way
 * Spark's {@code EnsureRequirements} and Presto's {@code AddExchanges} do.
 *
 * <p><b>Why this generalizes.</b> Bottom-up Volcano gathers every join to {@code COORDINATOR+SINGLETON}
 * (its cost gate knows only 3 fixed localities), so the CBO output is the degenerate "gather everything"
 * plan. This pass walks that plan bottom-up and, for each {@link DistributionAware} operator, asks each
 * input's {@link DistributionAware#requiredInputDistribution required} distribution and inserts an
 * exchange ONLY where the input's {@link DistributionAware#deriveOutputDistribution actual} distribution
 * does not {@code satisfy()} it. A co-partitioned child needs no exchange — so the multi-tier cascade,
 * agg-over-join, and scalar-subquery shapes all emerge from the {@code satisfies()} algebra, for any join
 * depth / key pattern / tree shape, with no per-shape code.
 *
 * <p><b>Peel-then-re-enforce.</b> CBO already inserted an {@link OpenSearchExchangeReducer} gather on each
 * distributed operator's inputs. The pass treats the CBO exchanges
 * ({@code ExchangeReducer}/{@code ShuffleExchange}/{@code BroadcastExchange}) as enforcement decisions it
 * re-makes: it recurses into the exchange's input to recover the input's CONTENT and derived distribution,
 * then the parent re-enforces its own requirement on that content. The root keeps its final SINGLETON
 * gather (the query result must land on the coordinator).
 *
 * <p><b>Scope (v1).</b> This pass places exchanges. The {@code Aggregate} SINGLE→PARTIAL/FINAL split that a
 * distributed aggregate needs is handled separately (the existing {@code FinalAggCallBuilder} machinery);
 * v1 wires the join cascade + shuffle/broadcast placement and leaves aggregate splitting to a follow-on
 * within B1. Gated behind {@code analytics.mpp.cbo_native_cascade}; the legacy rewriters remain the
 * default until sf=10 parity is proven (B3).
 *
 * @opensearch.internal
 */
public final class DistributionEnforcementPass {

    private static final Logger LOGGER = LogManager.getLogger(DistributionEnforcementPass.class);

    private final OpenSearchDistributionTraitDef traitDef;
    private final int partitionCount;
    private final long minRows;

    public DistributionEnforcementPass(OpenSearchDistributionTraitDef traitDef, int partitionCount, long minRows) {
        this.traitDef = traitDef;
        this.partitionCount = partitionCount;
        this.minRows = minRows;
    }

    /** Result of visiting a node: the (possibly rewritten) rel and the distribution it actually outputs. */
    private record Visited(RelNode rel, OpenSearchDistribution actualDistribution) {
    }

    /**
     * Returns {@code plan} with exchanges re-placed by the distribution algebra. The root's result is
     * gathered to {@code COORDINATOR+SINGLETON} (the query result lands on the coordinator). When the pass
     * makes no change (no distributable operator), the original plan is returned.
     *
     * @param partitionCount resolved shuffle partition count; {@code <= 1} disables the pass (returns plan)
     * @param minRows        size floor: an operator is distributed only when its larger scan subtree
     *                       exceeds this many rows (keeps small joins/aggregates coordinator-centric, the
     *                       same gate the legacy rewriters apply). {@code <= 0} disables the floor.
     */
    public static RelNode enforce(RelNode plan, OpenSearchDistributionTraitDef traitDef, int partitionCount, long minRows) {
        if (partitionCount <= 1) {
            return plan;
        }
        DistributionEnforcementPass pass = new DistributionEnforcementPass(traitDef, partitionCount, minRows);
        Visited root = pass.visit(plan);
        // The query result must be SINGLETON at the coordinator. If the root already produces SINGLETON
        // (the common case — its top op gathered), no enforcer is added; otherwise gather it.
        OpenSearchDistribution coordSingleton = traitDef.coordSingleton();
        if (root.actualDistribution != null && root.actualDistribution.satisfies(coordSingleton)) {
            return root.rel;
        }
        RelNode gathered = traitDef.buildEnforcer(root.rel, coordSingleton);
        return gathered == null ? root.rel : gathered;
    }

    /**
     * Bottom-up visit. Returns the rewritten subtree rooted at {@code node} plus the distribution it
     * outputs. Peels a CBO exchange wrapper to recover the underlying content + distribution (the parent
     * re-enforces). For a {@link DistributionAware} operator, enforces each input's required distribution.
     */
    private Visited visit(RelNode node) {
        RelNode n = RelNodeUtils.unwrapHep(node);

        // 1. CBO exchange wrapper: peel it, recover the input's content + actual distribution. The parent
        // that consumes this node will re-enforce its own requirement, re-inserting an exchange only if
        // the recovered actual distribution doesn't satisfy it.
        if (n instanceof OpenSearchExchangeReducer || n instanceof OpenSearchShuffleExchange || n instanceof OpenSearchBroadcastExchange) {
            return visit(((RelNode) n).getInput(0));
        }

        // 2. Leaf scan: its trait carries the actual (SHARD) distribution; nothing to enforce below.
        if (n instanceof OpenSearchTableScan) {
            return new Visited(n, distributionOf(n));
        }

        // 3. Recurse into children first (bottom-up).
        List<RelNode> childContents = new ArrayList<>(n.getInputs().size());
        List<OpenSearchDistribution> childDists = new ArrayList<>(n.getInputs().size());
        for (RelNode input : n.getInputs()) {
            Visited v = visit(input);
            childContents.add(v.rel);
            childDists.add(v.actualDistribution);
        }

        if (!(n instanceof DistributionAware aware)) {
            // Not distribution-aware: rebuild over the (peeled) children, but it imposes no requirement and
            // has no derivable output distribution — a parent that needs partitioning will demand its own
            // exchange. Re-gather each child to SINGLETON so a non-aware op still sees coordinator inputs
            // (conservative: matches the CBO-gathered baseline for operators the algebra doesn't model).
            List<RelNode> regathered = new ArrayList<>(childContents.size());
            for (int i = 0; i < childContents.size(); i++) {
                regathered.add(gatherIfNeeded(childContents.get(i), childDists.get(i)));
            }
            RelNode rebuilt = copyWithInputs(n, regathered);
            return new Visited(rebuilt, traitDef.coordSingleton());
        }

        // 3b. Size floor: distribute this operator only when it is worth it — either a child is ALREADY
        // distributed (a deeper op cleared the floor; keep the cascade going), or this operator's own scan
        // subtree exceeds minRows. Otherwise treat it as non-distributable (gather inputs, stay
        // coordinator-centric) so small joins/aggregates keep CBO's cheap coord-centric choice — the same
        // gate the legacy rewriters apply. A distributable op with no requirement on any input (pure-theta
        // join) also falls through to the non-aware handling.
        boolean anyChildDistributed = childDists.stream().anyMatch(DistributionEnforcementPass::isPartitioned);
        boolean aboveFloor = minRows <= 0 || subtreeMaxScanRows(n) >= minRows;
        boolean imposesRequirement = false;
        for (int i = 0; i < n.getInputs().size(); i++) {
            if (aware.requiredInputDistribution(i, partitionCount, traitDef) != null) {
                imposesRequirement = true;
                break;
            }
        }
        if (!imposesRequirement || (!anyChildDistributed && !aboveFloor)) {
            List<RelNode> regathered = new ArrayList<>(childContents.size());
            for (int i = 0; i < childContents.size(); i++) {
                regathered.add(gatherIfNeeded(childContents.get(i), childDists.get(i)));
            }
            RelNode rebuilt = copyWithInputs(n, regathered);
            return new Visited(rebuilt, traitDef.coordSingleton());
        }

        // 4. DistributionAware: enforce each input's required distribution.
        List<RelNode> newInputs = new ArrayList<>(childContents.size());
        List<OpenSearchDistribution> enforcedChildDists = new ArrayList<>(childContents.size());
        for (int i = 0; i < childContents.size(); i++) {
            RelNode childContent = childContents.get(i);
            OpenSearchDistribution childDist = childDists.get(i);
            OpenSearchDistribution required = aware.requiredInputDistribution(i, partitionCount, traitDef);
            if (required == null) {
                // No requirement on this input — leave it, but make sure it lands somewhere coherent: if its
                // distribution is unknown/partitioned and the op didn't ask for partitioning, gather it.
                RelNode landed = gatherIfNeeded(childContent, childDist);
                newInputs.add(landed);
                enforcedChildDists.add(landed == childContent ? childDist : traitDef.coordSingleton());
                continue;
            }
            if (childDist != null && childDist.satisfies(required)) {
                // Already co-partitioned — no exchange (this is where the cascade reuses upstream hashing).
                newInputs.add(childContent);
                enforcedChildDists.add(childDist);
            } else {
                RelNode enforced = traitDef.buildEnforcer(childContent, required);
                newInputs.add(enforced);
                enforcedChildDists.add(required);
                LOGGER.debug("enforce: {} input {} → {}", n.getRelTypeName(), i, required);
            }
        }
        // 5. Aggregate special case: when a SINGLE aggregate's input was distributed (hash-shuffled, or
        // its content is partitioned), the aggregate must be SPLIT into PARTIAL (over the partitioned
        // input) + FINAL (over a coordinator gather). Volcano's OpenSearchAggregateSplitRule can't re-fire
        // post-CBO, so the pass performs the split itself, reusing the exact same helpers
        // (repairLossyReturnTypes / FinalAggCallBuilder / wrapWithCastIfNeeded) the split rule uses.
        if (n instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.SINGLE) {
            OpenSearchDistribution req = aware.requiredInputDistribution(0, partitionCount, traitDef);
            if (req != null) {
                RelNode enforcedInput = newInputs.get(0);
                return splitAggregate(agg, enforcedInput);
            }
        }

        RelNode rebuilt = copyWithInputs(n, newInputs);
        OpenSearchDistribution out = aware.deriveOutputDistribution(enforcedChildDists, traitDef);
        return new Visited(rebuilt, out);
    }

    /**
     * Splits a {@code SINGLE} aggregate over a (now-distributed) input into
     * {@code FINAL( ER(SINGLETON)( PARTIAL(input) ) )}, mirroring {@code OpenSearchAggregateSplitRule.onMatch}
     * — the same {@link OpenSearchAggregateSplitRule#repairLossyReturnTypes} on PARTIAL, the same
     * {@code FinalAggCallBuilder} rebind on FINAL (argList → groupCount+i, COUNT→SUM), and the same
     * {@link OpenSearchAggregateSplitRule#wrapWithCastIfNeeded} empty-group nullability fix. The FINAL
     * gathers the partials to the coordinator, so the result is SINGLETON.
     */
    private Visited splitAggregate(OpenSearchAggregate agg, RelNode partialInput) {
        List<AggregateCall> partialCalls = OpenSearchAggregateSplitRule.repairLossyReturnTypes(agg.getAggCallList(), partialInput);
        OpenSearchAggregate partial = new OpenSearchAggregate(
            agg.getCluster(),
            partialInput.getTraitSet().replace(OpenSearchConvention.INSTANCE),
            partialInput,
            agg.getGroupSet(),
            agg.getGroupSets(),
            partialCalls,
            AggregateMode.PARTIAL,
            agg.getViableBackends(),
            agg.getCallAnnotations()
        );
        RelNode gathered = traitDef.buildEnforcer(partial, traitDef.coordSingleton());

        Map<Integer, List<RexLiteral>> finalExtraLiterals = OpenSearchAggregateSplitRule.captureLiteralArgsForFinal(
            agg.getAggCallList(),
            partialInput
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
        RelNode result = OpenSearchAggregateSplitRule.wrapWithCastIfNeeded(finalAgg, agg);
        LOGGER.debug("enforce: split SINGLE aggregate {} → PARTIAL/FINAL", agg.getId());
        return new Visited(result, traitDef.coordSingleton());
    }

    /** Gathers {@code rel} to COORDINATOR+SINGLETON unless it already produces SINGLETON. */
    private RelNode gatherIfNeeded(RelNode rel, OpenSearchDistribution actual) {
        OpenSearchDistribution coordSingleton = traitDef.coordSingleton();
        if (actual != null && actual.satisfies(coordSingleton)) {
            return rel;
        }
        RelNode gathered = traitDef.buildEnforcer(rel, coordSingleton);
        return gathered == null ? rel : gathered;
    }

    private static RelNode copyWithInputs(RelNode node, List<RelNode> newInputs) {
        boolean changed = newInputs.size() != node.getInputs().size();
        if (!changed) {
            for (int i = 0; i < newInputs.size(); i++) {
                if (newInputs.get(i) != RelNodeUtils.unwrapHep(node.getInput(i))) {
                    changed = true;
                    break;
                }
            }
        }
        return changed ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static OpenSearchDistribution distributionOf(RelNode rel) {
        for (int i = 0; i < rel.getTraitSet().size(); i++) {
            if (rel.getTraitSet().getTrait(i) instanceof OpenSearchDistribution dist) {
                return dist;
            }
        }
        return null;
    }

    /**
     * True iff {@code dist} is a {@code WORKER+HASH} partitioning — i.e. a child OPERATOR already ran
     * distributed (a shuffle/cascade level below). NOT a bare {@code SHARD+RANDOM} scan: that's just
     * "data lives on shards", which is true of every multi-shard input and would defeat the size floor.
     * The cascade-continuation signal is specifically a worker-side hash distribution flowing up.
     */
    private static boolean isPartitioned(OpenSearchDistribution dist) {
        if (dist == null) {
            return false;
        }
        return dist.getType() == org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED
            && dist.getLocality() == OpenSearchDistribution.Locality.WORKER;
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
}
