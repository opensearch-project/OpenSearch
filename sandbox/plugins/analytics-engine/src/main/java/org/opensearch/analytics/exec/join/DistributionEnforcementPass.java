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
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
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
 * plan. This pass walks that plan and, for each {@link DistributionAware} operator, asks each
 * input's {@link DistributionAware#requiredInputDistribution required} distribution and inserts an
 * exchange ONLY where the input's {@link DistributionAware#deriveOutputDistribution actual} distribution
 * does not {@code satisfy()} it. A co-partitioned child needs no exchange — so the multi-tier cascade,
 * agg-over-join, and scalar-subquery shapes all emerge from the {@code satisfies()} algebra, for any join
 * depth / key pattern / tree shape, with no per-shape code.
 *
 * <p><b>Demand flows DOWN, actual flows UP (the Presto {@code AddExchanges} shape).</b> {@link #visit} is a
 * single recursion that is BOTH directions at once: it takes the parent's required distribution as the
 * {@code demand} argument (top-down) and returns the subtree's derived distribution (bottom-up) — exactly
 * Presto's {@code InternalPlanVisitor<PlanWithProperties, PreferredProperties>}. A {@code DistributionAware}
 * operator passes each child {@code requiredInputDistribution(i)} as that child's demand; a row-transparent
 * Project/Filter (no requirement of its own) passes the parent's demand straight through and RIDES whatever
 * the child returns. This is why a filtered scan feeding a join stays shard-local and an intermediate
 * Project between two join tiers rides the lower worker — both emerge from demand-flow, not a special-case
 * "is the child partitioned or shard-local?" heuristic. Only transparent ops and the root gather consume
 * the incoming demand; joins/aggregates derive their children's demands internally.
 *
 * <p><b>Peel-then-re-enforce.</b> CBO already inserted an {@link OpenSearchExchangeReducer} gather on each
 * distributed operator's inputs. The pass treats the CBO exchanges
 * ({@code ExchangeReducer}/{@code ShuffleExchange}/{@code BroadcastExchange}) as enforcement decisions it
 * re-makes: it recurses into the exchange's input to recover the input's CONTENT and derived distribution,
 * then the parent re-enforces its own requirement on that content. The root keeps its final SINGLETON
 * gather (the query result must land on the coordinator).
 *
 * <p><b>Aggregate split.</b> Besides placing exchanges, the pass performs the {@code Aggregate}
 * SINGLE→PARTIAL/FINAL split a distributed aggregate needs (step 3c, {@link #splitAggregate} via the
 * {@code FinalAggCallBuilder} machinery): when an aggregate's child is already distributed, PARTIAL rides
 * the child's worker and FINAL gathers to the coordinator (the q5/q10 shape). This split is the sole
 * distributed-aggregate path — the legacy {@code OpenSearchAggregateShuffleSplitRule} CBO alternative is
 * gone. It honors the {@code analytics.mpp.shuffle.aggregate.enabled} sub-toggle: when off, the aggregate
 * gathers and runs coordinator-centric while distributed JOINS are unaffected.
 *
 * @opensearch.internal
 */
public final class DistributionEnforcementPass {

    private static final Logger LOGGER = LogManager.getLogger(DistributionEnforcementPass.class);

    private final OpenSearchDistributionTraitDef traitDef;
    private final int partitionCount;
    private final long minRows;
    private final boolean shuffleAggregateEnabled;

    public DistributionEnforcementPass(
        OpenSearchDistributionTraitDef traitDef,
        int partitionCount,
        long minRows,
        boolean shuffleAggregateEnabled
    ) {
        this.traitDef = traitDef;
        this.partitionCount = partitionCount;
        this.minRows = minRows;
        this.shuffleAggregateEnabled = shuffleAggregateEnabled;
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
     * @param shuffleAggregateEnabled per-strategy sub-toggle ({@code analytics.mpp.shuffle.aggregate.enabled}):
     *                       when {@code false}, a decomposable SINGLE aggregate is NOT split PARTIAL/FINAL —
     *                       it gathers and runs coordinator-centric, leaving distributed JOINS unaffected.
     */
    public static RelNode enforce(
        RelNode plan,
        OpenSearchDistributionTraitDef traitDef,
        int partitionCount,
        long minRows,
        boolean shuffleAggregateEnabled
    ) {
        if (partitionCount <= 1) {
            return plan;
        }
        DistributionEnforcementPass pass = new DistributionEnforcementPass(traitDef, partitionCount, minRows, shuffleAggregateEnabled);
        // Root demand is SINGLETON — the query result must land on the coordinator. This seeds the
        // top-down demand-flow; a transparent op directly under the root passes it through so its child
        // sees the SINGLETON demand (matching the prior behavior where the root gather handled it).
        Visited root = pass.visit(plan, traitDef.coordSingleton());
        // The query result must be SINGLETON at the coordinator. If the root already produces SINGLETON
        // (the common case — its top op gathered), no enforcer is added; otherwise gather it.
        OpenSearchDistribution coordSingleton = traitDef.coordSingleton();
        if (root.actualDistribution != null && root.actualDistribution.satisfies(coordSingleton)) {
            return root.rel;
        }
        // Force the gather from the TRACKED distribution — not buildEnforcer, whose satisfies() check
        // trusts root.rel's stale CBO coordSingleton trait and would wrongly insert no ER (e.g. a top join
        // the pass distributed still carries coordSingleton on its traitSet).
        return traitDef.buildReducer(root.rel);
    }

    /**
     * Visit carrying {@code demand} (the parent's required distribution for this subtree, top-down) and
     * returning the rewritten subtree plus the distribution it actually outputs (bottom-up). Peels a CBO
     * exchange wrapper to recover the underlying content + distribution (the parent re-enforces). For a
     * {@link DistributionAware} operator, derives each input's own demand and enforces it. A row-transparent
     * Project/Filter passes {@code demand} straight to its child and rides whatever the child returns.
     */
    private Visited visit(RelNode node, OpenSearchDistribution demand) {
        RelNode n = RelNodeUtils.unwrapHep(node);

        // 1. CBO exchange wrapper: peel it, recover the input's content + actual distribution. The parent
        // that consumes this node will re-enforce its own requirement, re-inserting an exchange only if
        // the recovered actual distribution doesn't satisfy it. The demand flows through the peel unchanged.
        if (n instanceof OpenSearchExchangeReducer || n instanceof OpenSearchShuffleExchange || n instanceof OpenSearchBroadcastExchange) {
            return visit(((RelNode) n).getInput(0), demand);
        }

        // 2. Leaf scan: its trait carries the actual (SHARD) distribution; nothing to enforce below.
        if (n instanceof OpenSearchTableScan) {
            return new Visited(n, distributionOf(n));
        }

        // 3a-pre. Row-transparent passthrough (Project/Filter with no requirement of its own): pass the
        // INCOMING demand straight down to the child and RIDE whatever the child returns. This is the
        // demand-flow realization of the old "ride a partitioned-or-shard-local child" heuristic — but
        // derived from the algebra, not a special case: a transparent op preserves rows, so its child's
        // distribution IS its output, and its child's demand IS its own incoming demand. Covers BOTH the q3
        // filtered-scan-stays-shard-local case (demand flows to the scan, which stays SHARD) AND the
        // intermediate-Project-rides-the-lower-worker case (demand flows to the lower join's output, which
        // stays WORKER+HASH) with one rule. A window/pinned Project does NOT reach here — it imposes a
        // SINGLETON requirement (requiredInputDistribution != null), so it is handled by the generic step 4.
        boolean rowTransparent = (n instanceof OpenSearchProject || n instanceof OpenSearchFilter) && n.getInputs().size() == 1;
        if (rowTransparent && n instanceof DistributionAware ta && ta.requiredInputDistribution(0, partitionCount, traitDef) == null) {
            Visited child = visit(n.getInput(0), demand);
            RelNode rebuilt = copyWithInputs(n, List.of(child.rel));
            return new Visited(rebuilt, child.actualDistribution);
        }

        // 3a-agg. PRESERVE a CBO-emitted PARTIAL/FINAL split that is already correctly placed. When CBO's
        // cost model picked the two-phase aggregate, it emits FINAL(ExchangeReducer(PARTIAL(input))): the
        // PARTIAL runs per-shard on the partitioned input, the ER gathers the partials, the FINAL merges on
        // the coordinator. This is the right shape — the bottom-up walk must NOT relocate that exchange.
        // - A PARTIAL aggregate RIDES its child's distribution (runs shard-local on the partitioned input),
        // exactly like a row-transparent op: do not gather its input (that would defeat the split and
        // leave the FINAL reading raw rows where it expects partial state → the HLL Utf8View→Binary cast
        // and count-state index errors).
        // - A FINAL aggregate sitting DIRECTLY over a CBO ExchangeReducer (the coordinator-gather split)
        // re-gathers its peeled PARTIAL to SINGLETON, re-inserting the reducer BETWEEN them so DAGBuilder
        // cuts the PARTIAL into its own shard stage and the FINAL's input becomes a StageInputScan (the
        // shape DistributedAggregateRewriter expects to retype the HLL state column). The FINAL branch is
        // scoped to a child ExchangeReducer ON PURPOSE: a FINAL over a SHUFFLE exchange (a worker-tier
        // hash-aggregate, were CBO ever to emit one) is NOT a coordinator gather and must fall through to
        // the generic step-4 enforcement so its shuffle is preserved, not collapsed to a reducer here.
        if (n instanceof OpenSearchAggregate cboAgg) {
            if (cboAgg.getMode() == AggregateMode.PARTIAL) {
                Visited child = visit(n.getInput(0), null);
                RelNode rebuilt = copyWithInputs(n, List.of(child.rel));
                return new Visited(rebuilt, child.actualDistribution);
            }
            if (cboAgg.getMode() == AggregateMode.FINAL && RelNodeUtils.unwrapHep(n.getInput(0)) instanceof OpenSearchExchangeReducer) {
                Visited child = visit(n.getInput(0), null);
                RelNode gatheredPartial = gatherIfNeeded(child.rel, child.actualDistribution);
                RelNode rebuilt = copyWithInputs(n, List.of(gatheredPartial));
                return new Visited(rebuilt, traitDef.coordSingleton());
            }
        }

        // 3. Recurse into children. Each child's demand is derived below per operator; for the recursion
        // here we don't yet know it (joins/aggregates compute per-input demands), so we visit with a null
        // (ANY) demand to recover each child's content + actual distribution, then enforce per-input demands
        // in step 4. (A null demand means "no constraint" — the child still gathers/rides per ITS subtree.)
        List<RelNode> childContents = new ArrayList<>(n.getInputs().size());
        List<OpenSearchDistribution> childDists = new ArrayList<>(n.getInputs().size());
        for (RelNode input : n.getInputs()) {
            Visited v = visit(input, null);
            childContents.add(v.rel);
            childDists.add(v.actualDistribution);
        }

        if (!(n instanceof DistributionAware aware)) {
            // Not distribution-aware (e.g. OpenSearchSort): rebuild over the (peeled) children, but it imposes
            // no requirement and has no derivable output distribution — a parent that needs partitioning will
            // demand its own exchange. Re-gather each child to SINGLETON so a non-aware op still sees
            // coordinator inputs (conservative: matches the CBO-gathered baseline). Sink the reducer below any
            // leading row-transparent Project so a computed-column Project (e.g. `eval r = round(int)`, whose
            // Calcite-declared type may differ from the backend runtime type) stays with this op in the reduce
            // stage instead of being stranded below the cut, where its declared StageInputScan type would
            // clash with the gathered data (the round() Int32-vs-Float64 substrait mismatch).
            List<RelNode> regathered = new ArrayList<>(childContents.size());
            for (int i = 0; i < childContents.size(); i++) {
                regathered.add(gatherSinkingProjects(childContents.get(i), childDists.get(i)));
            }
            RelNode rebuilt = copyWithInputs(n, regathered);
            return new Visited(rebuilt, traitDef.coordSingleton());
        }

        // (The transparent-passthrough Project/Filter ride is handled BEFORE the child loop, in step 3a-pre,
        // via demand-flow: a transparent op passes the parent's demand to its child and rides whatever the
        // child returns — covering both the q3 filtered-scan-stays-shard-local and the intermediate-Project-
        // rides-the-lower-worker cases without the old "is the child partitioned-or-shard-local?" heuristic.
        // A NON-decomposable SINGLE aggregate never reaches that branch — it is an OpenSearchAggregate, not a
        // Project/Filter — so it correctly falls through to step 3b's gather → coordinator-centric.)

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
            // An aggregate that gathers here (e.g. a non-decomposable percentile, whose
            // requiredInputDistribution is null) keeps its literal-arg Projects in its own fragment: sink the
            // reducer below them so DAGBuilder doesn't strand the percentile literal in the child stage. Other
            // operators gather their inputs plainly.
            boolean isAggregate = n instanceof OpenSearchAggregate;
            List<RelNode> regathered = new ArrayList<>(childContents.size());
            for (int i = 0; i < childContents.size(); i++) {
                regathered.add(
                    isAggregate
                        ? gatherSinkingProjects(childContents.get(i), childDists.get(i))
                        : gatherIfNeeded(childContents.get(i), childDists.get(i))
                );
            }
            RelNode rebuilt = copyWithInputs(n, regathered);
            return new Visited(rebuilt, traitDef.coordSingleton());
        }

        // 3b-bcast. PRESERVE a broadcast decision CBO already made. When CBO's cost model picked the
        // broadcast alternative for this join, one input arrives wrapped in an OpenSearchBroadcastExchange
        // (the small build side); the step-3 child loop peeled it, so childContents[buildIdx] is the build's
        // inner content. Re-emit the SAME Join(BroadcastExchange(build), probe) shape rather than peeling it
        // and re-driving the join through the shuffle algebra (which would repartition BOTH sides and lose
        // the optimization). The shape is identical to OpenSearchBroadcastJoinSplitRule's, so
        // DAGBuilder.cutBroadcast tags the build BROADCAST_BUILD and emits an OpenSearchBroadcastScan in the
        // consumer fragment. There is NO willFeedJoin / shape gate: UnifiedDispatch resolves broadcast by
        // CAPTURING each build and INJECTING it as a BroadcastInjectionInstructionNode on whatever stage
        // consumes it (shard leaf, shuffle producer, or worker), then dispatches the broadcast-free DAG. So a
        // broadcast whose output feeds a shuffle join (q3/q8/q9) and a broadcast build that is itself a
        // producer subtree (q17) both run — the broadcast is just an instruction, never a second stage role.
        // Gated to a SHARD-local probe: a CBO broadcast plan always keeps the probe SHARD-local; if the probe
        // came back partitioned this isn't the broadcast-probe shape — fall through to normal shuffle handling.
        if (n instanceof OpenSearchJoin) {
            int buildIdx = -1;
            for (int i = 0; i < n.getInputs().size(); i++) {
                if (RelNodeUtils.unwrapHep(n.getInput(i)) instanceof OpenSearchBroadcastExchange) {
                    buildIdx = i;
                    break;
                }
            }
            if (buildIdx >= 0) {
                int probeIdx = buildIdx == 0 ? 1 : 0;
                if (isShardLocal(childDists.get(probeIdx))) {
                    OpenSearchBroadcastExchange cboBuild = (OpenSearchBroadcastExchange) RelNodeUtils.unwrapHep(n.getInput(buildIdx));
                    RelNode newBuild = traitDef.buildBroadcastExchange(childContents.get(buildIdx), cboBuild.getProbeNodeEstimate());
                    List<RelNode> joinInputs = new ArrayList<>(2);
                    joinInputs.add(buildIdx == 0 ? newBuild : childContents.get(probeIdx));
                    joinInputs.add(buildIdx == 0 ? childContents.get(probeIdx) : newBuild);
                    RelNode rebuilt = copyWithInputs(n, joinInputs);
                    LOGGER.debug("enforce: join {} preserves CBO broadcast (build input {})", n.getRelTypeName(), buildIdx);
                    // Worker join runs at the probe's SHARD distribution (alongside the probe scan); the
                    // parent gathers it to the coordinator (the legacy broadcast plan's shape).
                    return new Visited(rebuilt, childDists.get(probeIdx));
                }
            }
        }

        // 3d. Shippable-producer gate for a JOIN tier boundary. Distributing a join shuffles BOTH its
        // inputs into binary shuffle PRODUCER stages. A producer can only SHIP a hash shuffle if its
        // content runs as a SHARD_FRAGMENT (shard-local scan/filter/project chain) or is itself an
        // already-distributed WORKER tier — those execute a leaf fragment and ship partitions. A join
        // input whose tracked distribution is COORDINATOR+SINGLETON is a GATHERED sub-stage (a
        // decorrelated subquery's aggregate — TPC-H q4 `exists`→SEMI, q22 `not exists`→ANTI, q2/q15
        // scalar subqueries): DAGBuilder cuts it as a ReduceStageExecution, which emits to its parent
        // sink and CANNOT ship a shuffle → the worker awaits a producer that never fires
        // (`ShuffleScanHandler timed out for input-N`). The general dispatch does not yet drive a
        // "reduce-then-shuffle" producer, so if EITHER join input is non-shippable we do NOT distribute
        // this join — gather both inputs and run it coordinator-centric (correct, the legacy fallback).
        // The "only distribute what dispatch can run" discipline. (sf=10 bucket A.)
        if (n instanceof OpenSearchJoin) {
            boolean allInputsShippable = true;
            for (OpenSearchDistribution cd : childDists) {
                if (!isPartitioned(cd) && !isShardLocal(cd)) {
                    allInputsShippable = false;
                    break;
                }
            }
            if (!allInputsShippable) {
                List<RelNode> regathered = new ArrayList<>(childContents.size());
                for (int i = 0; i < childContents.size(); i++) {
                    regathered.add(gatherIfNeeded(childContents.get(i), childDists.get(i)));
                }
                RelNode rebuilt = copyWithInputs(n, regathered);
                LOGGER.debug("enforce: join {} has a non-shippable (gathered sub-stage) input → coord-centric", n.getRelTypeName());
                return new Visited(rebuilt, traitDef.coordSingleton());
            }
        }

        // 3c. SINGLE decomposable aggregate — Variant A (NO group-key shuffle), mirroring the legacy
        // DistributedAggOverJoinRewriter. The PARTIAL result is correct under ANY input partitioning (a
        // decomposable PARTIAL doesn't care whether rows are partitioned by join-key or group-key — only the
        // FINAL needs all partials, and the FINAL gathers), so we do NOT honor the aggregate's
        // HASH(groupKeys) requirement here. Doing so (the bug codex found) inserts a group-key re-shuffle
        // whose consumer is the PARTIAL — a NON-join shuffle edge that GeneralShuffleDAGRewriter (joins-only)
        // never promotes/wires → hang. Instead:
        // - child already distributed (a join/cascade below) → split: PARTIAL rides on the child's worker
        // (no exchange), FINAL gathers. This is the q5/q10 shape and works for group-key == OR != the
        // join key, and for the empty-group case.
        // - child NOT distributed (bare scan) → distributing the aggregate itself would need a group-key
        // shuffle + a single-input agg worker tier, which the general dispatch does NOT yet drive. Rather
        // than emit an un-wireable agg-shuffle, GATHER and run the SINGLE aggregate on the coordinator
        // (coord-centric, correct). Generalizing dispatch to agg-consumer shuffle edges is the next step.
        if (n instanceof OpenSearchAggregate agg
            && agg.getMode() == AggregateMode.SINGLE
            && aware.requiredInputDistribution(0, partitionCount, traitDef) != null) {
            RelNode childContent = childContents.get(0);
            OpenSearchDistribution childDist = childDists.get(0);
            // Per-strategy sub-toggle (analytics.mpp.shuffle.aggregate.enabled): when off, do NOT split the
            // aggregate PARTIAL/FINAL — gather the (possibly distributed) child and run the SINGLE aggregate
            // coordinator-centric. This is the documented "disable distributed aggregation, keep MPP joins"
            // semantics: a join BELOW still distributes (its worker tier is untouched); only the aggregate's
            // own parallel split is suppressed. Default true → byte-identical to the split path (q5/q10).
            if (shuffleAggregateEnabled && isPartitioned(childDist)) {
                return splitAggregate(agg, childContent);
            }
            RelNode gathered = gatherSinkingProjects(childContent, childDist);
            return new Visited(copyWithInputs(n, List.of(gathered)), traitDef.coordSingleton());
        }

        // 4. DistributionAware: enforce each input's required distribution.
        //
        // Binary-tier lowering: a JOIN is a worker-tier boundary. The hash-shuffle transport delivers
        // exactly TWO named shuffle inputs per worker (left/right buffer slices — ShuffleScanInstructionNode
        // side ∈ {left,right}; the buffer has two slices), so every distributed join input must arrive as
        // its OWN shuffle producer stream. We therefore do NOT take the co-partition-reuse shortcut on a
        // join input: even when a lower join's output already satisfies the required hash, we insert a
        // same-key inter-tier shuffle so DAGBuilder cuts the lower join into its own binary producer stage
        // (rather than collapsing N joins into one fragment with N shuffle leaves, which the binary
        // transport cannot run). This same-key inter-tier shuffle is exactly what the legacy cascade
        // already does (each intermediate worker produces a shuffle to its parent worker). UNARY ops
        // (Project / Filter / PARTIAL aggregate) are NOT tier boundaries — they ride on the same worker as
        // their child, so they keep the reuse shortcut (e.g. a PARTIAL agg sits on its join's worker with
        // no extra shuffle). Eliminating the inter-tier shuffle for genuinely co-partitioned tiers is a
        // documented future optimization (needs N-ary shuffle transport); see MPP-GENERAL-SCHEDULING-DESIGN.md.
        boolean isTierBoundary = n instanceof OpenSearchJoin;
        List<RelNode> newInputs = new ArrayList<>(childContents.size());
        List<OpenSearchDistribution> enforcedChildDists = new ArrayList<>(childContents.size());
        for (int i = 0; i < childContents.size(); i++) {
            RelNode childContent = childContents.get(i);
            OpenSearchDistribution childDist = childDists.get(i);
            OpenSearchDistribution required = aware.requiredInputDistribution(i, partitionCount, traitDef);
            if (required == null) {
                // No requirement on this input — gather it to a coherent landing. (A transparent op over a
                // PARTITIONED child was already handled by the step-3a fast path, which rides on the child's
                // worker; reaching here means the child is not partitioned, e.g. a theta join's gathered
                // inputs, so a plain gather is correct.)
                RelNode landed = gatherIfNeeded(childContent, childDist);
                newInputs.add(landed);
                enforcedChildDists.add(landed == childContent ? childDist : traitDef.coordSingleton());
                continue;
            }
            if (!isTierBoundary && childDist != null && childDist.satisfies(required)) {
                // Unary op already co-partitioned — no exchange (rides on its child's worker).
                newInputs.add(childContent);
                enforcedChildDists.add(childDist);
            } else if (required.getType() == org.apache.calcite.rel.RelDistribution.Type.SINGLETON) {
                // A SINGLETON requirement (a window / pinned OpenSearchProject / global Sort that needs
                // fully-gathered input) over a distributed child must be GATHERED. Force buildReducer from the
                // TRACKED distribution — NOT buildEnforcer/forceShuffle, whose satisfies() check trusts the
                // child's stale CBO coordSingleton trait and would skip the ER, leaving the consumer running
                // per-partition (silently wrong global result — the codex BLOCKER). gatherIfNeeded is a no-op
                // when the child is genuinely already SINGLETON. The reducer is sunk BELOW any leading
                // row-transparent Project so a computed-column Project (e.g. `eval r = round(x)`, whose
                // Calcite-declared type may differ from the backend's runtime type) stays in the consumer's
                // fragment instead of being stranded below the cut, where its declared StageInputScan type
                // would clash with the gathered data (the round() Int32-vs-Float64 substrait mismatch).
                RelNode landed = gatherSinkingProjects(childContent, childDist);
                newInputs.add(landed);
                enforcedChildDists.add(landed == childContent ? childDist : traitDef.coordSingleton());
                LOGGER.debug("enforce: {} input {} → SINGLETON gather", n.getRelTypeName(), i);
            } else {
                // Join input (always) or a non-co-partitioned unary input with a HASH requirement:
                // materialize the shuffle. For a join input whose child is a lower distributed join the
                // child's traitSet still carries the CBO coordSingleton trait (the pass tracks the derived
                // HASH only in Visited), so buildEnforcer sees from=coordSingleton ⊭ HASH and inserts the
                // inter-tier shuffle.
                RelNode enforced = forceShuffle(isTierBoundary, childContent, required);
                newInputs.add(enforced);
                enforcedChildDists.add(required);
                LOGGER.debug("enforce: {} input {} → {}{}", n.getRelTypeName(), i, required, isTierBoundary ? " (tier boundary)" : "");
            }
        }
        // (The SINGLE-aggregate PARTIAL/FINAL split is handled earlier, in step 3c, NOT here — it must
        // decide on the child's tracked partitioning before the generic per-input enforcement, and it never
        // honors the aggregate's HASH(groupKeys) requirement. See step 3c.)
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
        // Force the ER between PARTIAL and FINAL. buildReducer (not buildEnforcer): the PARTIAL was built
        // over partialInput, whose traitSet may carry a stale coordSingleton (a join the pass distributed),
        // so the satisfies()-gated buildEnforcer would skip the gather and collapse PARTIAL+FINAL into one
        // stage — DAGBuilder then never cuts the worker boundary.
        RelNode gathered = traitDef.buildReducer(partial);

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

    /**
     * Enforces {@code required} (a WORKER+HASH demand) on {@code childContent}. At a JOIN tier boundary the
     * shuffle is built UNCONDITIONALLY ({@code buildShuffleExchange}) — even when the child's trait already
     * satisfies the required hash (a lower join CBO itself shuffled, so its traitSet carries HASH) — so
     * DAGBuilder cuts the child into its own binary producer stage. The binary shuffle transport delivers
     * exactly two named inputs per worker, so each distributed join input must be its own producer stream;
     * reusing an already-HASH child in place would collapse N joins into one fragment with N shuffle leaves,
     * which the transport cannot run. For a non-tier (unary) input we go through the satisfies-gated
     * {@code buildEnforcer} so a genuinely co-partitioned unary child rides on its child's worker with no
     * extra shuffle.
     */
    private RelNode forceShuffle(boolean isTierBoundary, RelNode childContent, OpenSearchDistribution required) {
        return isTierBoundary ? traitDef.buildShuffleExchange(childContent, required) : traitDef.buildEnforcer(childContent, required);
    }

    /**
     * Gathers {@code rel} to COORDINATOR+SINGLETON unless its TRACKED distribution {@code actual} already
     * satisfies SINGLETON. Decides off {@code actual} (the pass's tracked distribution), then forces the
     * reducer via {@code buildReducer} — NOT {@code buildEnforcer}, whose satisfies() check trusts
     * {@code rel}'s stale CBO trait and would skip the ER for a rel the pass actually left partitioned.
     */
    private RelNode gatherIfNeeded(RelNode rel, OpenSearchDistribution actual) {
        if (actual != null && actual.satisfies(traitDef.coordSingleton())) {
            return rel;
        }
        return traitDef.buildReducer(rel);
    }

    /**
     * Gathers {@code rel} to SINGLETON like {@link #gatherIfNeeded}, but sinks the reducer BELOW any leading
     * row-transparent {@link OpenSearchProject} chain so those Projects stay in the SAME fragment as their
     * consumer (the aggregate / sort / window that triggered the gather), rather than being stranded in the
     * child stage when {@code DAGBuilder} cuts at the reducer.
     *
     * <p>Two failures this prevents, both from a Project landing below the cut while its consumer lands above:
     * <ul>
     *   <li><b>Aggregate literal args.</b> The PPL frontend materializes an aggregate's literal config args
     *       (e.g. {@code percentile(x, 50)}'s {@code 50}, its type-flag) as constant columns in a Project
     *       directly below the aggregate. Stranding that Project makes the reduce fragment see the percentile
     *       arg as a plain {@code StageInputScan} column, not a literal → "APPROX_PERCENTILE_CONT must be a
     *       literal".</li>
     *   <li><b>Computed-column type drift.</b> A Project column whose Calcite-declared type differs from the
     *       backend's runtime type (e.g. {@code eval r = round(int)} — Calcite infers INTEGER, DataFusion's
     *       {@code round} returns FLOAT64) is fine within one fragment, but once it crosses a cut the
     *       {@code StageInputScan} declares the stale Calcite type while the gathered data carries the
     *       backend type → Substrait "Field has a different type" mismatch.</li>
     * </ul>
     * Keeping the Project with its consumer reproduces the single-fragment shape the coordinator-centric /
     * mpp-off path produces, where neither mismatch can arise.
     *
     * <p>Only leading {@code Project}s are descended (row-wise, so running them above the gather on the
     * coordinator is semantically identical and merely ships a few extra columns). The descent stops at the
     * first non-Project — a Filter, scan, join, or exchange — placing the reducer there; a Filter thus stays
     * BELOW the gather (runs shard-side, ships fewer rows).
     */
    private RelNode gatherSinkingProjects(RelNode rel, OpenSearchDistribution actual) {
        if (actual != null && actual.satisfies(traitDef.coordSingleton())) {
            return rel;
        }
        return sinkReducerBelowProjects(rel);
    }

    /**
     * Places the gather reducer below any leading row-transparent {@link OpenSearchProject} chain (see
     * {@link #gatherSinkingProjects}). Walks down single-input Projects (no {@code RexOver}), rebuilding them
     * above the reducer, and inserts the reducer at the first non-Project node.
     */
    private RelNode sinkReducerBelowProjects(RelNode rel) {
        if (rel instanceof OpenSearchProject project && !project.containsOver() && project.getInputs().size() == 1) {
            RelNode child = RelNodeUtils.unwrapHep(project.getInput(0));
            return copyWithInputs(project, List.of(sinkReducerBelowProjects(child)));
        }
        return traitDef.buildReducer(rel);
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

    /**
     * True iff {@code dist} is SHARD-resident (a table scan or shard-local filter/project output, before any
     * exchange). A transparent op over a shard-local child rides on the shards (stays shard-local) rather
     * than gathering — so a parent join can hash-ship it from the shards via a SHARD-FRAGMENT producer. (The
     * q3 fix: a filtered scan feeding a join must remain a shard producer, not become a coordinator reduce.)
     */
    private static boolean isShardLocal(OpenSearchDistribution dist) {
        return dist != null && dist.getLocality() == OpenSearchDistribution.Locality.SHARD;
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
