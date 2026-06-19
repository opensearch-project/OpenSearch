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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.dag.DistributedAggregateRewriter.FinalAggCallBuilder;
import org.opensearch.analytics.planner.dag.FragmentConversionDriver;
import org.opensearch.analytics.planner.dag.PlanAlternativeSelector;
import org.opensearch.analytics.planner.dag.PlanForker;
import org.opensearch.analytics.planner.dag.QueryDAG;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchShuffleExchange;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Variant A distributed aggregation over a cascade hash-shuffle join — the TPC-H q5/q10 enabler.
 *
 * <p><b>Problem.</b> For {@code source=… | join … | join … | … | stats sum(x) by g | sort …} where
 * the {@code stats} sits above a multi-way INNER join, the {@link CascadeShuffleDAGRewriter} lifts
 * only the bottom join (the one over two {@link OpenSearchShuffleExchange}s) into a worker tier; the
 * dimension joins, the pre-aggregate {@code Project}, and the {@code Aggregate(SINGLE)} all stay in
 * the coordinator reduce stage. The reduce sink buffers the FULL post-join row stream (~1.7M rows
 * for q5 at sf=10) before the coordinator aggregate runs → {@code ReduceSizeExceededException}. The
 * aggregate is the last operator never pushed off the coordinator.
 *
 * <p><b>Fix (Variant A — partial-agg-before-gather, NO new group-key shuffle).</b> This rewriter
 * lifts the WHOLE pre-aggregate pipeline plus a {@code PARTIAL} aggregate onto the cascade TOP
 * worker, leaving the {@code FINAL} aggregate (and any {@code Sort}) on the coordinator:
 *
 * <pre>
 *   coordinator reduce:  Sort? → Aggregate(FINAL) → StageInputScan(topWorker)
 *   top worker (per partition):
 *       Aggregate(PARTIAL by groupKeys)
 *         └ Project / Filter / dimension-Joins(broadcast form)
 *             └ Join_bottom( Shuffle(SIS→left), Shuffle(SIS→right) )
 * </pre>
 *
 * <p>Each partition worker pre-aggregates its slice of the joined rows, so the gather to the
 * coordinator is bounded by {@code N × #distinct-groups} partial rows (≈ tens of rows for q5's
 * {@code by n_name}) instead of the full join output. The {@code FINAL} re-merges across partitions
 * on the coordinator — correct for any decomposable aggregate because partitioning by join-key vs.
 * group-key is irrelevant to the PARTIAL result, only to whether FINAL can complete locally (it
 * gathers, so it always can). See {@code DISTRIBUTED-AGG-FOLLOWUP.md §3 Variant A / §4}.
 *
 * <p><b>The dimension-joins-between trap (§4).</b> q5's group key {@code n_name} is not available
 * until after {@code Join(nation)}; the {@code where r_name='ASIA'} filter likewise sits above the
 * dimension joins. A PARTIAL placed below the dimension joins would reference unavailable columns
 * and aggregate pre-filter rows — silently wrong. So the dimension joins MUST run on the worker too.
 * The dimension tables (supplier 100K / nation 25 / region 5) are tiny, so they ride to each worker
 * as BROADCAST inputs: this rewriter rewrites each dimension {@link OpenSearchStageInputScan} leaf in
 * the lifted pre-agg pipeline into an {@link OpenSearchBroadcastScan} (named {@code broadcast-<id>}),
 * and tags those dimension child stages {@link Stage.StageRole#BROADCAST_BUILD}. The dispatcher
 * ({@code DistributedAggOverJoinDispatch}) builds each dimension stage in a first pass, captures its
 * Arrow-IPC output, and injects a {@link org.opensearch.analytics.spi.BroadcastInjectionInstructionNode}
 * into the worker's plan alternatives so the worker session resolves the {@code broadcast-<id>}
 * {@code NamedScan} to the captured memtable — exactly the mechanism the 2-way broadcast probe uses,
 * but inside a worker session (the worker request carries plan-alternative instructions and the
 * data-node worker handler runs the instruction chain before executing the fragment).
 *
 * <p><b>Correctness gates (HARD — silently-wrong-results traps, do NOT loosen):</b>
 * <ul>
 *   <li>Reuses {@link OpenSearchAggregateSplitRule#shouldSkipPartialFinalSplit} — STATE_EXPANDING
 *       (TAKE/percentile/…), {@code COUNT(DISTINCT)}, and cross-family-non-prefix group sets are NOT
 *       distributed; the shape falls back to coordinator-centric.</li>
 *   <li>The liftable join AND every dimension join on the coordinator path must be INNER — the
 *       cascade worker implements only INNER hash-join across partitions; an outer/semi/anti join in
 *       the pre-agg pipeline keeps the aggregate on the coordinator.</li>
 *   <li>Only partition-preserving / coordinator ops may sit between the liftable join and the
 *       aggregate: {@link OpenSearchJoin} / {@link OpenSearchProject} / {@link OpenSearchFilter}. A
 *       window {@code Project}, or an {@code Aggregate}/{@code Sort}/{@code Limit}/{@code Union}
 *       between the join and the {@code stats}, is rejected (it would run per join-key-partition).</li>
 *   <li>Every dimension join input on the path must resolve to either a reducer-fed child stage
 *       (an {@link OpenSearchStageInputScan} that becomes a broadcast build) or another safe op — if
 *       a dimension input cannot be broadcast onto the worker, the rewrite bails (no PARTIAL is
 *       pushed that references columns the worker cannot produce).</li>
 * </ul>
 *
 * <p>This rewriter performs only the STRUCTURAL rewrite + the {@code forkAll → adaptAll → selectAll
 * → convertAll} pipeline (mandatory: {@code forkAll} re-expands alternatives from fragments, so the
 * lifted PARTIAL/FINAL are un-adapted; {@code adaptAll} re-applies {@code DistributedAggregate-
 * Rewriter} before {@code convertAll}, mirroring {@code HashShuffleAggregateDAGRewriter}). The
 * broadcast build/capture/inject is the dispatcher's job — see {@code DistributedAggOverJoinDispatch}.
 *
 * @opensearch.internal
 */
public final class DistributedAggOverJoinRewriter {

    private DistributedAggOverJoinRewriter() {}

    /**
     * True iff {@code dag}'s root stage is the distributable q5/q10 shape: an {@code Aggregate(SINGLE)}
     * (optionally under a single {@code Sort}) over a partition-preserving coordinator path
     * ({@code Project}/{@code Filter}/INNER-{@code Join}) that bottoms out at a single liftable join
     * (a {@link OpenSearchJoin} over two {@link OpenSearchShuffleExchange}s — what
     * {@link CascadeShuffleDAGRewriter#isCascade} recognizes), AND the aggregate is decomposable
     * ({@link OpenSearchAggregateSplitRule#shouldSkipPartialFinalSplit} is false), AND every
     * dimension join input on the path is a broadcastable reducer-fed child stage.
     *
     * <p>Read-only: makes no decisions about node placement, just gates the rewrite. Callers must
     * still confirm the DAG is a cascade ({@link CascadeShuffleDAGRewriter#isCascade}) — the lift
     * delegates the bottom join's worker construction to the cascade machinery.
     */
    public static boolean canPushPartial(QueryDAG dag) {
        if (dag == null || dag.rootStage() == null) {
            return false;
        }
        return analyze(dag.rootStage()) != null;
    }

    /**
     * Structural analysis of the root stage: locates the {@code Sort?}, the {@code Aggregate(SINGLE)},
     * the coordinator path operators (each rebuilt above the worker StageInputScan on the FINAL side),
     * the liftable join, and the dimension child-stage ids referenced by {@link OpenSearchStageInputScan}
     * leaves on the path (which become broadcast builds). Returns {@code null} when the shape or the
     * correctness gates do not hold.
     */
    static Analysis analyze(Stage rootStage) {
        RelNode fragment = RelNodeUtils.unwrapHep(rootStage.getFragment());
        // Peel the chain of single-input coordinator ops ABOVE the aggregate (top-down). The real q5
        // root is Sort(Aggregate(...)); the real q10 root is Project(Sort(Sort(Sort(Aggregate(...))))))
        // (verified from the sf=10 cluster log). All of Sort/Project/Filter are coordinator-side
        // post-aggregate ops that stay on the coordinator above the rebuilt FINAL — capture the whole
        // chain in top-down order so coordinatorFragment can replay it. A window Project is excluded:
        // a RexOver above a distributed agg would need the gathered FINAL output anyway, but is not a
        // shape q5/q10 produce, so reject it to stay conservative.
        List<RelNode> aboveAgg = new ArrayList<>();
        while (fragment instanceof OpenSearchSort
            || (fragment instanceof OpenSearchProject p && !p.containsOver())
            || fragment instanceof OpenSearchFilter) {
            if (fragment.getInputs().size() != 1) {
                return null;
            }
            aboveAgg.add(fragment);
            fragment = RelNodeUtils.unwrapHep(fragment.getInput(0));
        }
        if (!(fragment instanceof OpenSearchAggregate aggregate) || aggregate.getMode() != AggregateMode.SINGLE) {
            return null;
        }
        // Shared decomposability gate: STATE_EXPANDING / DISTINCT / cross-family-non-prefix stay
        // coordinator-centric. q5/q10 are pure SUM → passes. (correctness gate #1)
        if (OpenSearchAggregateSplitRule.shouldSkipPartialFinalSplit(aggregate)) {
            return null;
        }
        // Empty-group aggregate (`stats sum(x)` with no `by`) produces exactly ONE row, so the
        // coordinator gather of the partials is already bounded (≤ N partial rows, one per partition)
        // — there is NO OOM motivation to distribute it. Declining empty-group also sidesteps the
        // empty-group COUNT→SUM nullability gap that OpenSearchAggregateSplitRule.onMatch handles with
        // wrapWithCastIfNeeded (a coord-side Volcano-only fixup this rewriter does not replicate). q5/q10
        // both group `by` a non-empty key set, so this never excludes them. (correctness gate #1b)
        if (aggregate.getGroupSet().isEmpty()) {
            return null;
        }
        // Walk the coordinator path from the aggregate's child down to the liftable join, collecting
        // dimension (reducer-fed) child-stage ids to broadcast. The path must be partition-preserving
        // (INNER joins / Project-without-window / Filter) and bottom out at exactly one liftable join.
        RelNode preAgg = RelNodeUtils.unwrapHep(aggregate.getInput());
        OpenSearchJoin liftable = CascadeShuffleDAGRewriter.findLiftableJoin(preAgg);
        if (liftable == null) {
            return null;
        }
        List<Integer> dimStageIds = new ArrayList<>();
        if (!collectBroadcastableDims(preAgg, liftable, dimStageIds)) {
            return null;
        }
        // The pre-agg pipeline must reference at least one dimension to be worth the broadcast machinery
        // AND for the trap to apply; a bare Agg(SINGLE) directly over the liftable join (no dim joins)
        // is the q3 shape, which the plain cascade already handles (agg stays coordinator, gather is
        // bounded by the join output the cascade already distributes). Pushing PARTIAL there is a valid
        // optional optimization but out of scope here — keep this rewriter to the dimension-join shape.
        if (dimStageIds.isEmpty()) {
            return null;
        }
        return new Analysis(List.copyOf(aboveAgg), aggregate, preAgg, liftable, List.copyOf(dimStageIds));
    }

    /**
     * Walks the coordinator path from {@code node} down to {@code liftable}, validating each operator
     * is partition-preserving and collecting the child-stage id of every {@link OpenSearchStageInputScan}
     * leaf that is NOT under the liftable join (those are the dimension inputs to broadcast). Returns
     * false (bail to coord-centric) on any unsafe operator or a dimension input that is not a plain
     * reducer-fed stage-input scan.
     */
    private static boolean collectBroadcastableDims(RelNode node, OpenSearchJoin liftable, List<Integer> dimStageIds) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        if (n == liftable) {
            // Reached the liftable join — its own two shuffle inputs are the cascade producers, NOT
            // dimensions. Stop the descent here; do not collect its StageInputScan leaves.
            return true;
        }
        if (n instanceof OpenSearchJoin join) {
            // INNER only — an outer/semi/anti dimension join cannot run as a per-partition broadcast
            // probe on the worker (null-side / build-side-preservation semantics differ). (gate #2)
            if (join.getJoinType() != JoinRelType.INNER) {
                return false;
            }
            // Exactly one input contains the liftable join (the probe side); the OTHER input is the
            // dimension build side — it must be a reducer-fed StageInputScan to broadcast.
            RelNode left = RelNodeUtils.unwrapHep(join.getInput(0));
            RelNode right = RelNodeUtils.unwrapHep(join.getInput(1));
            boolean liftLeft = containsLiftable(left, liftable);
            boolean liftRight = containsLiftable(right, liftable);
            if (liftLeft == liftRight) {
                // Both or neither contain the liftable join — not the q5/q10 left-deep dimension shape.
                return false;
            }
            RelNode probeSide = liftLeft ? left : right;
            RelNode dimSide = liftLeft ? right : left;
            if (!collectDimInput(dimSide, dimStageIds)) {
                return false;
            }
            return collectBroadcastableDims(probeSide, liftable, dimStageIds);
        }
        if (n instanceof OpenSearchProject project) {
            // Row-wise only without a window — a RexOver needs the full gathered input. (gate #3)
            if (project.containsOver()) {
                return false;
            }
            return collectBroadcastableDims(n.getInput(0), liftable, dimStageIds);
        }
        if (n instanceof OpenSearchFilter) {
            return collectBroadcastableDims(n.getInput(0), liftable, dimStageIds);
        }
        // Aggregate / Sort / Limit / Union / anything else between the agg and the liftable join is
        // NOT partition-preserving — bail to coordinator-centric. (gate #3)
        return false;
    }

    /** A dimension build side must resolve to a single SINGLETON-reducer-fed
     *  {@link OpenSearchStageInputScan} — i.e. {@code OpenSearchExchangeReducer(StageInputScan)} (the
     *  shape DAGBuilder leaves in the root fragment when it cuts a dimension reducer into a child
     *  stage). Anything else (a nested join, a bare scan, another shuffle) cannot be broadcast onto
     *  the worker by this rewriter → bail. (gate #4) */
    private static boolean collectDimInput(RelNode dimSide, List<Integer> dimStageIds) {
        RelNode d = RelNodeUtils.unwrapHep(dimSide);
        OpenSearchStageInputScan sis = dimStageInputScan(d);
        if (sis != null) {
            dimStageIds.add(sis.getChildStageId());
            return true;
        }
        return false;
    }

    /** Returns the {@link OpenSearchStageInputScan} a dimension reducer wraps, or {@code null} if the
     *  node is not a {@code reducer(StageInputScan)} / bare {@code StageInputScan}. */
    private static OpenSearchStageInputScan dimStageInputScan(RelNode node) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        if (n instanceof OpenSearchExchangeReducer reducer) {
            RelNode inner = RelNodeUtils.unwrapHep(reducer.getInput());
            return inner instanceof OpenSearchStageInputScan sis ? sis : null;
        }
        return n instanceof OpenSearchStageInputScan sis ? sis : null;
    }

    private static boolean containsLiftable(RelNode node, OpenSearchJoin liftable) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        if (n == liftable) {
            return true;
        }
        for (RelNode input : n.getInputs()) {
            if (containsLiftable(input, liftable)) {
                return true;
            }
        }
        return false;
    }

    /** Result of {@link #analyze}: the parts of the root fragment the rewrite needs. {@code aboveAgg}
     *  is the chain of coordinator ops ABOVE the aggregate, TOP-DOWN (e.g. {@code [Project, Sort, Sort,
     *  Sort]} for q10, {@code [Sort]} for q5, {@code []} for a bare agg) — replayed above FINAL. */
    record Analysis(List<RelNode> aboveAgg, OpenSearchAggregate aggregate, RelNode preAgg, OpenSearchJoin liftable, List<
        Integer> dimStageIds) {
    }

    /** A dimension stage to broadcast onto the worker, with the named-input id the worker resolves. */
    public record BroadcastBuild(Stage buildStage, String namedInputId) {
    }

    /** Result of {@link #rewriteStructure}: the rewritten DAG, the dimension build stages (the
     *  dispatcher runs + captures these and injects them into the top worker), and the cascade
     *  structure (whose {@code buildLevels()} the dispatcher uses for the shuffle producer/worker
     *  enrichment of every join level). The top worker is the cascade structure's top-level worker. */
    public record Structure(QueryDAG dag, List<BroadcastBuild> broadcastBuilds, CascadeShuffleDAGRewriter.Structure cascade) {
    }

    /**
     * Rewrites {@code dag} into the distributed-agg-over-cascade shape and runs the full convert
     * pipeline. Delegates the worker-tier surgery to {@link CascadeShuffleDAGRewriter} via an
     * {@link CascadeShuffleDAGRewriter.AggLift} hook that decorates the top worker fragment with the
     * lifted pre-aggregate pipeline + {@code Aggregate(PARTIAL)} and rebuilds the coordinator as
     * {@code Sort? → Aggregate(FINAL)}.
     *
     * @throws IllegalStateException if the shape no longer matches (callers must gate on
     *     {@link #canPushPartial} + {@link CascadeShuffleDAGRewriter#isCascade} first)
     */
    public static Structure rewrite(
        QueryDAG dag,
        org.opensearch.analytics.planner.CapabilityRegistry registry,
        boolean preferMetadataDriver,
        CascadeShuffleDAGRewriter.NodeListResolver nodeResolver
    ) {
        Structure structure = rewriteStructure(dag, registry, nodeResolver);
        QueryDAG rewrittenDag = structure.dag();
        // Mandatory full pipeline (see class javadoc): forkAll re-expands alternatives from fragments,
        // adaptAll re-applies DistributedAggregateRewriter to the lifted FINAL aggregate, selectAll
        // re-applies the parent-backend correctness constraint, convertAll converts each fragment.
        PlanForker.forkAll(rewrittenDag, registry);
        BackendPlanAdapter.adaptAll(rewrittenDag, registry);
        PlanAlternativeSelector.selectAll(rewrittenDag, registry, preferMetadataDriver);
        FragmentConversionDriver.convertAll(rewrittenDag, registry);
        return structure;
    }

    /**
     * The structural half of {@link #rewrite} — no convert pipeline. Split out so unit tests validate
     * the rewritten DAG shape against a mock backend that has no fragment convertor.
     */
    public static Structure rewriteStructure(
        QueryDAG dag,
        org.opensearch.analytics.planner.CapabilityRegistry registry,
        CascadeShuffleDAGRewriter.NodeListResolver nodeResolver
    ) {
        Stage root = dag.rootStage();
        Analysis a = analyze(root);
        if (a == null) {
            throw new IllegalStateException(
                "DistributedAggOverJoinRewriter: root stage is not the distributable agg-over-cascade shape: "
                    + root.getFragment().explain()
            );
        }
        OpenSearchAggregate aggregate = a.aggregate();

        // Tag the dimension reducer-fed child stages BROADCAST_BUILD and collect them — the dispatcher
        // runs + captures each one's IPC and injects it into the top worker's plan alternatives. They
        // ride as extra top-worker children (kept, not orphaned). Indexed off the ORIGINAL root.
        Map<Integer, Stage> byId = new HashMap<>();
        indexStages(root, byId);
        List<BroadcastBuild> broadcastBuilds = new ArrayList<>(a.dimStageIds().size());
        List<Stage> dimBuildStages = new ArrayList<>(a.dimStageIds().size());
        for (int dimStageId : a.dimStageIds()) {
            Stage dim = byId.get(dimStageId);
            if (dim == null) {
                throw new IllegalStateException("DistributedAggOverJoinRewriter: dimension child stage " + dimStageId + " missing");
            }
            dim.setRole(Stage.StageRole.BROADCAST_BUILD);
            broadcastBuilds.add(new BroadcastBuild(dim, OpenSearchBroadcastScan.namedInputIdFor(dimStageId)));
            dimBuildStages.add(dim);
        }

        // The AggLift hook builds the worker (PARTIAL pipeline) + coordinator (FINAL) fragments. The
        // cascade rewriter calls workerFragment with the SAME liftable join it lifts (both come from
        // findLiftableJoin on the same root fragment), so rewriting a.preAgg() — which contains that
        // join — reproduces the worker pre-agg pipeline with dims → broadcast scans.
        CascadeShuffleDAGRewriter.AggLift aggLift = new CascadeShuffleDAGRewriter.AggLift() {
            @Override
            public RelNode workerFragment(OpenSearchJoin liftedTopJoin, int topWorkerId) {
                // Reuse the split rule's exact PARTIAL helpers so PARTIAL/FINAL agree on safety
                // (return-type repair). The PARTIAL sits above the whole pre-agg pipeline (dim joins,
                // filter, project), with each dimension StageInputScan rewritten to a broadcast scan.
                List<AggregateCall> partialAggCalls = OpenSearchAggregateSplitRule.repairLossyReturnTypes(
                    aggregate.getAggCallList(),
                    a.preAgg()
                );
                RelNode workerPreAgg = rewriteDimsToBroadcast(a.preAgg());
                return new OpenSearchAggregate(
                    aggregate.getCluster(),
                    aggregate.getTraitSet(),
                    workerPreAgg,
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    partialAggCalls,
                    AggregateMode.PARTIAL,
                    aggregate.getViableBackends(),
                    aggregate.getCallAnnotations()
                );
            }

            @Override
            public RelNode coordinatorFragment(Stage topWorker) {
                // The coordinator FINAL reads the worker's PARTIAL output via a StageInputScan. The
                // worker output row type = PARTIAL's row type (group keys, then partial-state columns);
                // build a representative PARTIAL to derive it. FINAL's aggCalls rebase against that
                // input via the shared FinalAggCallBuilder (argList → groupCount+i, COUNT→SUM, types).
                OpenSearchAggregate partialForType = (OpenSearchAggregate) RelNodeUtils.unwrapHep(topWorker.getFragment());
                RelDataType partialRowType = partialForType.getRowType();
                OpenSearchStageInputScan workerScan = new OpenSearchStageInputScan(
                    aggregate.getCluster(),
                    aggregate.getTraitSet(),
                    topWorker.getStageId(),
                    partialRowType,
                    aggregate.getViableBackends(),
                    partialForType.getOutputFieldStorage()
                );
                Map<Integer, List<org.apache.calcite.rex.RexLiteral>> finalExtraLiterals = OpenSearchAggregateSplitRule
                    .captureLiteralArgsForFinal(aggregate.getAggCallList(), a.preAgg());
                List<IntermediateField> intermediateFields = FinalAggCallBuilder.classify(aggregate.getAggCallList());
                List<AggregateCall> finalAggCalls = FinalAggCallBuilder.buildFinalCalls(
                    aggregate.getAggCallList(),
                    intermediateFields,
                    aggregate.getGroupSet().cardinality(),
                    workerScan,
                    aggregate.getGroupSet().isEmpty()
                );
                OpenSearchAggregate finalAgg = new OpenSearchAggregate(
                    aggregate.getCluster(),
                    aggregate.getTraitSet(),
                    workerScan,
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    finalAggCalls,
                    AggregateMode.FINAL,
                    aggregate.getViableBackends(),
                    aggregate.getCallAnnotations(),
                    finalExtraLiterals,
                    intermediateFields
                );
                // Replay the coordinator op-chain that sat ABOVE the original SINGLE aggregate, now
                // above FINAL (q5: [Sort]; q10: [Project, Sort, Sort, Sort]). aboveAgg is top-down, so
                // rebuild bottom-up: start from FINAL and copy() each op onto the running child. copy()
                // rebinds an op onto a new input; its collation / project indexes reference the
                // aggregate output positions, unchanged by the SINGLE→FINAL rewrite (FINAL's row type
                // equals SINGLE's group-keys-then-agg-results layout).
                RelNode coord = finalAgg;
                List<RelNode> aboveAgg = a.aboveAgg();
                for (int i = aboveAgg.size() - 1; i >= 0; i--) {
                    RelNode op = aboveAgg.get(i);
                    coord = op.copy(op.getTraitSet(), List.of(coord));
                }
                return coord;
            }

            @Override
            public List<Stage> broadcastBuildStages() {
                return dimBuildStages;
            }
        };

        CascadeShuffleDAGRewriter.Structure cascadeStructure = CascadeShuffleDAGRewriter.rewriteStructure(
            dag,
            registry,
            nodeResolver,
            aggLift
        );

        return new Structure(cascadeStructure.dag(), List.copyOf(broadcastBuilds), cascadeStructure);
    }

    /** Rewrites every {@link OpenSearchStageInputScan} that is a DIMENSION input (i.e. not under a
     *  shuffle exchange) into an {@link OpenSearchBroadcastScan}. Stage-input scans under a shuffle
     *  (the liftable join's cascade producers) are LEFT untouched — those are consumed via the
     *  partition stream, not broadcast. */
    private static RelNode rewriteDimsToBroadcast(RelNode node) {
        RelNode n = RelNodeUtils.unwrapHep(node);
        // A shuffle exchange's subtree is a cascade producer — its StageInputScan stays a stage input
        // (consumed via the partition stream, not broadcast). Leave the whole shuffle subtree intact.
        if (n instanceof OpenSearchShuffleExchange) {
            return node;
        }
        // A dimension reducer over a StageInputScan (DAGBuilder's SINGLETON-gather marker) becomes a
        // broadcast scan: the worker resolves broadcast-<dimStageId> from the injected memtable. The
        // broadcast scan carries the reducer's output row type + viable backends.
        if (n instanceof OpenSearchExchangeReducer reducer) {
            RelNode inner = RelNodeUtils.unwrapHep(reducer.getInput());
            if (inner instanceof OpenSearchStageInputScan sis) {
                // Thread the reducer's EXACT per-column FieldStorageInfo into the broadcast scan. The
                // broadcast scan replaces the reducer as one input of the worker's dimension join; the
                // Project/PARTIAL-agg above that join resolves RexInputRefs against the union of its
                // inputs' getOutputFieldStorage(). An empty/short list here truncates that union and
                // throws "RexInputRef[N] has no matching FieldStorageInfo entry" at conversion (q5/q10).
                return new OpenSearchBroadcastScan(
                    reducer.getCluster(),
                    reducer.getTraitSet(),
                    sis.getChildStageId(),
                    reducer.getRowType(),
                    reducer.getViableBackends(),
                    reducer.getOutputFieldStorage()
                );
            }
        }
        List<RelNode> newInputs = new ArrayList<>(n.getInputs().size());
        boolean changed = false;
        for (RelNode input : n.getInputs()) {
            RelNode rewritten = rewriteDimsToBroadcast(input);
            newInputs.add(rewritten);
            if (rewritten != RelNodeUtils.unwrapHep(input) && rewritten != input) {
                changed = true;
            }
        }
        return changed ? n.copy(n.getTraitSet(), newInputs) : n;
    }

    private static void indexStages(Stage stage, Map<Integer, Stage> out) {
        if (stage == null) {
            return;
        }
        out.put(stage.getStageId(), stage);
        for (Stage child : stage.getChildStages()) {
            indexStages(child, out);
        }
    }
}
