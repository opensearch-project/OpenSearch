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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.AggregateFunction.IntermediateField;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * OpenSearch custom Aggregate carrying viable backend list and per-call annotations.
 *
 * <p>Per-call annotations are kept in a side-map keyed by call index — NOT in
 * {@code AggregateCall#rexList}. Keeping them out of rexList avoids contaminating
 * Calcite's {@code AggCallBinding.preOperands}, which would otherwise corrupt
 * inferReturnType for functions that read {@code getOperandType(0)} (PPL's
 * {@code ARG0_ARRAY} for {@code take} / {@code list} / {@code values}).
 *
 * @opensearch.internal
 */
public class OpenSearchAggregate extends Aggregate implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final AggregateMode mode;
    /**
     * Per-call annotations keyed by call index in {@link #getAggCallList()}. May be empty when
     * the aggregate has no annotations yet (pre-marking) or when copied from a Calcite-internal
     * rule that doesn't preserve them. Order is stable for {@link #getAnnotations()} /
     * {@link #copyResolved}.
     */
    private final Map<Integer, AggregateCallAnnotation> callAnnotations;
    /**
     * FINAL-side carrier for literal aggregate-args (e.g. TAKE's N) captured by the
     * split rule from the original SINGLE aggregate's child Project. Empty otherwise.
     * Used by {@code DistributedAggregateRewriter} to re-create the literals as
     * constant columns below FINAL, since the StageInputScan only carries the state.
     */
    private final Map<Integer, List<RexLiteral>> finalExtraLiteralArgs;
    /** Per-call {@link IntermediateField} classification, parallel to {@link #getAggCallList()}; null entry = no SPI decomposition; empty for SINGLE/PARTIAL. */
    private final List<IntermediateField> perCallIntermediateField;

    public OpenSearchAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        AggregateMode mode,
        List<String> viableBackends,
        Map<Integer, AggregateCallAnnotation> callAnnotations
    ) {
        this(cluster, traitSet, input, groupSet, groupSets, aggCalls, mode, viableBackends, callAnnotations, Map.of(), List.of());
    }

    public OpenSearchAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        AggregateMode mode,
        List<String> viableBackends,
        Map<Integer, AggregateCallAnnotation> callAnnotations,
        Map<Integer, List<RexLiteral>> finalExtraLiteralArgs
    ) {
        this(
            cluster,
            traitSet,
            input,
            groupSet,
            groupSets,
            aggCalls,
            mode,
            viableBackends,
            callAnnotations,
            finalExtraLiteralArgs,
            List.of()
        );
    }

    public OpenSearchAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        AggregateMode mode,
        List<String> viableBackends,
        Map<Integer, AggregateCallAnnotation> callAnnotations,
        Map<Integer, List<RexLiteral>> finalExtraLiteralArgs,
        List<IntermediateField> perCallIntermediateField
    ) {
        super(
            cluster,
            traitSet,
            List.of(),
            input,
            frontGroupSetForFinal(groupSet, mode),
            frontGroupSetsForFinal(groupSet, groupSets, mode),
            aggCalls
        );
        this.mode = mode;
        this.viableBackends = viableBackends;
        this.callAnnotations = Map.copyOf(callAnnotations);
        this.finalExtraLiteralArgs = Map.copyOf(finalExtraLiteralArgs);
        // Collections.unmodifiableList — List.copyOf would NPE on the null pass-through entries.
        this.perCallIntermediateField = Collections.unmodifiableList(new ArrayList<>(perCallIntermediateField));
    }

    /**
     * FINAL reads PARTIAL's output, where Calcite has fronted the group keys to {@code 0..n-1}; group
     * on the prefix range so a non-prefix key (e.g. {@code avg(x) by span(y,5)} → {@code {1}}) isn't
     * read as an agg-state column. No-op for SINGLE/PARTIAL (raw input) and already-fronted sets.
     */
    private static ImmutableBitSet frontGroupSetForFinal(ImmutableBitSet groupSet, AggregateMode mode) {
        return mode == AggregateMode.FINAL ? ImmutableBitSet.range(groupSet.cardinality()) : groupSet;
    }

    /**
     * Keeps {@code groupSets} consistent with the fronted {@code groupSet}. PPL only emits simple
     * (single-set) aggregates, so this collapses to one set; revisit if GROUPING SETS is ever added.
     */
    private static List<ImmutableBitSet> frontGroupSetsForFinal(
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        AggregateMode mode
    ) {
        if (mode != AggregateMode.FINAL || groupSets == null || groupSets.isEmpty()) {
            return groupSets;
        }
        return List.of(ImmutableBitSet.range(groupSet.cardinality()));
    }

    /** Builds a FINAL aggregate post-rewrite; clears both stashes so a later {@code copy()} can't replay them. */
    public static OpenSearchAggregate finalAfterRewrite(OpenSearchAggregate prior, RelNode newInput, List<AggregateCall> rebuiltCalls) {
        return new OpenSearchAggregate(
            prior.getCluster(),
            prior.getTraitSet(),
            newInput,
            prior.getGroupSet(),
            prior.getGroupSets(),
            rebuiltCalls,
            AggregateMode.FINAL,
            prior.viableBackends,
            prior.callAnnotations,
            Map.of(),
            List.of()
        );
    }

    public AggregateMode getMode() {
        return mode;
    }

    /** Returns the per-call annotation map (keyed by call index). */
    public Map<Integer, AggregateCallAnnotation> getCallAnnotations() {
        return callAnnotations;
    }

    public Map<Integer, List<RexLiteral>> getFinalExtraLiteralArgs() {
        return finalExtraLiteralArgs;
    }

    /** See {@link #perCallIntermediateField}. */
    public List<IntermediateField> getIntermediateFields() {
        return perCallIntermediateField;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /**
     * Aggregate output: group-by fields first (inherited from input), then agg results (derived).
     * Group-by fields inherit storage info from the input. Agg results are derived columns.
     */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        List<FieldStorageInfo> inputStorage = (input instanceof OpenSearchRelNode openSearchInput)
            ? openSearchInput.getOutputFieldStorage()
            : List.of();

        List<FieldStorageInfo> outputStorage = new ArrayList<>();

        // Group-by fields: inherit from input
        for (int groupIdx : getGroupSet()) {
            if (groupIdx < inputStorage.size()) {
                outputStorage.add(inputStorage.get(groupIdx));
            }
        }

        // Agg results: derived columns whose physical-deps are the union of arg refs' deps
        // (preserving first-seen order across argList, then rexList).
        for (AggregateCall aggCall : getAggCallList()) {
            LinkedHashSet<String> deps = new LinkedHashSet<>();
            for (int argIdx : aggCall.getArgList()) {
                if (argIdx >= inputStorage.size()) {
                    throw new IllegalStateException(
                        "AggregateCall arg["
                            + argIdx
                            + "] has no matching FieldStorageInfo entry "
                            + "(input only declares "
                            + inputStorage.size()
                            + " columns)"
                    );
                }
                FieldStorageInfo src = inputStorage.get(argIdx);
                if (src.isDerived()) {
                    deps.addAll(src.getDependsOnPhysicalCols());
                } else {
                    deps.add(src.getFieldName());
                }
            }
            for (RexNode rex : aggCall.rexList) {
                deps.addAll(RelNodeUtils.resolvePhysicalDeps(rex, inputStorage));
            }
            outputStorage.add(FieldStorageInfo.derivedColumn(aggCall.getName(), aggCall.getType().getSqlTypeName(), deps));
        }

        return outputStorage;
    }

    @Override
    public Aggregate copy(
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls
    ) {
        return new OpenSearchAggregate(
            getCluster(),
            traitSet,
            input,
            groupSet,
            groupSets,
            aggCalls,
            mode,
            viableBackends,
            callAnnotations,
            finalExtraLiteralArgs,
            perCallIntermediateField
        );
    }

    /**
     * SINGLE-mode aggregate over partitioned input is incorrect (each shard would aggregate
     * independently, results would never merge). FINAL has two legal input shapes:
     * SINGLETON+COORDINATOR (the M0/M1 coord-centric path — partials gathered to coord, FINAL
     * merges) and HASH+WORKER (the M3 shuffle path — partials hash-shuffled by group keys,
     * FINAL runs on each worker over its hash bucket). Anything else is rejected.
     *
     * <p>PARTIAL is unconstrained (it consumes whatever the child distribution is and emits
     * partial-state output at the same locality).
     *
     * <p>The check tolerates ANY (Volcano's "still exploring" placeholder) on either side so the
     * planner can register alternatives during memo expansion before the trait is finalized.
     *
     * <p>Cost: FINAL pays merge cost proportional to its input rows. At COORDINATOR+SINGLETON
     * the merge runs serially → cost = inputRows. At HASH+WORKER+N the merge runs across N
     * workers in parallel → cost = inputRows / N. The parallelism win is what makes the shuffle
     * path beat the coord-centric path on high-cardinality {@code GROUP BY} despite paying an
     * extra gather ER on top: for shuffle to win the savings on FINAL must exceed the extra
     * gather ER's setup + final-output rows. This naturally amortizes only at scale, leaving
     * tiny aggregates on coord-centric.
     *
     * <p><b>DO NOT REMOVE the SINGLE-mode infinite-cost branch below.</b> It is the correctness
     * backstop for the whole split: {@code OpenSearchAggregateSplitRule} now emits a single
     * alternative deterministically (no cost comparison), so this gate is the ONLY thing that
     * rejects a SINGLE aggregate placed over RANDOM (multi-shard) input. Without it, Volcano can
     * legally land a SINGLE aggregate directly on partitioned data — each shard aggregates in
     * isolation, the partials never merge, and queries return silently wrong results. Plan-shape
     * tests happen to catch the current shapes, but they are not a substitute for this gate;
     * deleting it breaks correctness, not just a test.
     *
     * <p>Top-down does NOT retire that gate, which was measured rather than assumed. The gate reads the
     * INPUT's trait, so it only works while the input subset carries a concrete distribution: seeding the
     * child (a join) UNRESOLVED instead makes the check skip, because a demand for {@code Type.ANY} is
     * satisfied by anything ({@link OpenSearchDistribution#satisfies}) and {@code ANY} means UNRESOLVED
     * here. The measured result was a plan with a per-partition {@code SINGLE} aggregate directly over a
     * 3-way hash-partitioned join, its groups concatenated by the root gather and never merged. The gate
     * and the marking rules' concrete trait claims are ONE mechanism: the claim exists so this check can
     * read it. Both retire together, when Logical/Physical aggregate nodes are split apart and every
     * physical alternative comes from {@link #passThroughTraits}/{@link #deriveTraits} — which set self
     * and input traits together, so the illegal pair cannot be constructed at all.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        OpenSearchDistribution inputDistribution = OpenSearchRelNode.distributionOf(getInput().getTraitSet());
        // An UNRESOLVED input cannot be consumed: its placement is still undecided, so nothing above it has a
        // defined cost or a defined correctness. This ONE invariant does the work that a shape-by-shape
        // legality table used to — the marking phase's seed lives in the ANY subset, and only the concrete
        // alternatives its passThrough/derive hooks produce are consumable.
        // No distribution trait at all is not the same as an unresolved one — it carries no placement claim
        // either way, so it costs what it did before distribution became a search dimension.
        if (inputDistribution == null) {
            return planner.getCostFactory().makeTinyCost();
        }
        if (inputDistribution.getType() == RelDistribution.Type.ANY) {
            return planner.getCostFactory().makeInfiniteCost();
        }
        assert assertPlacementIsLegal(inputDistribution);

        // FINAL merges its input rows, divided by the parallelism the input supplies: a coordinator gather
        // merges serially, a worker-tier shuffle merges across N partitions. That /N is what lets the shuffle
        // path win on high-cardinality GROUP BY despite the extra gather above it. Real cost, not a gate.
        if (mode == AggregateMode.FINAL) {
            boolean hashWorker = inputDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                && inputDistribution.getLocality() == OpenSearchDistribution.Locality.WORKER;
            int partitionCount = hashWorker && inputDistribution.getPartitionCount() != null
                ? Math.max(1, inputDistribution.getPartitionCount())
                : 1;
            double finalCost = mq.getRowCount(getInput()) / partitionCount;
            return planner.getCostFactory().makeCost(finalCost, finalCost, 0);
        }
        return planner.getCostFactory().makeTinyCost();
    }

    /**
     * The placement invariants each aggregate mode implies. ASSERTED, not priced — these were three
     * {@code makeInfiniteCost()} branches, i.e. legality expressed through the cost channel, which exists for
     * ranking. Each is unreachable because the requirement is now stated in the trait hooks instead:
     * <ul>
     *   <li>{@code SINGLE} needs gathered input, and {@link #passThroughTraits} demands SINGLETON;</li>
     *   <li>{@code PARTIAL} needs partitioned input, and BOTH hooks decline a singleton — the demand side in
     *       {@link #passThroughTraits} and the derive side in {@link #deriveTraits}. Volcano builds
     *       alternatives from either direction, so closing only one leaves the other able to produce the
     *       illegal pair;</li>
     *   <li>{@code FINAL} runs over a coordinator gather or a worker shuffle, and every builder sets that
     *       trait explicitly.</li>
     * </ul>
     * Asserted rather than deleted because a violation is silently wrong results — an under-count — not a slow
     * plan. Assertions are on in the suites, so a builder that breaks one fails there instead of shipping.
     *
     * @return always {@code true}, so this reads as {@code assert assertPlacementIsLegal(...)}
     * @throws IllegalStateException when a mode meets input it cannot correctly consume
     */
    private boolean assertPlacementIsLegal(OpenSearchDistribution inputDistribution) {
        // The UNRESOLVED seed makes no placement claim yet, so its mode cannot contradict its input. The HEP
        // marking rule registers exactly such a node (SINGLE over a RANDOM(SHARD) scan, self trait ANY); it is
        // costed but NOT consumable, because every parent refuses an unresolved input. Only a node that has
        // COMMITTED to a distribution can be illegal.
        OpenSearchDistribution selfDistribution = OpenSearchRelNode.distributionOf(getTraitSet());
        if (selfDistribution == null || selfDistribution.getType() == RelDistribution.Type.ANY) {
            return true;
        }
        boolean inputIsSingleton = inputDistribution.getType() == RelDistribution.Type.SINGLETON;
        if (mode == AggregateMode.SINGLE && !inputIsSingleton) {
            throw new IllegalStateException("SINGLE aggregate over partitioned input [" + inputDistribution + "] would under-count");
        }
        if (mode == AggregateMode.PARTIAL && inputIsSingleton) {
            throw new IllegalStateException("PARTIAL aggregate over already-gathered input [" + inputDistribution + "]");
        }
        if (mode == AggregateMode.FINAL) {
            boolean coordGather = inputIsSingleton && inputDistribution.getLocality() == OpenSearchDistribution.Locality.COORDINATOR;
            boolean workerShuffle = inputDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                && inputDistribution.getLocality() == OpenSearchDistribution.Locality.WORKER;
            if (!coordGather && !workerShuffle) {
                throw new IllegalStateException(
                    "FINAL aggregate over neither a coordinator gather nor a worker shuffle [" + inputDistribution + "]"
                );
            }
        }
        if (mode == AggregateMode.FINAL) {
            boolean singletonCoord = inputIsSingleton && inputDistribution.getLocality() == OpenSearchDistribution.Locality.COORDINATOR;
            boolean hashWorker = inputDistribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                && inputDistribution.getLocality() == OpenSearchDistribution.Locality.WORKER;
            if (!singletonCoord && !hashWorker) {
                throw new IllegalStateException(
                    "FINAL aggregate over neither a coordinator gather nor a worker shuffle [" + inputDistribution + "]"
                );
            }
        }
        return true;
    }

    // ---- PhysicalNode (top-down trait propagation) ----

    /**
     * Distribution demand per aggregate mode. The three modes behave differently on purpose, mirroring
     * the invariants {@link #computeSelfCost} charges infinite cost to violate:
     * <ul>
     *   <li>{@code PARTIAL} RIDES its input — it runs wherever the data already is, per shard/partition.
     *       Demanding a gather here would defeat the whole split and leave {@code FINAL} reading raw rows
     *       where it expects partial state.</li>
     *   <li>{@code FINAL} DEMANDS {@code COORDINATOR+SINGLETON}: it must see every partial to merge them.
     *       (A worker-tier hash-aggregate FINAL would demand HASH instead; we do not emit that shape.)</li>
     *   <li>{@code SINGLE} declines. A SINGLE aggregate over partitioned input is exactly what the cost
     *       gate rejects as a CORRECTNESS violation — it would under-count. Whether to split it
     *       PARTIAL/FINAL is a decision the post-CBO enforcement pass owns (it also gates on the size
     *       floor and the {@code shuffle.aggregate.enabled} toggle), so top-down must not pre-empt it by
     *       claiming an alternative here.</li>
     * </ul>
     */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        OpenSearchDistribution requiredDistribution = OpenSearchRelNode.distributionOf(required);
        if (requiredDistribution == null) {
            return null;
        }
        if (mode == AggregateMode.PARTIAL) {
            // PARTIAL rides its input's partitioning, so it cannot DELIVER a gather. Passing a SINGLETON
            // demand down would build PARTIAL-over-gathered — the very shape computeSelfCost prices at
            // infinity — so decline here rather than manufacture an alternative only cost can reject. The
            // gather belongs ABOVE a PARTIAL (the ER that FINAL reads), never below it.
            if (requiredDistribution.getType() == RelDistribution.Type.SINGLETON) {
                return null;
            }
            return Pair.of(getTraitSet().replace(requiredDistribution), List.of(getInput().getTraitSet().replace(requiredDistribution)));
        }
        if (mode == AggregateMode.FINAL) {
            if (requiredDistribution.getType() != RelDistribution.Type.SINGLETON) {
                return null;
            }
            OpenSearchDistributionTraitDef traitDef = (OpenSearchDistributionTraitDef) requiredDistribution.getTraitDef();
            OpenSearchDistribution singleton = traitDef.coordSingleton();
            return Pair.of(getTraitSet().replace(singleton), List.of(getInput().getTraitSet().replace(singleton)));
        }
        // SINGLE rides any SINGLETON demand, passing the required locality through verbatim. This is where
        // the requirement "a SINGLE aggregate needs gathered input" is DECLARED, so the planner inserts the
        // gather below rather than cost having to reject the alternative afterwards.
        if (mode == AggregateMode.SINGLE && requiredDistribution.getType() == RelDistribution.Type.SINGLETON) {
            return Pair.of(getTraitSet().replace(requiredDistribution), List.of(getInput().getTraitSet().replace(requiredDistribution)));
        }
        return null;
    }

    /**
     * Only {@code PARTIAL} derives from its child, and only to report that it rides. {@code FINAL} always
     * outputs SINGLETON regardless of its child, and {@code SINGLE} stays undecided (see
     * {@link #passThroughTraits}). No mode advertises a co-partitionable output: an aggregate's groups are
     * only hash-partitioned when it ran distributed, and FINAL gathers to the coordinator anyway.
     */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        if (childId != 0 || mode != AggregateMode.PARTIAL) {
            return null;
        }
        OpenSearchDistribution childDistribution = OpenSearchRelNode.distributionOf(childTraits);
        if (childDistribution == null) {
            return null;
        }
        // Decline an already-gathered child, mirroring passThroughTraits. A PARTIAL exists to aggregate
        // per-partition; over a singleton it is pure overhead AND it is the shape whose FINAL would then read
        // partial state where it expects raw rows. Deriving it anyway is what made the illegal pair reachable:
        // Volcano calls derive for every child subset, so a SINGLETON one produced PARTIAL@SINGLETON over
        // SINGLETON, and only cost stopped it from being chosen. Declining here closes that on the trait side,
        // which is what lets the infinite-cost branch become an assertion.
        if (childDistribution.getType() == RelDistribution.Type.SINGLETON) {
            return null;
        }
        return Pair.of(getTraitSet().replace(childDistribution), List.of(childTraits));
    }

    /**
     * A SINGLE aggregate must not have traits derived into it. {@link #computeSelfCost} charges infinite
     * cost for SINGLE-over-partitioned because it would UNDER-COUNT (each partition aggregating in
     * isolation with no FINAL merge) — a correctness backstop, not a cost preference. Calcite's default
     * {@code LEFT_FIRST} derivation would manufacture exactly that variant. FINAL is likewise prohibited:
     * it always gathers, so there is nothing to derive. Only PARTIAL rides its child.
     */
    @Override
    public DeriveMode getDeriveMode() {
        return mode == AggregateMode.PARTIAL ? DeriveMode.LEFT_FIRST : DeriveMode.PROHIBITED;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("mode", mode).item("viableBackends", viableBackends);
    }

    @Override
    public List<OperatorAnnotation> getAnnotations() {
        if (callAnnotations.isEmpty()) {
            return List.of();
        }
        List<OperatorAnnotation> annotations = new ArrayList<>(callAnnotations.size());
        for (int i = 0; i < getAggCallList().size(); i++) {
            AggregateCallAnnotation annotation = callAnnotations.get(i);
            if (annotation != null) {
                annotations.add(annotation);
            }
        }
        return annotations;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        // Rebuild the side-map preserving call-index keys, swapping annotation values
        // for the resolved (single-backend-narrowed) variants in the same iteration order
        // getAnnotations() used.
        Map<Integer, AggregateCallAnnotation> rebuilt = new LinkedHashMap<>(callAnnotations.size());
        int annotationIndex = 0;
        for (int i = 0; i < getAggCallList().size(); i++) {
            if (callAnnotations.containsKey(i)) {
                rebuilt.put(i, (AggregateCallAnnotation) resolvedAnnotations.get(annotationIndex++));
            }
        }
        return new OpenSearchAggregate(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            getGroupSet(),
            getGroupSets(),
            getAggCallList(),
            mode,
            List.of(backend),
            rebuilt,
            finalExtraLiteralArgs,
            perCallIntermediateField
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // Annotations live out-of-band; the aggCall list passes through unchanged.
        return LogicalAggregate.create(strippedChildren.getFirst(), List.of(), getGroupSet(), getGroupSets(), getAggCallList());
    }
}
