/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
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
        super(cluster, traitSet, List.of(), input, groupSet, groupSets, aggCalls);
        this.mode = mode;
        this.viableBackends = viableBackends;
        this.callAnnotations = Map.copyOf(callAnnotations);
        this.finalExtraLiteralArgs = Map.copyOf(finalExtraLiteralArgs);
        // Collections.unmodifiableList — List.copyOf would NPE on the null pass-through entries.
        this.perCallIntermediateField = Collections.unmodifiableList(new ArrayList<>(perCallIntermediateField));
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
     * independently, results would never merge). Return infinite cost so Volcano picks the
     * split alternative. Allow SOURCE/EXECUTION SINGLETON (already gathered) and ANY
     * (Volcano's "still exploring" placeholder). PARTIAL/FINAL skip the gate.
     *
     * <p><b>DO NOT REMOVE the infinite-cost branch below.</b> It is the correctness backstop for
     * the whole split: {@code OpenSearchAggregateSplitRule} now emits a single alternative
     * deterministically (no cost comparison), so this gate is the ONLY thing that rejects a SINGLE
     * aggregate placed over RANDOM (multi-shard) input. Without it, Volcano can legally land a
     * SINGLE aggregate directly on partitioned data — each shard aggregates in isolation, the
     * partials never merge, and queries return silently wrong results. Plan-shape tests happen to
     * catch the current shapes, but they are not a substitute for this gate; deleting it breaks
     * correctness, not just a test.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (mode == AggregateMode.SINGLE) {
            for (int index = 0; index < getInput().getTraitSet().size(); index++) {
                RelTrait trait = getInput().getTraitSet().getTrait(index);
                if (!(trait instanceof OpenSearchDistribution distribution)) continue;
                boolean singletonOrAny = distribution.getType() == RelDistribution.Type.SINGLETON
                    || distribution.getType() == RelDistribution.Type.ANY;
                if (!singletonOrAny) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
            }
        }
        return planner.getCostFactory().makeTinyCost();
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
