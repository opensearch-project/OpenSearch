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
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
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
        this(cluster, traitSet, input, groupSet, groupSets, aggCalls, mode, viableBackends, callAnnotations, Map.of());
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
        super(cluster, traitSet, List.of(), input, groupSet, groupSets, aggCalls);
        this.mode = mode;
        this.viableBackends = viableBackends;
        this.callAnnotations = Map.copyOf(callAnnotations);
        this.finalExtraLiteralArgs = Map.copyOf(finalExtraLiteralArgs);
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

        // Agg results: derived columns with no physical storage
        for (AggregateCall aggCall : getAggCallList()) {
            outputStorage.add(FieldStorageInfo.derivedColumn(aggCall.getName(), aggCall.getType().getSqlTypeName()));
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
            finalExtraLiteralArgs
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
        } else if (mode == AggregateMode.FINAL) {
            int partitionCount = 1;
            boolean traitResolved = false;
            for (int index = 0; index < getInput().getTraitSet().size(); index++) {
                RelTrait trait = getInput().getTraitSet().getTrait(index);
                if (!(trait instanceof OpenSearchDistribution distribution)) continue;
                if (distribution.getType() == RelDistribution.Type.ANY) continue;
                boolean singletonCoord = distribution.getType() == RelDistribution.Type.SINGLETON
                    && distribution.getLocality() == OpenSearchDistribution.Locality.COORDINATOR;
                boolean hashWorker = distribution.getType() == RelDistribution.Type.HASH_DISTRIBUTED
                    && distribution.getLocality() == OpenSearchDistribution.Locality.WORKER;
                if (!singletonCoord && !hashWorker) {
                    return planner.getCostFactory().makeInfiniteCost();
                }
                traitResolved = true;
                if (hashWorker && distribution.getPartitionCount() != null) {
                    partitionCount = Math.max(1, distribution.getPartitionCount());
                }
            }
            // FINAL pays a merge cost proportional to its input row count. Coord-centric merges
            // serially (partitionCount=1); HASH+WORKER merges in parallel across N workers
            // (partitionCount=N). The /N discount is what lets the shuffle path beat the
            // coord-centric path on high-cardinality GROUP BY despite paying an extra gather
            // ER on top — but only when the savings exceed the gather's setup, so tiny inputs
            // still route coord-centric. Use tinyCost while the trait is unresolved (Volcano's
            // ANY placeholder) so memo expansion can register alternatives without committing
            // to a cost.
            if (traitResolved) {
                double finalRows = mq.getRowCount(getInput());
                double finalCost = finalRows / partitionCount;
                return planner.getCostFactory().makeCost(finalCost, finalCost, 0);
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
            finalExtraLiteralArgs
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        // Annotations live out-of-band; the aggCall list passes through unchanged.
        return LogicalAggregate.create(strippedChildren.getFirst(), List.of(), getGroupSet(), getGroupSets(), getAggCallList());
    }
}
