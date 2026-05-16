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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * OpenSearch custom Aggregate carrying viable backend list and per-call annotations.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregate extends Aggregate implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final AggregateMode mode;

    public OpenSearchAggregate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        ImmutableBitSet groupSet,
        List<ImmutableBitSet> groupSets,
        List<AggregateCall> aggCalls,
        AggregateMode mode,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, List.of(), input, groupSet, groupSets, aggCalls);
        this.mode = mode;
        this.viableBackends = viableBackends;
    }

    public AggregateMode getMode() {
        return mode;
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
        return new OpenSearchAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls, mode, viableBackends);
    }

    /**
     * SINGLE aggregate is only correct when its input is already on one node — SINGLETON
     * in either kind. Over partitioned input (RANDOM) each shard would aggregate its own
     * rows independently and the results would never merge. Returning infinite cost forces
     * Volcano to pick the {@link org.opensearch.analytics.planner.rules.OpenSearchAggregateSplitRule}
     * alternative (PARTIAL ← ER ← FINAL) instead.
     *
     * <p>Accepts:
     * <ul>
     *   <li>{@code SOURCE(SINGLETON)} — single-shard scan, all rows co-located.</li>
     *   <li>{@code EXECUTION(SINGLETON)} — gathered pipeline (e.g. nested aggregate over
     *       an inner FINAL's output).</li>
     *   <li>{@code ANY} — Volcano's "still exploring" placeholder; don't prune before
     *       conversions land.</li>
     * </ul>
     *
     * <p>PARTIAL and FINAL modes skip the gate — PARTIAL is shard-side by contract,
     * FINAL always sits over an ER (SINGLETON input by construction).
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
        List<OperatorAnnotation> annotations = new ArrayList<>();
        for (AggregateCall aggCall : getAggCallList()) {
            for (RexNode rex : aggCall.rexList) {
                if (rex instanceof AggregateCallAnnotation annotation) {
                    annotations.add(annotation);
                }
            }
        }
        return annotations;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        int annotationIndex = 0;
        List<AggregateCall> resolvedCalls = new ArrayList<>();
        for (AggregateCall aggCall : getAggCallList()) {
            List<RexNode> newRexList = new ArrayList<>();
            for (RexNode rex : aggCall.rexList) {
                if (rex instanceof AggregateCallAnnotation) {
                    newRexList.add((RexNode) resolvedAnnotations.get(annotationIndex++));
                } else {
                    // Non-annotation entries (e.g. argument refs) are passed through unchanged.
                    newRexList.add(rex);
                }
            }
            resolvedCalls.add(
                AggregateCall.create(
                    aggCall.getAggregation(),
                    aggCall.isDistinct(),
                    aggCall.isApproximate(),
                    aggCall.ignoreNulls(),
                    newRexList,
                    aggCall.getArgList(),
                    aggCall.filterArg,
                    aggCall.distinctKeys,
                    aggCall.collation,
                    aggCall.type,
                    aggCall.name
                )
            );
        }
        return new OpenSearchAggregate(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            getGroupSet(),
            getGroupSets(),
            resolvedCalls,
            mode,
            List.of(backend)
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return stripAnnotations(strippedChildren, OperatorAnnotation::unwrap);
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren, Function<OperatorAnnotation, RexNode> annotationResolver) {
        List<AggregateCall> strippedCalls = new ArrayList<>();
        for (AggregateCall aggCall : getAggCallList()) {
            // TODO: when aggregate delegation is implemented, use annotationResolver
            // to replace delegated AggregateCallAnnotations with placeholders instead
            // of just filtering them out.
            List<RexNode> cleanRexList = aggCall.rexList.stream().filter(rex -> !(rex instanceof AggregateCallAnnotation)).toList();
            strippedCalls.add(
                AggregateCall.create(
                    aggCall.getAggregation(),
                    aggCall.isDistinct(),
                    aggCall.isApproximate(),
                    aggCall.ignoreNulls(),
                    cleanRexList,
                    aggCall.getArgList(),
                    aggCall.filterArg,
                    aggCall.distinctKeys,
                    aggCall.collation,
                    aggCall.type,
                    aggCall.name
                )
            );
        }
        return LogicalAggregate.create(strippedChildren.getFirst(), List.of(), getGroupSet(), getGroupSets(), strippedCalls);
    }
}
