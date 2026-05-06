/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * OpenSearch custom Aggregate carrying viable backend list and per-call annotations.
 *
 * <p>Annotations live in {@link #callAnnotations}, a side map keyed by aggregate-call
 * index. They are NOT stored in the call's {@code rexList} — see
 * {@link AggregateCallAnnotation} for why.
 *
 * @opensearch.internal
 */
public class OpenSearchAggregate extends Aggregate implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final AggregateMode mode;
    private final Map<Integer, AggregateCallAnnotation> callAnnotations;

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
        super(cluster, traitSet, List.of(), input, groupSet, groupSets, aggCalls);
        this.mode = mode;
        this.viableBackends = viableBackends;
        this.callAnnotations = Map.copyOf(callAnnotations);
    }

    public AggregateMode getMode() {
        return mode;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Per-call annotations keyed by aggregate-call index. */
    public Map<Integer, AggregateCallAnnotation> getCallAnnotations() {
        return callAnnotations;
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
        return new OpenSearchAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls, mode, viableBackends, callAnnotations);
    }

    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(
        org.apache.calcite.plan.RelOptPlanner planner,
        org.apache.calcite.rel.metadata.RelMetadataQuery mq
    ) {
        // SINGLE mode aggregate over partitioned input can't execute without splitting.
        // Return infinite cost to force Volcano to explore the split rule.
        // SINGLE over SINGLETON input is fine (single-shard case).
        if (mode == AggregateMode.SINGLE) {
            for (int index = 0; index < getInput().getTraitSet().size(); index++) {
                org.apache.calcite.plan.RelTrait trait = getInput().getTraitSet().getTrait(index);
                if (trait instanceof OpenSearchDistribution distribution
                    && distribution.getType() != org.apache.calcite.rel.RelDistribution.Type.SINGLETON
                    && distribution.getType() != org.apache.calcite.rel.RelDistribution.Type.ANY) {
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
        // Iteration order matches insertion order (LinkedHashMap in the rule), which
        // copyResolved relies on to align with resolvedAnnotations.
        return List.copyOf(callAnnotations.values());
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        Map<Integer, AggregateCallAnnotation> resolvedMap = new LinkedHashMap<>();
        int annotationIndex = 0;
        for (Map.Entry<Integer, AggregateCallAnnotation> entry : callAnnotations.entrySet()) {
            resolvedMap.put(entry.getKey(), (AggregateCallAnnotation) resolvedAnnotations.get(annotationIndex++));
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
            resolvedMap
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return stripAnnotations(strippedChildren, OperatorAnnotation::unwrap);
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren, Function<OperatorAnnotation, RexNode> annotationResolver) {
        // Per the PR 21424 refactor (3e50f563394), AggregateCallAnnotations live in a
        // side map on OpenSearchAggregate — NOT in each AggregateCall's rexList — so
        // pass the calls through unchanged. The annotationResolver is ignored for
        // aggregates; it's retained on the interface for symmetry with other
        // OpenSearch RelNodes (filter/project) that DO carry annotations inline.
        return LogicalAggregate.create(strippedChildren.getFirst(), List.of(), getGroupSet(), getGroupSets(), getAggCallList());
    }
}
