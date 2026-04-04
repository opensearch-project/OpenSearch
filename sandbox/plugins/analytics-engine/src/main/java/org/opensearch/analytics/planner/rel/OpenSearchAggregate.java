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
import org.opensearch.analytics.planner.FieldStorageInfo;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.ArrayList;
import java.util.List;

/**
 * @opensearch.internal
 */
public class OpenSearchAggregate extends Aggregate implements OpenSearchRelNode {

    private final List<String> viableBackends;
    private final AggregateMode mode;

    public OpenSearchAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                               ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                               List<AggregateCall> aggCalls, AggregateMode mode,
                               List<String> viableBackends) {
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
            outputStorage.add(FieldStorageInfo.derivedColumn(aggCall.getName(),
                aggCall.getType().getSqlTypeName()));
        }

        return outputStorage;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new OpenSearchAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls, mode, viableBackends);
    }

    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(org.apache.calcite.plan.RelOptPlanner planner,
                                                               org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
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
    public OperatorCapability getOperatorCapability() {
        return OperatorCapability.AGGREGATE;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("mode", mode).item("viableBackends", viableBackends);
    }

    @Override
    public List<OperatorAnnotation> getAnnotations() {
        List<OperatorAnnotation> annotations = new ArrayList<>();
        for (AggregateCall aggCall : getAggCallList()) {
            AggregateCallAnnotation annotation = AggregateCallAnnotation.find(aggCall);
            if (annotation != null) {
                annotations.add(annotation);
            }
        }
        return annotations;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children,
                                List<OperatorAnnotation> resolvedAnnotations) {
        int annotationIndex = 0;
        List<AggregateCall> resolvedCalls = new ArrayList<>();
        for (AggregateCall aggCall : getAggCallList()) {
            AggregateCallAnnotation existing = AggregateCallAnnotation.find(aggCall);
            if (existing != null) {
                AggregateCallAnnotation resolved = (AggregateCallAnnotation) resolvedAnnotations.get(annotationIndex++);
                // Replace annotation in rexList
                List<RexNode> newRexList = new ArrayList<>();
                for (RexNode rex : aggCall.rexList) {
                    if (rex instanceof AggregateCallAnnotation) {
                        newRexList.add(resolved);
                    } else {
                        newRexList.add(rex);
                    }
                }
                resolvedCalls.add(AggregateCall.create(
                    aggCall.getAggregation(), aggCall.isDistinct(), aggCall.isApproximate(),
                    aggCall.ignoreNulls(), newRexList, aggCall.getArgList(), aggCall.filterArg,
                    aggCall.distinctKeys, aggCall.collation, aggCall.type, aggCall.name
                ));
            } else {
                resolvedCalls.add(aggCall);
            }
        }
        return new OpenSearchAggregate(getCluster(), getTraitSet(), children.getFirst(),
            getGroupSet(), getGroupSets(), resolvedCalls, mode, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        List<AggregateCall> strippedCalls = new ArrayList<>();
        for (AggregateCall aggCall : getAggCallList()) {
            List<RexNode> cleanRexList = aggCall.rexList.stream()
                .filter(rex -> !(rex instanceof AggregateCallAnnotation))
                .toList();
            strippedCalls.add(AggregateCall.create(
                aggCall.getAggregation(), aggCall.isDistinct(), aggCall.isApproximate(),
                aggCall.ignoreNulls(), cleanRexList, aggCall.getArgList(), aggCall.filterArg,
                aggCall.distinctKeys, aggCall.collation, aggCall.type, aggCall.name
            ));
        }
        return LogicalAggregate.create(strippedChildren.getFirst(), List.of(),
            getGroupSet(), getGroupSets(), strippedCalls);
    }}
