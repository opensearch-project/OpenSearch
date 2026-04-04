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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @opensearch.internal
 */
public class OpenSearchFilter extends Filter implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
                            RexNode condition, List<String> viableBackends) {
        super(cluster, traitSet, input, condition);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    /** Filter doesn't change schema — pass through child's field storage. */
    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        if (getInput() instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new OpenSearchFilter(getCluster(), traitSet, input, condition, viableBackends);
    }

    @Override
    public org.apache.calcite.plan.RelOptCost computeSelfCost(org.apache.calcite.plan.RelOptPlanner planner,
                                                               org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("viableBackends", viableBackends);
    }

    @Override
    public List<OperatorAnnotation> getAnnotations() {
        List<OperatorAnnotation> annotations = new ArrayList<>();
        collectAnnotations(getCondition(), annotations);
        return annotations;
    }

    private void collectAnnotations(RexNode node, List<OperatorAnnotation> result) {
        if (node instanceof AnnotatedPredicate predicate) {
            result.add(predicate);
        } else if (node instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                collectAnnotations(operand, result);
            }
        }
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children,
                                List<OperatorAnnotation> resolvedAnnotations) {
        RexNode resolvedCondition = replaceAnnotations(getCondition(), resolvedAnnotations, new int[]{0});
        return new OpenSearchFilter(getCluster(), getTraitSet(), children.getFirst(),
            resolvedCondition, List.of(backend));
    }

    private RexNode replaceAnnotations(RexNode node, List<OperatorAnnotation> resolved, int[] index) {
        if (node instanceof AnnotatedPredicate) {
            return (RexNode) resolved.get(index[0]++);
        }
        if (node instanceof RexCall call) {
            List<RexNode> newOperands = new ArrayList<>();
            boolean changed = false;
            for (RexNode operand : call.getOperands()) {
                RexNode replaced = replaceAnnotations(operand, resolved, index);
                newOperands.add(replaced);
                if (replaced != operand) {
                    changed = true;
                }
            }
            return changed ? call.clone(call.getType(), newOperands) : call;
        }
        return node;
    }
}
