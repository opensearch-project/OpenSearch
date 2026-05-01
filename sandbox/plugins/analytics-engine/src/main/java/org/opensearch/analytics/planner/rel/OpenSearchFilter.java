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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * OpenSearch custom Filter carrying viable backend list and per-predicate annotations.
 *
 * @opensearch.internal
 */
public class OpenSearchFilter extends Filter implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition, List<String> viableBackends) {
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
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (input instanceof OpenSearchRelNode openSearchInput) {
            return openSearchInput.getOutputFieldStorage();
        }
        return List.of();
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new OpenSearchFilter(getCluster(), traitSet, input, condition, viableBackends);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
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
        // Leaf nodes (RexInputRef, RexLiteral, etc.) have no annotations — skip.
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        RexNode resolvedCondition = replaceAnnotations(getCondition(), resolvedAnnotations.listIterator());
        return new OpenSearchFilter(getCluster(), getTraitSet(), children.getFirst(), resolvedCondition, List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return LogicalFilter.create(strippedChildren.getFirst(), stripCondition(getCondition()));
    }

    private RexNode replaceAnnotations(RexNode node, ListIterator<OperatorAnnotation> annotationIterator) {
        if (node instanceof AnnotatedPredicate) return (RexNode) annotationIterator.next();
        if (node instanceof RexCall call) {
            List<RexNode> newOperands = new ArrayList<>();
            boolean changed = false;
            for (RexNode operand : call.getOperands()) {
                RexNode replaced = replaceAnnotations(operand, annotationIterator);
                newOperands.add(replaced);
                if (replaced != operand) changed = true;
            }
            return changed ? call.clone(call.getType(), newOperands) : call;
        }
        // Leaf node (RexInputRef, RexLiteral, etc.) — no annotation, pass through.
        return node;
    }

    private RexNode stripCondition(RexNode node) {
        if (node instanceof AnnotatedPredicate predicate) return predicate.unwrap();
        if (node instanceof RexCall call) {
            List<RexNode> newOperands = new ArrayList<>();
            boolean changed = false;
            for (RexNode operand : call.getOperands()) {
                RexNode stripped = stripCondition(operand);
                newOperands.add(stripped);
                if (stripped != operand) changed = true;
            }
            return changed ? call.clone(call.getType(), newOperands) : call;
        }
        return node;
    }
}
