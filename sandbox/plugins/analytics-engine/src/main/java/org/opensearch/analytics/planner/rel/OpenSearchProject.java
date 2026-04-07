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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.planner.FieldStorageInfo;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.ArrayList;
import java.util.List;

/**
 * @opensearch.internal
 */
public class OpenSearchProject extends Project implements OpenSearchRelNode {

    private final List<String> viableBackends;

    public OpenSearchProject(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        List<? extends RexNode> projects,
        RelDataType rowType,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, List.of(), input, projects, rowType);
        this.viableBackends = viableBackends;
    }

    @Override
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (!(input instanceof OpenSearchRelNode openSearchChild)) {
            throw new IllegalStateException("Project child is not OpenSearchRelNode: " + input.getClass().getSimpleName());
        }
        List<FieldStorageInfo> inputStorage = openSearchChild.getOutputFieldStorage();

        List<FieldStorageInfo> result = new ArrayList<>(getProjects().size());
        for (int i = 0; i < getProjects().size(); i++) {
            RexNode expr = getProjects().get(i);
            if (expr instanceof RexInputRef ref && ref.getIndex() < inputStorage.size()) {
                result.add(inputStorage.get(ref.getIndex()));
            } else {
                String fieldName = getRowType().getFieldList().get(i).getName();
                result.add(FieldStorageInfo.derivedColumn(fieldName, getRowType().getFieldList().get(i).getType().getSqlTypeName()));
            }
        }
        return result;
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new OpenSearchProject(getCluster(), traitSet, input, projects, rowType, viableBackends);
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
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression annotated) {
                annotations.add(annotated);
            }
        }
        return annotations;
    }

    @Override
    public OperatorCapability getOperatorCapability() {
        return OperatorCapability.PROJECT;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        int annotationIndex = 0;
        List<RexNode> resolvedExprs = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression) {
                resolvedExprs.add((RexNode) resolvedAnnotations.get(annotationIndex++));
            } else {
                resolvedExprs.add(expr);
            }
        }
        return new OpenSearchProject(getCluster(), getTraitSet(), children.getFirst(), resolvedExprs, getRowType(), List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        List<RexNode> strippedExprs = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression annotated) {
                strippedExprs.add(annotated.unwrap());
            } else {
                strippedExprs.add(expr);
            }
        }
        return LogicalProject.create(strippedChildren.getFirst(), List.of(), strippedExprs, getRowType());
    }
}
