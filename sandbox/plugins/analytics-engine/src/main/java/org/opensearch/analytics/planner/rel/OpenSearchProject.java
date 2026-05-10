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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * OpenSearch custom Project carrying viable backend list and per-expression annotations.
 *
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
            if (expr instanceof AnnotatedProjectExpression annotation) {
                annotations.add(annotation);
            }
        }
        return annotations;
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        int annotationIndex = 0;
        List<RexNode> resolvedExprs = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression) {
                resolvedExprs.add((RexNode) resolvedAnnotations.get(annotationIndex++));
            } else {
                // Plain expressions (field refs, literals, scalar calls) have no annotation — pass through.
                resolvedExprs.add(expr);
            }
        }
        return new OpenSearchProject(getCluster(), getTraitSet(), children.getFirst(), resolvedExprs, getRowType(), List.of(backend));
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return stripAnnotations(strippedChildren, OperatorAnnotation::unwrap);
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren, Function<OperatorAnnotation, RexNode> annotationResolver) {
        // OpenSearchProjectRule.annotateExpr recurses into operands when validating viable
        // backends, so a top-level call like COALESCE(num0, CEIL(num1)) ends up with the inner
        // CEIL also wrapped. The supplied annotationResolver controls how each top-level
        // wrapper is unwrapped (defaults to OperatorAnnotation::unwrap, returning the original
        // RexNode); a RexShuttle then sweeps the resolver's result to strip any remaining
        // nested wrappers. Substrait conversion only recognizes the underlying RexCall shape,
        // so every wrapper at every depth must be removed before the plan is handed to a
        // backend's FragmentConvertor.
        //
        // Top-level baseline operators (BASELINE_SCALAR_OPS — COALESCE, CASE, CAST, arithmetic,
        // IS_NULL, …) bypass the AnnotatedProjectExpression wrap at the call site, but their
        // operands still go through annotation. The shuttle therefore runs on every project
        // expression — including plain ones — to catch annotated operands nested inside a
        // baseline-op root.
        RexShuttle nestedAnnotationStripper = new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (call instanceof AnnotatedProjectExpression nested) {
                    return nested.getOriginal().accept(this);
                }
                return super.visitCall(call);
            }
        };
        List<RexNode> strippedExprs = new ArrayList<>();
        for (RexNode expr : getProjects()) {
            if (expr instanceof AnnotatedProjectExpression annotated) {
                RexNode resolved = annotationResolver.apply(annotated);
                strippedExprs.add(resolved.accept(nestedAnnotationStripper));
            } else {
                // Baseline scalar operators (OpenSearchProjectRule.BASELINE_SCALAR_OPS —
                // COALESCE, CASE, CAST, arithmetic, IS_NULL, …) are not wrapped at the
                // top level but their operands may still be annotated. The shuttle is
                // idempotent for calls without nested wrappers, so run it unconditionally
                // to strip AnnotatedProjectExpression at any depth.
                strippedExprs.add(expr.accept(nestedAnnotationStripper));
            }
        }
        return LogicalProject.create(strippedChildren.getFirst(), List.of(), strippedExprs, getRowType());
    }
}
