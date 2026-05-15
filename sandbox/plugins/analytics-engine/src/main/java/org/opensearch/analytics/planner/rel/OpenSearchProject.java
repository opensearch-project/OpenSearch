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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    /**
     * Projects containing {@code RexOver} (window functions) need fully-gathered input so the
     * window's global frame semantics are correct — infinite cost unless input is SINGLETON.
     * Volcano picks the plan where an ER sits under this project.
     *
     * <p>Plain projects (no RexOver) have no ordering requirement — tiny cost unconditionally.
     */
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!containsOver()) {
            return planner.getCostFactory().makeTinyCost();
        }
        // containsOver() is Calcite's own — inherited from Project.
        for (int i = 0; i < getInput().getTraitSet().size(); i++) {
            RelTrait trait = getInput().getTraitSet().getTrait(i);
            if (trait instanceof OpenSearchDistribution distribution) {
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
                // Pass-through expressions (RexInputRef, RexLiteral) have no annotation to
                // resolve. Running the shuttle is defensive and idempotent — atomic nodes
                // contain no nested AnnotatedProjectExpression to strip.
                strippedExprs.add(expr.accept(nestedAnnotationStripper));
            }
        }

        // Lift nested RexOver expressions out of scalar calls into a child LogicalProject.
        // PPL's `bin` command lowers `bins=N` / `minspan=N` / `start=… end=…` to a single
        // top-level scalar call whose operands embed RexOver: e.g.
        //     width_bucket(f, N, MAX(f) OVER () - MIN(f) OVER (), MAX(f) OVER ())
        // DataFusion's substrait consumer auto-lifts *top-level* WindowFunction project
        // expressions into a LogicalWindow (datafusion-substrait
        // `from_project_rel`), but the nested RexOvers inside `width_bucket(...)` stay
        // where they are and reach DataFusion's physical planner — which then errors
        // with "Physical plan does not support logical expression WindowFunction(...)".
        //
        // Pre-substrait fix: walk every project expression, hoist each unique RexOver
        // into a child Project as its own top-level expression, and rewrite the original
        // expression to reference the hoisted column via RexInputRef. The child Project
        // becomes:
        //     [input_field_0, input_field_1, ..., input_field_(n-1), MAX(f) OVER (), MIN(f) OVER ()]
        // and the outer Project's expressions reference those new columns by index.
        // DataFusion sees the WindowFunctions at the top level of the inner Project and
        // wraps them in a LogicalWindow as expected.
        Project lifted = liftNestedRexOver(strippedChildren.getFirst(), strippedExprs);
        if (lifted != null) {
            return lifted;
        }
        return LogicalProject.create(strippedChildren.getFirst(), List.of(), strippedExprs, getRowType());
    }

    /**
     * Hoists nested {@link RexOver} expressions out of {@code outerExprs} into a child
     * {@link LogicalProject} sitting on top of {@code input}. Returns {@code null} if no
     * RexOver was found (caller should emit a single-level Project as before).
     */
    private Project liftNestedRexOver(RelNode input, List<RexNode> outerExprs) {
        // Collect unique RexOvers from the expression trees. LinkedHashMap by digest so
        // the same RexOver from multiple expressions (e.g. MAX(f) OVER () appearing as
        // both data_range operand and max_value operand of width_bucket) is hoisted once
        // and shares a single column slot.
        LinkedHashMap<String, RexOver> uniqueOvers = new LinkedHashMap<>();
        RexShuttle collector = new RexShuttle() {
            @Override
            public RexNode visitOver(RexOver over) {
                uniqueOvers.putIfAbsent(over.toString(), over);
                return over;
            }
        };
        for (RexNode expr : outerExprs) {
            expr.accept(collector);
        }
        if (uniqueOvers.isEmpty()) {
            return null;
        }

        int inputFieldCount = input.getRowType().getFieldCount();
        RexBuilder rexBuilder = getCluster().getRexBuilder();

        // Build the lower-Project expressions: passthrough every input field as RexInputRef,
        // then append each unique RexOver as its own top-level expression. The lower-Project's
        // row type matches: input fields followed by appended window-output columns.
        List<RexNode> lowerExprs = new ArrayList<>(inputFieldCount + uniqueOvers.size());
        for (int i = 0; i < inputFieldCount; i++) {
            lowerExprs.add(rexBuilder.makeInputRef(input, i));
        }
        // overIndex maps "over digest" → its column index in the lower Project's output.
        Map<String, Integer> overIndex = new LinkedHashMap<>();
        int nextSlot = inputFieldCount;
        for (Map.Entry<String, RexOver> entry : uniqueOvers.entrySet()) {
            overIndex.put(entry.getKey(), nextSlot++);
            lowerExprs.add(entry.getValue());
        }
        Project lowerProject = LogicalProject.create(input, List.of(), lowerExprs, (List<String>) null);

        // Rewrite outer expressions: replace each RexOver with a RexInputRef into the
        // lower Project's output. Field names of the lower Project are anonymous (Calcite
        // auto-generates) — that's fine, we reference by index.
        RexShuttle rewriter = new RexShuttle() {
            @Override
            public RexNode visitOver(RexOver over) {
                Integer slot = overIndex.get(over.toString());
                if (slot == null) {
                    // Should not happen — collector found every RexOver.
                    return super.visitOver(over);
                }
                return rexBuilder.makeInputRef(lowerProject, slot);
            }
        };
        List<RexNode> rewrittenOuter = new ArrayList<>(outerExprs.size());
        for (RexNode expr : outerExprs) {
            rewrittenOuter.add(expr.accept(rewriter));
        }

        return LogicalProject.create(lowerProject, List.of(), rewrittenOuter, getRowType());
    }
}
