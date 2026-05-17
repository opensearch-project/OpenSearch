/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Walks a resolved plan and applies per-function {@link ScalarFunctionAdapter}s
 * provided by the driving backend. Runs between plan forking and fragment conversion.
 *
 * <p>Each backend declares adapters keyed by {@link ScalarFunction} via
 * {@link org.opensearch.analytics.spi.BackendCapabilityProvider#scalarFunctionAdapters()}.
 * This component looks up the adapter for each scalar function RexCall in the plan
 * and applies it if present.
 *
 * @opensearch.internal
 */
public class BackendPlanAdapter {

    private static final Logger LOGGER = LogManager.getLogger(BackendPlanAdapter.class);

    private BackendPlanAdapter() {}

    /**
     * Adapt all plan alternatives in the DAG using each alternative's driving backend's adapters.
     */
    public static void adaptAll(QueryDAG dag, CapabilityRegistry registry) {
        adaptStage(dag.rootStage(), registry);
    }

    private static void adaptStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            adaptStage(child, registry);
        }
        List<StagePlan> adapted = new ArrayList<>(stage.getPlanAlternatives().size());
        for (StagePlan plan : stage.getPlanAlternatives()) {
            Map<ScalarFunction, ScalarFunctionAdapter> adapters = registry.getBackend(plan.backendId())
                .getCapabilityProvider()
                .scalarFunctionAdapters();
            LOGGER.debug("Before adaptation [{}]:\n{}", plan.backendId(), RelOptUtil.toString(plan.resolvedFragment()));
            RelNode fragment = adaptNode(plan.resolvedFragment(), adapters);
            LOGGER.debug("After adaptation [{}]:\n{}", plan.backendId(), RelOptUtil.toString(fragment));
            if (fragment != plan.resolvedFragment()) {
                adapted.add(new StagePlan(fragment, plan.backendId()));
            } else {
                adapted.add(plan);
            }
        }
        stage.setPlanAlternatives(adapted);
    }

    private static RelNode adaptNode(RelNode node, Map<ScalarFunction, ScalarFunctionAdapter> adapters) {
        List<RelNode> adaptedChildren = new ArrayList<>(node.getInputs().size());
        boolean childrenChanged = false;
        for (RelNode child : node.getInputs()) {
            RelNode adaptedChild = adaptNode(child, adapters);
            adaptedChildren.add(adaptedChild);
            if (adaptedChild != child) childrenChanged = true;
        }

        if (node instanceof OpenSearchFilter filter) {
            return adaptFilter(filter, adapters, adaptedChildren, childrenChanged);
        }
        if (node instanceof OpenSearchProject project) {
            return adaptProject(project, adapters, adaptedChildren, childrenChanged);
        }
        if (node instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.FINAL) {
            OpenSearchAggregate withAdaptedChildren = childrenChanged
                ? (OpenSearchAggregate) agg.copy(agg.getTraitSet(), adaptedChildren)
                : agg;
            return DistributedAggregateRewriter.rewrite(withAdaptedChildren);
        }

        return childrenChanged ? node.copy(node.getTraitSet(), adaptedChildren) : node;
    }

    private static RelNode adaptFilter(
        OpenSearchFilter filter,
        Map<ScalarFunction, ScalarFunctionAdapter> adapters,
        List<RelNode> adaptedChildren,
        boolean childrenChanged
    ) {
        List<FieldStorageInfo> fieldStorage = filter.getOutputFieldStorage();
        RexNode adaptedCondition = adaptRex(filter.getCondition(), adapters, fieldStorage, filter.getCluster());
        if (adaptedCondition != filter.getCondition() || childrenChanged) {
            return new OpenSearchFilter(
                filter.getCluster(),
                filter.getTraitSet(),
                childrenChanged ? adaptedChildren.getFirst() : filter.getInput(),
                adaptedCondition,
                filter.getViableBackends()
            );
        }
        return filter;
    }

    private static RelNode adaptProject(
        OpenSearchProject project,
        Map<ScalarFunction, ScalarFunctionAdapter> adapters,
        List<RelNode> adaptedChildren,
        boolean childrenChanged
    ) {
        // RexInputRef in project expressions references the input's row type
        OpenSearchRelNode inputNode = (OpenSearchRelNode) RelNodeUtils.unwrapHep(project.getInput());
        List<FieldStorageInfo> fieldStorage = inputNode.getOutputFieldStorage();
        List<RexNode> adaptedProjects = new ArrayList<>(project.getProjects().size());
        boolean projectsChanged = false;
        for (RexNode projectExpr : project.getProjects()) {
            RexNode adapted = adaptRex(projectExpr, adapters, fieldStorage, project.getCluster());
            adaptedProjects.add(adapted);
            if (adapted != projectExpr) projectsChanged = true;
        }

        // If the child's row type shifted (e.g. FINAL aggregate's rewriter produced SUM of NOT-NULL
        // column → nullable BIGINT), the project's RexInputRefs still carry the old types. Rebind
        // them against the new input row type and CAST each projection back to the project's
        // declared column type so the outer-visible schema is preserved.
        RelNode newInput = childrenChanged ? adaptedChildren.getFirst() : project.getInput();
        if (childrenChanged && !newInput.getRowType().equals(project.getInput().getRowType())) {
            adaptedProjects = rebindProjectsAgainstInput(adaptedProjects, project, newInput);
            projectsChanged = true;
        }

        if (projectsChanged || childrenChanged) {
            return new OpenSearchProject(
                project.getCluster(),
                project.getTraitSet(),
                newInput,
                adaptedProjects,
                project.getRowType(),
                project.getViableBackends()
            );
        }
        return project;
    }

    /**
     * Adapts RexNodes bottom-up: operands are adapted before the call itself.
     *
     * <p>This means a parent adapter receives already-adapted operands. This is safe
     * because adapters only inspect their <b>direct</b> operands via
     * {@code operand instanceof RexInputRef} to resolve field storage. If a child
     * adapter wraps an operand in CAST, the parent sees a {@code RexCall} (not
     * {@code RexInputRef}) and skips adaptation — no double-CAST occurs.
     *
     * <p>This ordering is validated by {@code testNestedAdaptedFunctionsProduceSingleCast}
     * which confirms {@code SIN(ABS($0))} with both adapted produces one CAST at the leaf.
     */
    private static RexNode adaptRex(
        RexNode node,
        Map<ScalarFunction, ScalarFunctionAdapter> adapters,
        List<FieldStorageInfo> fieldStorage,
        RelOptCluster cluster
    ) {
        if (!(node instanceof RexCall call)) {
            return node;
        }

        // Annotation wrappers: adapt the inner expression and re-wrap with same metadata.
        // Plain RexCall.clone() would drop the annotation subclass, breaking later stripping.
        if (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            RexNode adaptedInner = adaptRex(annotation.unwrap(), adapters, fieldStorage, cluster);
            return adaptedInner == annotation.unwrap() ? node : annotation.withAdaptedOriginal(adaptedInner);
        }

        // Recurse into operands first
        List<RexNode> adaptedOperands = new ArrayList<>(call.getOperands().size());
        boolean operandsChanged = false;
        for (RexNode operand : call.getOperands()) {
            RexNode adapted = adaptRex(operand, adapters, fieldStorage, cluster);
            adaptedOperands.add(adapted);
            if (adapted != operand) operandsChanged = true;
        }

        RexCall current = operandsChanged ? call.clone(call.getType(), adaptedOperands) : call;

        // Look up adapter for this function
        ScalarFunction function = resolveFunction(current);
        if (function != null) {
            ScalarFunctionAdapter adapter = adapters.get(function);
            if (adapter != null) {
                return adapter.adapt(current, fieldStorage, cluster);
            }
        }

        return current;
    }

    private static ScalarFunction resolveFunction(RexCall call) {
        return ScalarFunction.fromSqlOperatorWithFallback(call.getOperator());
    }

    /**
     * Rebind a Project's expressions against a new input whose row type has shifted (typically
     * in nullability — e.g. FINAL aggregate's rewriter turned a NOT-NULL count into a nullable
     * BIGINT). RexInputRefs get retyped to the new column types; projections that diverge from
     * the Project's declared column type get wrapped in a CAST so the outer-visible schema is
     * preserved.
     */
    private static ArrayList<RexNode> rebindProjectsAgainstInput(
        List<RexNode> projects,
        OpenSearchProject originalProject,
        RelNode newInput
    ) {
        RexBuilder rexBuilder = originalProject.getCluster().getRexBuilder();
        List<RelDataType> newInputTypes = new ArrayList<>();
        for (RelDataTypeField f : newInput.getRowType().getFieldList()) {
            newInputTypes.add(f.getType());
        }
        RexShuttle rebind = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                RelDataType actual = newInputTypes.get(ref.getIndex());
                if (ref.getType().equals(actual)) return ref;
                return new RexInputRef(ref.getIndex(), actual);
            }
        };
        ArrayList<RexNode> rebound = new ArrayList<>(projects.size());
        for (int i = 0; i < projects.size(); i++) {
            RexNode expr = projects.get(i).accept(rebind);
            RelDataType targetType = originalProject.getRowType().getFieldList().get(i).getType();
            if (!expr.getType().equals(targetType)) {
                expr = rexBuilder.makeCast(targetType, expr);
            }
            rebound.add(expr);
        }
        return rebound;
    }
}
