/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts {@link Project} → {@link OpenSearchProject}.
 *
 * <p>Validates that the child's backend can evaluate all projection expressions,
 * either natively or via delegation ({@link DelegationType#PROJECT}).
 *
 * @opensearch.internal
 */
public class OpenSearchProjectRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchProjectRule(PlannerContext context) {
        super(operand(Project.class, operand(RelNode.class, any())), "OpenSearchProjectRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        RelNode child = call.rel(1);

        if (project instanceof OpenSearchProject) {
            return;
        }

        if (!(child instanceof OpenSearchRelNode openSearchChild)) {
            throw new IllegalStateException(
                "Project rule encountered unmarked child [" + child.getClass().getSimpleName() + "]");
        }

        String backend = openSearchChild.getBackend();

        List<RexNode> annotatedExprs = new ArrayList<>(project.getProjects().size());
        for (RexNode expr : project.getProjects()) {
            annotatedExprs.add(annotateExpr(expr, backend));
        }

        call.transformTo(new OpenSearchProject(
            project.getCluster(),
            child.getTraitSet(),
            RelNodeUtils.unwrapHep(project.getInput()),
            annotatedExprs,
            project.getRowType(),
            backend
        ));
    }

    private RexNode annotateExpr(RexNode expr, String backend) {
        if (!(expr instanceof RexCall rexCall)) {
            return expr;
        }

        AnalyticsSearchBackendPlugin plugin = context.getBackends().get(backend);
        if (plugin == null) {
            throw new IllegalStateException("Backend [" + backend + "] not found");
        }

        // Opaque operations (painless, highlighting, etc.) — no recursion into operands
        if (rexCall.getOperator() instanceof SqlFunction sqlFunction) {
            String funcName = sqlFunction.getName();
            if (isOpaqueOperation(funcName)) {
                if (plugin.supportedOpaqueProjectOperations().contains(funcName)) {
                    return new AnnotatedProjectExpression(rexCall.getType(), rexCall, backend);
                }
                String delegationTarget = canDelegateProject(plugin, funcName);
                if (delegationTarget != null) {
                    return new AnnotatedProjectExpression(rexCall.getType(), rexCall, delegationTarget);
                }
                throw new IllegalStateException(
                    "Backend [" + backend + "] cannot evaluate [" + funcName
                        + "] and no delegation path exists");
            }
        }

        // Standard scalar function — validate support
        ScalarFunction scalarFunc = ScalarFunction.fromSqlKind(rexCall.getKind());
        if (scalarFunc != null && !plugin.supportedScalarFunctions().contains(scalarFunc)) {
            throw new IllegalStateException(
                "Backend [" + backend + "] does not support scalar function [" + scalarFunc + "]");
        }

        // Recurse into operands to validate and annotate nested expressions
        boolean changed = false;
        List<RexNode> newOperands = new ArrayList<>(rexCall.getOperands().size());
        for (RexNode operand : rexCall.getOperands()) {
            RexNode annotated = annotateExpr(operand, backend);
            newOperands.add(annotated);
            if (annotated != operand) {
                changed = true;
            }
        }

        RexCall target = changed ? rexCall.clone(rexCall.getType(), newOperands) : rexCall;
        return new AnnotatedProjectExpression(target.getType(), target, backend);
    }

    private boolean isOpaqueOperation(String funcName) {
        return context.getBackends().values().stream()
            .anyMatch(b -> b.supportedOpaqueProjectOperations().contains(funcName));
    }

    private String canDelegateProject(AnalyticsSearchBackendPlugin plugin, String funcName) {
        if (!plugin.supportedDelegations().contains(DelegationType.PROJECT)) {
            return null;
        }
        return context.getBackends().values().stream()
            .filter(other -> !other.name().equals(plugin.name())
                && other.acceptedDelegations().contains(DelegationType.PROJECT)
                && other.supportedOpaqueProjectOperations().contains(funcName))
            .map(AnalyticsSearchBackendPlugin::name)
            .findFirst()
            .orElse(null);
    }
}
