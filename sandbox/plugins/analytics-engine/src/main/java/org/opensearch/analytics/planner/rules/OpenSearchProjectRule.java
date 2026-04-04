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
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.OperatorCapability;
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

        List<String> childViableBackends = openSearchChild.getViableBackends();

        // TODO: precompute SqlKind → viable backends map to avoid repeated filtering per node
        // TODO: reuse childViableBackends list when all candidates pass instead of allocating
        List<RexNode> annotatedExprs = new ArrayList<>(project.getProjects().size());
        for (RexNode expr : project.getProjects()) {
            annotatedExprs.add(annotateExpr(expr, childViableBackends));
        }

        List<String> viableBackends = computeProjectViableBackends(annotatedExprs, childViableBackends);
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException(
                "No backend can execute all project expressions among " + childViableBackends);
        }

        call.transformTo(new OpenSearchProject(
            project.getCluster(),
            child.getTraitSet(),
            RelNodeUtils.unwrapHep(project.getInput()),
            annotatedExprs,
            project.getRowType(),
            viableBackends
        ));
    }

    private RexNode annotateExpr(RexNode expr, List<String> childViableBackends) {
        if (!(expr instanceof RexCall rexCall)) {
            return expr;
        }

        // Opaque operations — no recursion into operands
        if (rexCall.getOperator() instanceof SqlFunction sqlFunction) {
            String funcName = sqlFunction.getName();
            if (isOpaqueOperation(funcName)) {
                List<String> exprViable = resolveOpaqueViableBackends(funcName, childViableBackends);
                if (exprViable.isEmpty()) {
                    throw new IllegalStateException(
                        "No backend can evaluate [" + funcName + "] and no delegation path exists");
                }
                return new AnnotatedProjectExpression(rexCall.getType(), rexCall, exprViable,
                    context.nextAnnotationId());
            }
        }

        // Standard scalar function
        List<String> scalarViable = resolveScalarViableBackends(rexCall, childViableBackends);
        if (scalarViable.isEmpty()) {
            throw new IllegalStateException(
                "No backend supports scalar function [" + ScalarFunction.fromSqlKind(rexCall.getKind())
                    + "] among " + childViableBackends);
        }

        // Recurse into operands
        boolean changed = false;
        List<RexNode> newOperands = new ArrayList<>(rexCall.getOperands().size());
        for (RexNode operand : rexCall.getOperands()) {
            RexNode annotated = annotateExpr(operand, childViableBackends);
            newOperands.add(annotated);
            if (annotated != operand) {
                changed = true;
            }
        }

        RexCall target = changed ? rexCall.clone(rexCall.getType(), newOperands) : rexCall;
        return new AnnotatedProjectExpression(target.getType(), target, scalarViable,
            context.nextAnnotationId());
    }

    private List<String> resolveOpaqueViableBackends(String funcName, List<String> childViableBackends) {
        CapabilityRegistry registry = context.getCapabilityRegistry();
        List<String> viable = registry.opaqueBackendsAnyFormat(funcName);
        if (viable.isEmpty()) {
            return viable;
        }
        // At least one child viable backend must be able to reach an evaluator:
        // either it's in viable itself (native), or it can delegate to one that accepts
        List<String> delegationSupporters = registry.delegationSupporters(DelegationType.PROJECT);
        List<String> delegationAcceptors = registry.delegationAcceptors(DelegationType.PROJECT);
        boolean reachable = childViableBackends.stream().anyMatch(candidateName ->
            viable.contains(candidateName)
                || (delegationSupporters.contains(candidateName)
                    && viable.stream().anyMatch(delegationAcceptors::contains)));
        return reachable ? viable : List.of();
    }

    private List<String> resolveScalarViableBackends(RexCall rexCall, List<String> childViableBackends) {
        ScalarFunction scalarFunc = ScalarFunction.fromSqlKind(rexCall.getKind());
        if (scalarFunc == null) {
            return List.of();
        }
        FieldType fieldType = FieldType.fromSqlTypeName(rexCall.getType().getSqlTypeName());
        if (fieldType == null) {
            return List.of();
        }

        CapabilityRegistry registry = context.getCapabilityRegistry();
        List<String> allCapable = registry.scalarBackendsAnyFormat(scalarFunc, fieldType);

        // Prefer child viable backends
        List<String> viable = new ArrayList<>();
        for (String candidateName : childViableBackends) {
            if (allCapable.contains(candidateName)) {
                viable.add(candidateName);
            }
        }
        if (!viable.isEmpty()) {
            return viable;
        }
        // Fallback: other backends if reachable via delegation
        List<String> delegationSupporters = registry.delegationSupporters(DelegationType.PROJECT);
        List<String> delegationAcceptors = registry.delegationAcceptors(DelegationType.PROJECT);
        boolean canDelegate = childViableBackends.stream().anyMatch(delegationSupporters::contains);
        if (!canDelegate) {
            return viable;
        }
        for (String backendName : allCapable) {
            if (delegationAcceptors.contains(backendName)) {
                viable.add(backendName);
            }
        }
        return viable;
    }

    private List<String> computeProjectViableBackends(List<RexNode> annotatedExprs,
                                                      List<String> childViableBackends) {
        // A child viable backend is viable for the project if for every expression it can
        // either evaluate natively (present in expression's viableBackends) or delegate to
        // a backend that can (supports PROJECT delegation to an acceptor in expression's viableBackends)
        CapabilityRegistry registry = context.getCapabilityRegistry();
        List<String> delegationSupporters = registry.delegationSupporters(DelegationType.PROJECT);
        List<String> delegationAcceptors = registry.delegationAcceptors(DelegationType.PROJECT);

        List<String> result = new ArrayList<>();
        List<String> projectCapable = registry.operatorBackends(OperatorCapability.PROJECT);
        for (String candidateName : childViableBackends) {
            if (!projectCapable.contains(candidateName)) {
                continue;
            }
            boolean canHandleAll = true;
            for (RexNode expr : annotatedExprs) {
                if (!(expr instanceof AnnotatedProjectExpression annotation)) {
                    continue;
                }
                if (annotation.getViableBackends().contains(candidateName)) {
                    continue;
                }
                boolean canDelegate = delegationSupporters.contains(candidateName)
                    && annotation.getViableBackends().stream().anyMatch(delegationAcceptors::contains);
                if (!canDelegate) {
                    canHandleAll = false;
                    break;
                }
            }
            if (canHandleAll) {
                result.add(candidateName);
            }
        }
        return result;
    }

    private boolean isOpaqueOperation(String funcName) {
        return context.getCapabilityRegistry().isOpaqueOperation(funcName);
    }
}
