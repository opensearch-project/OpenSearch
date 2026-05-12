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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ScalarFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Converts {@link Project} → {@link OpenSearchProject}.
 *
 * <p>Validates that the child's backend can evaluate all projection expressions,
 * either natively or via delegation ({@link DelegationType#PROJECT}).
 *
 * <h2>Baseline vs capability-declared scalars</h2>
 * <p>Calcite plan-rewrite rules (e.g. {@code AggregateReduceFunctionsRule},
 * {@code ReduceExpressionsRule}) routinely introduce arithmetic, CAST, CASE, and
 * null-predicate operators while rewriting expressions. These are <em>SQL-execution
 * primitives</em> that every viable backend must support — they are not optional
 * features worth modeling in the capability registry.
 *
 * <p>Treating them as capability-declared creates two bad outcomes: (1) every new
 * backend has to enumerate ~20 operators that are never actually optional, and
 * (2) any Calcite rule that incidentally emits one of them (e.g. a CAST around
 * {@code SUM(x) / COUNT(x)} to match AVG's original return type) would fail plan-time
 * checks with a misleading error, even though the query semantics are unambiguous.
 *
 * <p>{@link #BASELINE_SCALAR_OPS} carves these primitives out of capability-registry
 * enforcement. Operands are still recursed into — a CAST wrapping a non-baseline
 * function still forces the inner function through capability resolution.
 *
 * @opensearch.internal
 */
public class OpenSearchProjectRule extends RelOptRule {

    /**
     * Scalar operators that any viable backend is implicitly assumed to support.
     * These are SQL-execution primitives (arithmetic, type coercion, null handling,
     * logical composition) that arise incidentally during plan rewriting and that no
     * real execution engine lacks. They bypass {@link #resolveScalarViableBackends}
     * and flow through {@link OpenSearchProject} without backend annotation.
     *
     * <p>If a future backend genuinely cannot execute one of these operators (e.g.
     * Lucene rejects a CAST between incompatible types), that becomes a runtime
     * error inside the backend's executor — complementary to plan-time capability
     * enforcement, not a replacement for it.
     *
     * <p>Intentionally conservative: extend only when a specific plan-rewrite rule
     * demonstrably emits a new operator that every backend already supports.
     */
    private static final Set<SqlOperator> BASELINE_SCALAR_OPS = Set.of(
        // Arithmetic
        SqlStdOperatorTable.PLUS,
        SqlStdOperatorTable.MINUS,
        SqlStdOperatorTable.MULTIPLY,
        SqlStdOperatorTable.DIVIDE,
        SqlStdOperatorTable.UNARY_MINUS,
        SqlStdOperatorTable.UNARY_PLUS,
        // Type coercion
        SqlStdOperatorTable.CAST,
        // Null handling
        SqlStdOperatorTable.IS_NULL,
        SqlStdOperatorTable.IS_NOT_NULL,
        SqlStdOperatorTable.COALESCE,
        // Conditional
        SqlStdOperatorTable.CASE
    );

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
            throw new IllegalStateException("Project rule encountered unmarked child [" + child.getClass().getSimpleName() + "]");
        }

        List<String> childViableBackends = openSearchChild.getViableBackends();

        // Note: if JMH benchmarks show this as a hotspot, consider (a) precomputing a
        // SqlKind → viable backends map once per onMatch() call, and (b) returning
        // childViableBackends directly when all candidates pass to avoid allocation.
        List<RexNode> annotatedExprs = new ArrayList<>(project.getProjects().size());
        boolean requiresBackendCapabilityEvaluation = false;
        for (RexNode expr : project.getProjects()) {
            RexNode annotated = annotateExpr(expr, childViableBackends);
            annotatedExprs.add(annotated);
            if (annotated instanceof AnnotatedProjectExpression) {
                requiresBackendCapabilityEvaluation = true;
            }
        }

        // Passthrough projection: no RexCall to evaluate, so any child backend can emit it.
        List<String> viableBackends = requiresBackendCapabilityEvaluation
            ? computeProjectViableBackends(annotatedExprs, childViableBackends)
            : childViableBackends;

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend can execute all project expressions among " + childViableBackends);
        }

        call.transformTo(
            new OpenSearchProject(
                project.getCluster(),
                child.getTraitSet(),
                RelNodeUtils.unwrapHep(project.getInput()),
                annotatedExprs,
                project.getRowType(),
                viableBackends
            )
        );
    }

    private RexNode annotateExpr(RexNode expr, List<String> childViableBackends) {
        if (!(expr instanceof RexCall rexCall)) {
            // TODO: RexInputRef and RexLiteral are left unannotated — they are implicitly handled
            // by whichever backend executes the operator (pass-through for refs, constant for literals).
            // Revisit if delegation requires knowing which backend evaluates each expression
            // independently, or if a backend cannot handle pass-through refs natively.
            return expr;
        }

        // Baseline operators — arithmetic, CAST, null-handling, conditional — are assumed
        // supported by every backend and are not subject to capability-registry enforcement.
        // Recurse into operands so a non-baseline function nested inside (e.g.
        // CAST(regexp_match(col, 'x'))) still flows through capability resolution.
        if (BASELINE_SCALAR_OPS.contains(rexCall.getOperator())) {
            boolean changed = false;
            List<RexNode> newOperands = new ArrayList<>(rexCall.getOperands().size());
            for (RexNode operand : rexCall.getOperands()) {
                RexNode annotated = annotateExpr(operand, childViableBackends);
                newOperands.add(annotated);
                if (annotated != operand) {
                    changed = true;
                }
            }
            return changed ? rexCall.clone(rexCall.getType(), newOperands) : rexCall;
        }

        // Opaque operations — no recursion into operands
        if (rexCall.getOperator() instanceof SqlFunction sqlFunction) {
            String funcName = sqlFunction.getName();
            if (isOpaqueOperation(funcName)) {
                List<String> exprViable = resolveOpaqueViableBackends(funcName, childViableBackends);
                if (exprViable.isEmpty()) {
                    throw new IllegalStateException("No backend can evaluate [" + funcName + "] and no delegation path exists");
                }
                return new AnnotatedProjectExpression(rexCall.getType(), rexCall, exprViable, context.nextAnnotationId());
            }
        }

        // Standard scalar function
        List<String> scalarViable = resolveScalarViableBackends(rexCall, childViableBackends);
        if (scalarViable.isEmpty()) {
            ScalarFunction resolved = ScalarFunction.fromSqlOperatorWithFallback(rexCall.getOperator());
            String label = resolved != null ? resolved.name() : rexCall.getOperator().getName();
            throw new IllegalStateException("No backend supports scalar function [" + label + "] among " + childViableBackends);
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
        return new AnnotatedProjectExpression(target.getType(), target, scalarViable, context.nextAnnotationId());
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
        boolean reachable = childViableBackends.stream()
            .anyMatch(
                candidateName -> viable.contains(candidateName)
                    || (delegationSupporters.contains(candidateName) && viable.stream().anyMatch(delegationAcceptors::contains))
            );
        return reachable ? viable : List.of();
    }

    private List<String> resolveScalarViableBackends(RexCall rexCall, List<String> childViableBackends) {
        ScalarFunction scalarFunc = ScalarFunction.fromSqlOperatorWithFallback(rexCall.getOperator());
        if (scalarFunc == null) {
            return List.of();
        }
        FieldType fieldType = FieldType.fromSqlTypeName(rexCall.getType().getSqlTypeName());
        // Polymorphic UDF fallback: Calcite UDFs with indeterminate return types (SqlTypeName.ANY)
        // — e.g. PPL's ScalarMaxFunction / ScalarMinFunction — do not map to a concrete FieldType
        // directly. When a viability check for such a call lands here, fall back to the first
        // operand's type. The scalar function's backend capabilities are defined over operand
        // types anyway (SCALAR_MAX(double, double, ...) → DOUBLE), so inferring from operands
        // preserves correct backend dispatch while deferring actual type-tightening until the
        // backend's ScalarFunctionAdapter rewrites the call to a typed library operator.
        if (fieldType == null) {
            for (RexNode operand : rexCall.getOperands()) {
                FieldType operandType = FieldType.fromSqlTypeName(operand.getType().getSqlTypeName());
                if (operandType != null) {
                    fieldType = operandType;
                    break;
                }
            }
        }
        if (fieldType == null) {
            return List.of();
        }

        CapabilityRegistry registry = context.getCapabilityRegistry();
        List<String> allCapable = hasFieldRef(rexCall)
            ? registry.scalarBackendsAnyFormat(scalarFunc, fieldType)
            : registry.literalScalarBackends(scalarFunc, fieldType);

        List<String> viable = new ArrayList<>();
        for (String candidateName : childViableBackends) {
            if (allCapable.contains(candidateName)) {
                viable.add(candidateName);
            }
        }
        if (!viable.isEmpty()) {
            return viable;
        }
        List<String> delegationSupporters = registry.delegationSupporters(DelegationType.PROJECT);
        List<String> delegationAcceptors = registry.delegationAcceptors(DelegationType.PROJECT);
        if (childViableBackends.stream().anyMatch(delegationSupporters::contains)) {
            for (String backendName : allCapable) {
                if (delegationAcceptors.contains(backendName)) {
                    viable.add(backendName);
                }
            }
        }
        return viable;
    }

    private boolean hasFieldRef(RexNode node) {
        if (node instanceof RexInputRef) return true;
        if (node instanceof RexCall rexCall) {
            for (RexNode operand : rexCall.getOperands()) {
                if (hasFieldRef(operand)) return true;
            }
        }
        return false;
    }

    private List<String> computeProjectViableBackends(List<RexNode> annotatedExprs, List<String> childViableBackends) {
        // A child viable backend is viable for the project if for every expression it can
        // either evaluate natively (present in expression's viableBackends) or delegate to
        // a backend that can (supports PROJECT delegation to an acceptor in expression's viableBackends)
        CapabilityRegistry registry = context.getCapabilityRegistry();
        List<String> delegationSupporters = registry.delegationSupporters(DelegationType.PROJECT);
        List<String> delegationAcceptors = registry.delegationAcceptors(DelegationType.PROJECT);

        List<String> result = new ArrayList<>();
        List<String> projectCapable = new ArrayList<>(registry.projectCapableBackends());
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
