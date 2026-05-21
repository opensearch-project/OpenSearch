/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Pre-isthmus pass that rewrites PPL window-based BRAIN expressions for the
 * analytics-engine route.
 *
 * <p>Two coordinated transformations:
 *
 * <ol>
 *   <li><b>RexOver substitution.</b> Every {@link RexOver} whose operator is
 *       PPL's {@code INTERNAL_PATTERN} (operator name {@code "pattern"}) is
 *       replaced by a {@code RexOver} targeting
 *       {@link DataFusionFragmentConvertor#LOCAL_INTERNAL_PATTERN_WINDOW_OP}
 *       with a {@code VARCHAR} return type. The Rust window UDF in
 *       {@code udwf/internal_pattern.rs} returns the matched wildcard pattern
 *       string per row, so the substrait plan sees only concrete types.</li>
 *   <li><b>RexInputRef type cascade.</b> Calcite's
 *       {@code OpenSearchProject#liftNestedRexOver} hoists every
 *       {@code RexOver} into its own child {@link Project} before fragment
 *       conversion. The parent Project's {@code PATTERN_PARSER} call (and any
 *       further downstream consumer) captures a {@link RexInputRef} pointing
 *       at the hoisted column, with the {@code RexInputRef}'s declared type
 *       frozen to the original {@code MAP<VARCHAR, ANY> ARRAY}. After we
 *       substitute the RexOver's return type to VARCHAR the inner Project's
 *       row type updates, but Calcite still validates downstream operators by
 *       comparing the inputs' current row types against each
 *       {@code RexInputRef}'s captured type — mismatch
 *       ({@code "type mismatch: ref: MAP NOT NULL ARRAY, input: VARCHAR"})
 *       at the next Aggregate/Sort. To resolve, we walk every RelNode bottom-up
 *       and retype each {@link RexInputRef} so its declared type matches the
 *       current input row type. Type changes propagate upward through wrapping
 *       {@link RexCall}s via re-inference, so the chain converges naturally.</li>
 * </ol>
 *
 * <p>Mirrors {@link PplAggregateCallRewriter} for the aggregate side — same
 * intent (route PPL's INTERNAL_PATTERN through a local stub that isthmus binds
 * by operator identity), more plumbing because the RexOver is structurally
 * nested in a multi-Project chain.
 *
 * @opensearch.internal
 */
final class PplWindowCallRewriter {

    private PplWindowCallRewriter() {}

    static RelNode rewrite(RelNode root) {
        return descendAndRewrite(root);
    }

    /**
     * Custom shuttle that visits children first, then retypes the current
     * RelNode's RexNodes against the (potentially rewritten) children's row
     * types before reconstructing. Avoids {@code RelShuttleImpl.visitChildren}'s
     * eager {@code copy()} which would invoke {@code Project.isValid}
     * validation before any retyping could occur.
     */
    private static RelNode descendAndRewrite(RelNode rel) {
        List<RelNode> oldInputs = rel.getInputs();
        if (oldInputs.isEmpty()) {
            return rel;
        }
        List<RelNode> newInputs = new ArrayList<>(oldInputs.size());
        boolean childChanged = false;
        for (RelNode input : oldInputs) {
            RelNode rewritten = descendAndRewrite(input);
            if (rewritten != input) {
                childChanged = true;
            }
            newInputs.add(rewritten);
        }
        // If a child's row type changed, retype this rel's expressions against
        // the new first input's row type. Single-input cover: this BRAIN-label
        // code path only flows through Project/Filter/Sort/LogicalSystemLimit/
        // Aggregate — none of which span multi-input row types in the affected
        // expressions. Join/Correlate are intentionally out of scope here.
        boolean inputRowTypeChanged = false;
        if (childChanged) {
            for (int i = 0; i < oldInputs.size(); i++) {
                if (!oldInputs.get(i).getRowType().equals(newInputs.get(i).getRowType())) {
                    inputRowTypeChanged = true;
                    break;
                }
            }
        }

        if (rel instanceof Project project) {
            // Pass {@code childChanged} (not just row-type change): even when a
            // descendant rebuild preserves the row type, we must rebuild every
            // ancestor so the new child is woven into the returned tree.
            // Otherwise we'd return the original ancestor referencing stale
            // children, discarding the lower rebuilds.
            RelNode result = rewriteProject(project, newInputs.get(0), childChanged);
            return result;
        }

        if (inputRowTypeChanged) {
            // Retype this rel's expressions against the new (first) input's row
            // type, then ask the rel to copy() itself with the rewritten
            // expressions baked in. We can't use RelNode.copy(traits, inputs)
            // directly because it preserves the original expressions; instead,
            // accept a RexShuttle to swap RexNodes, then call copy on the
            // result with the new inputs.
            RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            ExprRewriter shuttle = new ExprRewriter(rexBuilder, newInputs.get(0).getRowType());
            RelNode retyped = rel.accept(shuttle);
            if (retyped == null) {
                retyped = rel;
            }
            return retyped.copy(retyped.getTraitSet(), newInputs);
        } else if (childChanged) {
            return rel.copy(rel.getTraitSet(), newInputs);
        }
        return rel;
    }

    /**
     * For each project expression, walk top-down:
     *
     * <ul>
     *   <li>Re-type any {@link RexInputRef} whose declared type diverges from
     *       the input's current row type at that column.</li>
     *   <li>Substitute any {@link RexOver} on the PPL {@code "pattern"}
     *       operator to {@link DataFusionFragmentConvertor#LOCAL_INTERNAL_PATTERN_WINDOW_OP}
     *       with a VARCHAR return type.</li>
     *   <li>Rebuild every {@link RexCall} bottom-up via
     *       {@link RexBuilder#makeCall(org.apache.calcite.sql.SqlOperator, java.util.List)}
     *       (re-derives the call's return type from the new operand types) so
     *       cascading type changes propagate cleanly through wrapping operators.
     *       Calls whose declared type matches the re-inferred type are returned
     *       unchanged.</li>
     * </ul>
     *
     * <p>Rebuilds the Project via {@link LogicalProject#create} when anything
     * changes so the project's row type re-derives from the new expressions —
     * downstream parents will then pick up the new column types when this
     * shuttle visits them on the next bottom-up step.
     */
    private static RelNode rewriteProject(Project project, RelNode newInput, boolean inputChanged) {
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        RexShuttle shuttle = new ExprRewriter(rexBuilder, newInput.getRowType());
        List<RexNode> oldExprs = project.getProjects();
        List<RexNode> newExprs = new ArrayList<>(oldExprs.size());
        boolean exprChanged = false;
        for (RexNode expr : oldExprs) {
            RexNode rewritten = expr.accept(shuttle);
            if (rewritten != expr) {
                exprChanged = true;
            }
            newExprs.add(rewritten);
        }
        if (!exprChanged && !inputChanged) {
            return project;
        }
        // Re-derive the row type from the new expressions. The Project's field
        // names stay the same (auto-aliased from input where the expression is
        // a simple input ref; otherwise from the parent Project's row type, which
        // we preserve via the names argument).
        List<String> fieldNames = project.getRowType().getFieldNames();
        return LogicalProject.create(newInput, List.of(), newExprs, fieldNames);
    }

    private static final class ExprRewriter extends RexShuttle {
        private final RexBuilder rexBuilder;
        private final RelDataType inputRowType;

        ExprRewriter(RexBuilder rexBuilder, RelDataType inputRowType) {
            this.rexBuilder = rexBuilder;
            this.inputRowType = inputRowType;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            int idx = inputRef.getIndex();
            if (idx < 0 || idx >= inputRowType.getFieldCount()) {
                return inputRef;
            }
            RelDataType currentType = inputRowType.getFieldList().get(idx).getType();
            if (currentType.equals(inputRef.getType())) {
                return inputRef;
            }
            // The input column's type was updated by an upstream rewrite (e.g. a
            // RexOver substituted from MAP ARRAY → VARCHAR). Retype this ref so
            // downstream validation sees a consistent picture.
            return new RexInputRef(idx, currentType);
        }

        @Override
        public RexNode visitOver(RexOver over) {
            SqlAggFunction op = over.getAggOperator();
            if (op == DataFusionFragmentConvertor.LOCAL_INTERNAL_PATTERN_WINDOW_OP) {
                return over;
            }
            if (!"PATTERN".equalsIgnoreCase(op.getName())) {
                return super.visitOver(over);
            }
            // Recurse into operands first so the operand types are up-to-date.
            RexNode recursed = super.visitOver(over);
            if (!(recursed instanceof RexOver recursedOver)) {
                return recursed;
            }
            RelDataType varcharNullable = rexBuilder.getTypeFactory()
                .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true);
            RexWindow window = recursedOver.getWindow();
            return rexBuilder.makeOver(
                varcharNullable,
                DataFusionFragmentConvertor.LOCAL_INTERNAL_PATTERN_WINDOW_OP,
                recursedOver.getOperands(),
                window.partitionKeys,
                com.google.common.collect.ImmutableList.copyOf(window.orderKeys),
                window.getLowerBound(),
                window.getUpperBound(),
                window.isRows(),
                true,
                false,
                recursedOver.isDistinct(),
                recursedOver.ignoreNulls()
            );
        }

        @Override
        public RexNode visitCall(RexCall call) {
            boolean[] changed = { false };
            List<RexNode> newOperands = visitList(call.getOperands(), changed);
            if (!changed[0]) {
                return call;
            }
            // Preserve the call's declared return type by using the 3-arg
            // makeCall. Re-inferring via the 2-arg overload triggers operator-
            // specific inference (e.g. SqlCastFunction's lambda needs a target-
            // type operand at index 1 that doesn't exist for SAFE_CAST as
            // constructed via {@code RexBuilder.makeCast}). The validator
            // downstream only checks operand types against the input row type,
            // so preserving the call type is safe.
            return rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
        }
    }
}
