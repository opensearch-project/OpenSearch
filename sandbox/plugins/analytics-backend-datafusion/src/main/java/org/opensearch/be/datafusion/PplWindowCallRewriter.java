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

/** Pre-isthmus rewrite for PPL BRAIN window expressions onto LOCAL_INTERNAL_PATTERN_WINDOW_OP. */
final class PplWindowCallRewriter {

    private PplWindowCallRewriter() {}

    static RelNode rewrite(RelNode root) {
        return descendAndRewrite(root);
    }

    // Children first, then retype the rel's RexNodes against new child row types before copy().
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
            return rewriteProject(project, newInputs.get(0), childChanged);
        }

        if (inputRowTypeChanged) {
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
            // 3-arg makeCall preserves declared type; 2-arg re-infers and breaks SAFE_CAST.
            return rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
        }
    }
}
