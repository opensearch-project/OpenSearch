/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;
import java.util.Set;

/**
 * Wraps VARCHAR operands in {@code CAST(... AS TIMESTAMP)} for datetime scalar
 * functions whose Substrait signature requires a timestamp operand.
 *
 * <p>Runs as a {@link RelHomogeneousShuttle} so ancestor rowtypes are rebuilt
 * in the same pass. Rewriting operand types under a materialized Project trips
 * {@code Project.isValid} and crashes under {@code -ea}.
 *
 * @opensearch.internal
 */
public final class DatetimeOperandCoerceShuttle extends RelHomogeneousShuttle {

    public static final DatetimeOperandCoerceShuttle INSTANCE = new DatetimeOperandCoerceShuttle();

    private static final Set<SqlOperator> COMPARISON_OPS = Set.of(
        SqlStdOperatorTable.EQUALS,
        SqlStdOperatorTable.NOT_EQUALS,
        SqlStdOperatorTable.GREATER_THAN,
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
        SqlStdOperatorTable.LESS_THAN,
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL
    );

    private DatetimeOperandCoerceShuttle() {}

    @Override
    public RelNode visit(RelNode other) {
        RelNode visited = super.visit(other);
        RexBuilder rexBuilder = visited.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        return visited.accept(new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                RexCall visitedCall = (RexCall) super.visitCall(call);
                RexNode coerced = DatetimeOperandCoercer.coerce(visitedCall, rexBuilder, typeFactory);
                if (coerced != visitedCall) {
                    return coerced;
                }
                if (COMPARISON_OPS.contains(visitedCall.getOperator())) {
                    return coerceComparison(visitedCall, rexBuilder, typeFactory);
                }
                return visitedCall;
            }
        });
    }

    private static RexNode coerceComparison(RexCall call, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
        List<RexNode> operands = call.getOperands();
        if (operands.size() != 2) return call;
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);
        boolean leftVarchar = DatetimeOperandCoercer.isVarchar(left);
        boolean rightVarchar = DatetimeOperandCoercer.isVarchar(right);
        boolean leftTs = DatetimeOperandCoercer.isTimestamp(left);
        boolean rightTs = DatetimeOperandCoercer.isTimestamp(right);
        if (leftVarchar && rightTs) {
            return call.clone(call.getType(), List.of(DatetimeOperandCoercer.castToTimestamp(left, rexBuilder, typeFactory), right));
        }
        if (rightVarchar && leftTs) {
            return call.clone(call.getType(), List.of(left, DatetimeOperandCoercer.castToTimestamp(right, rexBuilder, typeFactory)));
        }
        return call;
    }
}
