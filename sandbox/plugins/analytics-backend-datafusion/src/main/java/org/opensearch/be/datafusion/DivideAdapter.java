/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Adapter for PPL {@code DIVIDE}. Substrait's {@code divide} declares an
 * {@code on_division_by_zero: NULL} option, but isthmus's Calcite→Substrait emission can't
 * thread function options through. DataFusion's runtime therefore raises
 * {@code Arrow error: Divide by zero error} when the divisor is zero, while PPL expects NULL.
 *
 * <p>Wrap each {@code DIVIDE(x, y)} in {@code CASE WHEN y = 0 THEN NULL ELSE DIVIDE(x, y) END}
 * so the null is materialized at adapter time. Calcite's reduce-expressions pass simplifies the
 * CASE away when the divisor is a non-zero literal.
 *
 * @opensearch.internal
 */
class DivideAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();

        RexNode lhs = original.getOperands().get(0);
        RexNode rhs = original.getOperands().get(1);

        RexNode divideExpr = original.getOperator() == SqlStdOperatorTable.DIVIDE
            ? original
            : rexBuilder.makeCall(original.getType(), SqlStdOperatorTable.DIVIDE, List.of(lhs, rhs));

        // Skip the CASE wrap when the divisor is a known non-zero literal. ReduceExpressionsRule
        // runs before adapters and can't fold the CASE introduced here, so emitting one for every
        // `x / literal` would add a runtime branch per row. The short-circuit also keeps these
        // queries on the same substrait shape they had before this adapter existed.
        if (isNonZeroLiteral(rhs)) {
            return divideExpr;
        }

        RelDataType nullableExprType = typeFactory.createTypeWithNullability(divideExpr.getType(), true);
        RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO, rhs.getType());
        RexNode isZero = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(rhs, zero));
        RexNode nullLit = rexBuilder.makeNullLiteral(nullableExprType);
        RexNode coercedExpr = divideExpr.getType().equals(nullableExprType)
            ? divideExpr
            : rexBuilder.makeCast(nullableExprType, divideExpr);
        return rexBuilder.makeCall(nullableExprType, SqlStdOperatorTable.CASE, List.of(isZero, nullLit, coercedExpr));
    }

    private static boolean isNonZeroLiteral(RexNode node) {
        if (!(node instanceof RexLiteral lit) || lit.isNull()) {
            return false;
        }
        BigDecimal value = lit.getValueAs(BigDecimal.class);
        return value != null && value.signum() != 0;
    }
}
