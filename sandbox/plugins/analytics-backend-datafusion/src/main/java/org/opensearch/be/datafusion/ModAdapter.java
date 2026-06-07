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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Adapter for PPL {@code MOD}.
 *
 * <p>Widens mixed-type operands (e.g. {@code MOD(fp32?, i32)} from
 * {@code float_number % 2}) to a common numeric type, then emits the standard
 * {@link SqlStdOperatorTable#MOD}. Substrait's stock {@code modulus} extension declares
 * integer-only signatures; the fp signatures are supplied by
 * {@code opensearch_arithmetic_overloads.yaml} so isthmus emits the call under the standard
 * substrait name and DataFusion's substrait consumer routes it to its native fp
 * {@code %} operator (precision-correct IEEE 754 remainder).
 *
 * <p>For all operands the result is wrapped in {@code CASE WHEN y = 0 THEN NULL ELSE … END}
 * — PPL semantics return NULL on divide-by-zero, but isthmus's Calcite→Substrait emission
 * can't thread substrait's {@code on_domain_error: NULL} option through.
 *
 * @opensearch.internal
 */
class ModAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();

        RexNode lhs = original.getOperands().get(0);
        RexNode rhs = original.getOperands().get(1);
        SqlTypeName lhsType = lhs.getType().getSqlTypeName();
        SqlTypeName rhsType = rhs.getType().getSqlTypeName();

        int lhsRank = numericRank(lhsType);
        int rhsRank = numericRank(rhsType);

        if (lhsRank < 0 || rhsRank < 0) {
            return wrapWithNullOnZero(
                rexBuilder.makeCall(original.getType(), SqlStdOperatorTable.MOD, List.of(lhs, rhs)),
                rhs,
                rexBuilder,
                typeFactory
            );
        }

        SqlTypeName common = lhsRank >= rhsRank ? lhsType : rhsType;
        RexNode newLhs = lhsType == common ? lhs : cast(lhs, common, typeFactory, rexBuilder);
        RexNode newRhs = rhsType == common ? rhs : cast(rhs, common, typeFactory, rexBuilder);
        RexNode modExpr = rexBuilder.makeCall(original.getType(), SqlStdOperatorTable.MOD, List.of(newLhs, newRhs));
        return wrapWithNullOnZero(modExpr, newRhs, rexBuilder, typeFactory);
    }

    private static RexNode wrapWithNullOnZero(RexNode expr, RexNode divisor, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
        RelDataType nullableExprType = typeFactory.createTypeWithNullability(expr.getType(), true);
        RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO, divisor.getType());
        RexNode isZero = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, List.of(divisor, zero));
        RexNode nullLit = rexBuilder.makeNullLiteral(nullableExprType);
        RexNode coercedExpr = expr.getType().equals(nullableExprType) ? expr : rexBuilder.makeCast(nullableExprType, expr);
        return rexBuilder.makeCall(nullableExprType, SqlStdOperatorTable.CASE, List.of(isZero, nullLit, coercedExpr));
    }

    private static RexNode cast(RexNode operand, SqlTypeName target, RelDataTypeFactory factory, RexBuilder rexBuilder) {
        RelDataType targetType = factory.createTypeWithNullability(factory.createSqlType(target), operand.getType().isNullable());
        return rexBuilder.makeCast(targetType, operand);
    }

    private static int numericRank(SqlTypeName type) {
        return switch (type) {
            case TINYINT -> 1;
            case SMALLINT -> 2;
            case INTEGER -> 3;
            case BIGINT -> 4;
            case FLOAT, REAL -> 5;
            case DOUBLE -> 6;
            default -> -1;
        };
    }
}
