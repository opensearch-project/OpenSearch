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
 * Lowerings for PPL {@code PERIOD_ADD} / {@code PERIOD_DIFF}, which operate on MySQL "period"
 * integers in {@code YYYYMM} form. Both reduce to pure integer arithmetic — no temporal primitives
 * — by converting a period to an absolute month index {@code monthIndex(P) = (P / 100) * 12 + (P % 100)}.
 *
 * <p>The PPL frontend's 2-digit-year shorthand ({@code YYMM → 20YY}) is not reproduced here: PPL
 * emits these calls with full {@code YYYYMM} integers, and the absolute-month arithmetic below is
 * exact for that form.
 *
 * @opensearch.internal
 */
final class PeriodArithmeticAdapters {

    private PeriodArithmeticAdapters() {}

    private static RexNode intLit(RexBuilder rexBuilder, RelDataType intType, long value) {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value), intType);
    }

    /** {@code (P / 100) * 12 + (P % 100)} — the absolute month index of a YYYYMM period. */
    private static RexNode monthIndex(RexBuilder rexBuilder, RelDataType intType, RexNode period) {
        RexNode years = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, period, intLit(rexBuilder, intType, 100));
        RexNode yearMonths = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, years, intLit(rexBuilder, intType, 12));
        RexNode month = rexBuilder.makeCall(SqlStdOperatorTable.MOD, period, intLit(rexBuilder, intType, 100));
        return rexBuilder.makeCall(SqlStdOperatorTable.PLUS, yearMonths, month);
    }

    private static RexNode pinReturnType(RexBuilder rexBuilder, RexNode expr, RexCall original) {
        if (expr.getType().equals(original.getType())) {
            return expr;
        }
        return rexBuilder.makeCast(original.getType(), expr, true);
    }

    /**
     * PPL {@code PERIOD_ADD(P, n)} — add {@code n} months to period {@code P}, returning YYYYMM.
     * With {@code m = monthIndex(P) - 1 + n} (shift to 0-based month), the result is
     * {@code (m / 12) * 100 + (m % 12) + 1}.
     */
    static final class PeriodAddAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().size() != 2) {
                return original;
            }
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RelDataType intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
            RexNode period = rexBuilder.makeCast(intType, original.getOperands().get(0), true);
            RexNode months = rexBuilder.makeCast(intType, original.getOperands().get(1), true);
            // m = monthIndex(P) - 1 + n (0-based absolute month)
            RexNode base = rexBuilder.makeCall(
                SqlStdOperatorTable.MINUS,
                monthIndex(rexBuilder, intType, period),
                intLit(rexBuilder, intType, 1)
            );
            RexNode m = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, base, months);
            // (m / 12) * 100 + (m % 12) + 1
            RexNode years = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, m, intLit(rexBuilder, intType, 12));
            RexNode yearPart = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, years, intLit(rexBuilder, intType, 100));
            RexNode monthPart = rexBuilder.makeCall(SqlStdOperatorTable.MOD, m, intLit(rexBuilder, intType, 12));
            RexNode yyyymm = rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                rexBuilder.makeCall(SqlStdOperatorTable.PLUS, yearPart, monthPart),
                intLit(rexBuilder, intType, 1)
            );
            return pinReturnType(rexBuilder, yyyymm, original);
        }
    }

    /**
     * PPL {@code PERIOD_DIFF(P1, P2)} — months between two YYYYMM periods ({@code P1 - P2}):
     * {@code monthIndex(P1) - monthIndex(P2)}.
     */
    static final class PeriodDiffAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().size() != 2) {
                return original;
            }
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RelDataType intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
            RexNode p1 = rexBuilder.makeCast(intType, original.getOperands().get(0), true);
            RexNode p2 = rexBuilder.makeCast(intType, original.getOperands().get(1), true);
            RexNode diff = rexBuilder.makeCall(
                SqlStdOperatorTable.MINUS,
                monthIndex(rexBuilder, intType, p1),
                monthIndex(rexBuilder, intType, p2)
            );
            return pinReturnType(rexBuilder, diff, original);
        }
    }
}
