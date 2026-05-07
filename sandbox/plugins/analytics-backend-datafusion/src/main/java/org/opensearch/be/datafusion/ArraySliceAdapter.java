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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapter for Calcite's {@link SqlLibraryOperators#ARRAY_SLICE}. Two transforms
 * are needed before substrait emission:
 *
 * <ol>
 *   <li><b>Index coercion to {@code BIGINT}.</b> PPL's parser types positive
 *       integer literals as {@code DECIMAL(20,0)} (precision wide enough to
 *       hold any 64-bit unsigned value), but DataFusion's {@code array_slice}
 *       signature accepts only integer indexes and refuses to coerce decimal
 *       arguments — failing with {@code "No function matches the given name
 *       and argument types 'array_slice(List(Int32), Decimal128(20, 0),
 *       Decimal128(22, 0))'"}.
 *   <li><b>Semantic conversion: 0-based {@code (start, length)} →
 *       1-based {@code (start, end)} inclusive.</b> Calcite's
 *       {@link SqlLibraryOperators#ARRAY_SLICE} (used by PPL's
 *       {@code MVIndexFunctionImp.resolveRange}) is the Spark / Hive flavor
 *       with 0-based start and a length-of-elements third arg. DataFusion's
 *       native {@code array_slice} is 1-based with an inclusive end-index
 *       third arg. Without this conversion, {@code mvindex(arr=[1..5], 1, 3)}
 *       would emit {@code ARRAY_SLICE(arr, 1, 3)} → DataFusion returns
 *       {@code [1, 2, 3]}, but the PPL expectation is {@code [2, 3, 4]}
 *       (0-based positions 1..3 inclusive).
 *       <p>The conversion is purely arithmetic on the operands:
 *       <ul>
 *         <li>{@code start' = start + 1}
 *         <li>{@code end'   = start + length} (which is {@code start + 1 +
 *             (length - 1)} = the 1-based inclusive end)
 *       </ul>
 *       Negative indexes have already been normalized to non-negative
 *       0-based positions by {@code MVIndexFunctionImp} before this adapter
 *       runs (it uses {@code arrayLen + idx} for both start and end), so the
 *       arithmetic above applies uniformly.
 * </ol>
 *
 * <p>The 2-arg form {@code ARRAY_SLICE(arr, start)} (single-element extract)
 * is not produced by PPL's {@code MVIndexFunctionImp} (single-element access
 * lowers through {@code INTERNAL_ITEM} instead), so this adapter handles
 * only the 3-arg form.
 *
 * @opensearch.internal
 */
class ArraySliceAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        List<RexNode> operands = original.getOperands();
        if (operands.size() != 3) {
            // Defensive: unexpected arity. Fall through with BIGINT coercion only — the substrait
            // converter will surface a missing-signature error with a clear message.
            return rexBuilder.makeCall(original.getType(), original.getOperator(), coerceIndexes(rexBuilder, typeFactory, bigint, operands));
        }
        List<RexNode> coerced = coerceIndexes(rexBuilder, typeFactory, bigint, operands);
        RexNode array = coerced.get(0);
        RexNode start = coerced.get(1);
        RexNode length = coerced.get(2);
        RexNode one = rexBuilder.makeExactLiteral(BigDecimal.ONE, bigint);
        RexNode oneBasedStart = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, start, one);
        RexNode endInclusive = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, start, length);
        return rexBuilder.makeCall(original.getType(), original.getOperator(), List.of(array, oneBasedStart, endInclusive));
    }

    private static List<RexNode> coerceIndexes(RexBuilder rexBuilder, RelDataTypeFactory typeFactory, RelDataType bigint, List<RexNode> operands) {
        List<RexNode> coerced = new ArrayList<>(operands.size());
        for (int i = 0; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            if (i == 0 || operand.getType().getSqlTypeName() == SqlTypeName.BIGINT) {
                coerced.add(operand);
            } else {
                RelDataType nullableBigint = typeFactory.createTypeWithNullability(bigint, operand.getType().isNullable());
                coerced.add(rexBuilder.makeCast(nullableBigint, operand, true, false));
            }
        }
        return coerced;
    }
}
