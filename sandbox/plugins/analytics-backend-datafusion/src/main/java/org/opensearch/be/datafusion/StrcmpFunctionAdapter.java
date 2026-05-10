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
 * Adapts PPL {@code strcmp(a, b)} into a pure Substrait/DataFusion CASE expression.
 * <ul>
 *   <li>{@code -1} when {@code a < b}</li>
 *   <li>{@code  0} when {@code a = b}</li>
 *   <li>{@code  1} when {@code a > b}</li>
 *   <li>{@code NULL} when either operand is {@code NULL}</li>
 * </ul>
 *
 * <p>Rewrite:
 * <pre>{@code
 *   strcmp(a, b)
 *     →
 *   CASE
 *     WHEN a IS NULL OR b IS NULL THEN NULL
 *     WHEN a < b THEN -1
 *     WHEN a = b THEN  0
 *     ELSE              1
 *   END
 * }</pre>
 *
 * <p>Why the adapter beats a row-by-row Rust UDF: the {@code <} and {@code =}
 * comparisons between {@code StringArray} operands lower to arrow-rs compute
 * kernels ({@code arrow::compute::lt}, {@code arrow::compute::eq}) which are
 * SIMD-vectorized on x86_64 (AVX2) and arm64 (NEON). The CASE ({@code ifelse})
 * is also an arrow vectorized kernel. A UDF that loops
 * {@code for i in 0..n { str::cmp(...) }} per row is strictly slower — it
 * amortizes FFI over the batch but the inner compare is scalar.
 *
 * <p>PPL's frontend reverses {@code strcmp}'s args vs. user order. This adapter
 * swaps them back — operands are consumed as {@code (arg1, arg0)} from the
 * original call so the resulting {@code a < b} / {@code a = b} maps 1:1 to the
 * user-intended {@code -1 / 0 / 1} convention.
 *
 * @opensearch.internal
 */
class StrcmpFunctionAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.size() != 2) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // Swap to undo the PPL frontend's argument reversal.
        RexNode a = operands.get(1);
        RexNode b = operands.get(0);

        RelDataType intType = cluster.getTypeFactory()
            .createTypeWithNullability(cluster.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
        RexNode neg1 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(-1), intType);
        RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO, intType);
        RexNode one = rexBuilder.makeExactLiteral(BigDecimal.ONE, intType);
        RexNode nullLit = rexBuilder.makeNullLiteral(intType);

        // NULL propagation must be explicit — SQL comparators on NULL return NULL, but
        // the CASE below needs to short-circuit them so we don't fall through to the
        // `ELSE 1` branch when either operand is NULL.
        RexNode aIsNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, a);
        RexNode bIsNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, b);
        RexNode anyNull = rexBuilder.makeCall(SqlStdOperatorTable.OR, aIsNull, bIsNull);

        RexNode lessThan = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, a, b);
        RexNode equalTo = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, a, b);

        // CASE WHEN anyNull THEN NULL WHEN a<b THEN -1 WHEN a=b THEN 0 ELSE 1 END
        return rexBuilder.makeCall(intType, SqlStdOperatorTable.CASE, List.of(anyNull, nullLit, lessThan, neg1, equalTo, zero, one));
    }
}
