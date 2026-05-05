/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapts {@code ||(a, b, ...)} (Calcite {@code SqlStdOperatorTable.CONCAT}) into a
 * null-propagating form for the DataFusion backend.
 *
 * <p>Calcite's {@code ||} operator follows the SQL standard: if any operand is NULL, the result
 * is NULL. Substrait's default {@code concat} extension is documented with the same semantics,
 * but DataFusion's substrait reader maps it to the DataFusion {@code concat()} function — which
 * deviates from the standard and treats NULL operands as empty strings. To preserve Calcite's
 * semantics on the analytics-engine path, this adapter rewrites
 *
 * <pre>{@code
 *   ||(a, b)
 *     →
 *   CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE ||(a, b) END
 * }</pre>
 *
 * The inner {@code ||} is left intact and serializes through the same Substrait conversion path,
 * but with the surrounding CASE/IS_NULL the DataFusion {@code concat()} call is short-circuited
 * for any input that contains a NULL — restoring SQL-standard null-propagation without requiring
 * a custom DataFusion UDF.
 *
 * <p>Single-operand calls fall through unchanged (the result equals the operand, so no
 * null-handling rewrite is needed).
 */
class ConcatFunctionAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.size() < 2) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> nullChecks = new ArrayList<>(operands.size());
        for (RexNode operand : operands) {
            // RexLiteral.isAlwaysTrue is false for non-NULL literals, but the dedicated
            // RexBuilder.makeNullLiteral path is unnecessary — IS_NULL on a non-null literal
            // becomes a constant-false predicate, so OR(constant_false, ...) reduces cleanly.
            nullChecks.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, operand));
        }
        RexNode anyNull = nullChecks.get(0);
        for (int i = 1; i < nullChecks.size(); i++) {
            anyNull = rexBuilder.makeCall(SqlStdOperatorTable.OR, anyNull, nullChecks.get(i));
        }
        // Result type stays the same as the original CONCAT — nullable VARCHAR.
        RexNode nullLiteral = rexBuilder.makeNullLiteral(original.getType());
        return rexBuilder.makeCall(original.getType(), SqlStdOperatorTable.CASE, List.of(anyNull, nullLiteral, original));
    }
}
