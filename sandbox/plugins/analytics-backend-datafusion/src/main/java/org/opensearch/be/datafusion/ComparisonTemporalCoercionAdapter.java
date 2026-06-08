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
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Coerces a bare character operand of a binary comparison to {@code TIMESTAMP} when the other
 * operand is temporal ({@code DATE} / {@code TIME} / {@code TIMESTAMP}), so the comparison resolves
 * to a Substrait signature DataFusion can execute.
 *
 * <p>Needed because a temporal comparison inside a subquery loses its coercion: decorrelation
 * constant-folds the PPL {@code TIMESTAMP} UDF wrapper to a bare string before the scalar-function
 * adapter pass runs, leaving an unconvertible {@code timestamp vs string} comparison. Recovering it
 * here is independent of how the string arrived (folded UDF, native {@code DATE '...'} literal, or a
 * string column); the cast reuses {@link DatePartAdapters#coerceCharacterOperandToTimestamp}. Other
 * shapes (e.g. {@code gender = 'F'}, numeric ranges) pass through unchanged.
 *
 * @opensearch.internal
 */
class ComparisonTemporalCoercionAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.size() != 2) {
            return original;
        }
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);
        boolean leftChar = isCharacter(left);
        boolean rightChar = isCharacter(right);
        boolean leftTemporal = isTemporal(left);
        boolean rightTemporal = isTemporal(right);

        // Only act on a temporal-vs-character mix: coerce the character side to TIMESTAMP. Any other
        // shape (temporal/temporal, char/char, numeric, etc.) is left to isthmus unchanged.
        RexNode newLeft = left;
        RexNode newRight = right;
        if (leftChar && rightTemporal) {
            newLeft = DatePartAdapters.coerceCharacterOperandToTimestamp(left, cluster);
        } else if (rightChar && leftTemporal) {
            newRight = DatePartAdapters.coerceCharacterOperandToTimestamp(right, cluster);
        } else {
            return original;
        }

        if (newLeft == left && newRight == right) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> coerced = new ArrayList<>(2);
        coerced.add(newLeft);
        coerced.add(newRight);
        return rexBuilder.makeCall(original.getType(), original.getOperator(), coerced);
    }

    private static boolean isCharacter(RexNode node) {
        return SqlTypeFamily.CHARACTER.contains(node.getType());
    }

    private static boolean isTemporal(RexNode node) {
        RelDataType type = node.getType();
        SqlTypeName name = type.getSqlTypeName();
        return name == SqlTypeName.TIMESTAMP
            || name == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            || name == SqlTypeName.DATE
            || name == SqlTypeName.TIME
            || name == SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
    }
}
