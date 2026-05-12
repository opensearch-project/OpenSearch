/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Shared helper for VARCHAR→TIMESTAMP operand coercion on datetime scalar calls.
 * Used by {@link DatetimeOperandCoerceShuttle} (pre-planning) and by
 * {@code BackendPlanAdapter} (post-adapter, covers calls rewritten to internal
 * Substrait names like {@code date_part} / {@code to_timestamp}).
 *
 * <p>Scope is intentionally limited to VARCHAR literals — non-literal VARCHAR
 * columns would require per-row CAST and are not in scope for current failing
 * tests. Extend to all VARCHAR if a future failure mode requires it.
 *
 * @opensearch.internal
 */
public final class DatetimeOperandCoercer {

    // Function-name (lowercase) → operand slots to coerce when VARCHAR.
    private static final Map<String, int[]> TIMESTAMP_OPERAND_SLOTS = Map.of(
        "convert_tz", new int[] { 0 },
        "to_timestamp", new int[] { 0 },
        "date_part", new int[] { 1 }
    );

    private DatetimeOperandCoercer() {}

    /**
     * Returns a call with VARCHAR-literal operands at timestamp-typed slots
     * wrapped in {@code CAST(... AS TIMESTAMP)}, or the input call unchanged
     * if no coercion applies.
     */
    public static RexNode coerce(RexCall call, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
        int[] slots = TIMESTAMP_OPERAND_SLOTS.get(call.getOperator().getName().toLowerCase());
        if (slots == null) {
            return call;
        }
        List<RexNode> operands = call.getOperands();
        List<RexNode> rewritten = null;
        for (int slot : slots) {
            if (slot >= operands.size()) continue;
            RexNode operand = operands.get(slot);
            if (!(operand instanceof RexLiteral) || !isVarchar(operand)) continue;
            if (rewritten == null) {
                rewritten = new ArrayList<>(operands);
            }
            rewritten.set(slot, castToTimestamp(operand, rexBuilder, typeFactory));
        }
        return rewritten == null ? call : call.clone(call.getType(), rewritten);
    }

    static boolean isVarchar(RexNode node) {
        SqlTypeName typeName = node.getType().getSqlTypeName();
        return typeName == SqlTypeName.VARCHAR || typeName == SqlTypeName.CHAR;
    }

    static boolean isTimestamp(RexNode node) {
        SqlTypeName typeName = node.getType().getSqlTypeName();
        return typeName == SqlTypeName.TIMESTAMP || typeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    static RexNode castToTimestamp(RexNode operand, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
        RelDataType baseType = typeFactory.createSqlType(
            SqlTypeName.TIMESTAMP,
            typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIMESTAMP)
        );
        RelDataType targetType = typeFactory.createTypeWithNullability(baseType, operand.getType().isNullable());
        return rexBuilder.makeCast(targetType, operand);
    }
}
