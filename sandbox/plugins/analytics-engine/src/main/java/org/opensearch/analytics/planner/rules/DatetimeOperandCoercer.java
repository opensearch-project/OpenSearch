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
import org.apache.calcite.util.TimestampString;

import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
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
        "convert_tz",
        new int[] { 0 },
        "to_timestamp",
        new int[] { 0 },
        "date_part",
        new int[] { 1 }
    );

    private DatetimeOperandCoercer() {}

    /**
     * Returns a call with VARCHAR-literal operands at timestamp-typed slots
     * wrapped in {@code CAST(... AS TIMESTAMP)}, or the input call unchanged
     * if no coercion applies.
     *
     * <p>Recurses into non-datetime wrappers (e.g. {@code CAST}, {@code FLOOR},
     * arithmetic {@code +}) so composite adapters that emit a nested
     * {@code date_part} under an outer CAST (DayOfWeekAdapter, SecondAdapter,
     * MinuteOfDayAdapter) still get their inner VARCHAR literal coerced.
     */
    public static RexNode coerce(RexCall call, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
        List<RexNode> operands = call.getOperands();
        List<RexNode> rewritten = null;
        for (int i = 0; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            if (operand instanceof RexCall innerCall) {
                RexNode coercedInner = coerce(innerCall, rexBuilder, typeFactory);
                if (coercedInner != operand) {
                    if (rewritten == null) rewritten = new ArrayList<>(operands);
                    rewritten.set(i, coercedInner);
                }
            }
        }
        RexCall current = rewritten == null ? call : call.clone(call.getType(), rewritten);

        int[] slots = TIMESTAMP_OPERAND_SLOTS.get(current.getOperator().getName().toLowerCase(Locale.ROOT));
        if (slots == null) {
            return current;
        }
        List<RexNode> currentOperands = current.getOperands();
        List<RexNode> coerced = null;
        for (int slot : slots) {
            if (slot >= currentOperands.size()) continue;
            RexNode operand = currentOperands.get(slot);
            if (!isVarchar(operand)) continue;
            // Accept VARCHAR literals (date strings) and VARCHAR-typed RexCalls
            // (e.g. CAST(time AS VARCHAR) emitted by DatePartAdapters for TIME
            // operands). Column-ref VARCHAR inputs would force per-row CASTs;
            // today's tests don't exercise that path, so we take it as-is.
            if (!(operand instanceof RexLiteral) && !(operand instanceof RexCall)) continue;
            if (coerced == null) {
                coerced = new ArrayList<>(currentOperands);
            }
            coerced.set(slot, castToTimestamp(operand, rexBuilder, typeFactory));
        }
        return coerced == null ? current : current.clone(current.getType(), coerced);
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
        // Time-only VARCHAR literals (e.g. "17:30:00") can't reach DataFusion as
        // CAST(varchar AS TIMESTAMP): the simplifier folds the cast and Arrow's
        // parser rejects strings shorter than 10 chars. Reference PPL treats a
        // bare TIME as LocalDateTime.of(epoch, time), so synthesize a TIMESTAMP
        // literal pinned to 1970-01-01 for that case.
        if (operand instanceof RexLiteral lit && lit.getType().getSqlTypeName() == SqlTypeName.VARCHAR) {
            String value = lit.getValueAs(String.class);
            RexNode tsLit = tryMakeTimeOnlyTimestampLiteral(value, rexBuilder);
            if (tsLit != null) return tsLit;
        }
        RelDataType baseType = typeFactory.createSqlType(
            SqlTypeName.TIMESTAMP,
            typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.TIMESTAMP)
        );
        RelDataType targetType = typeFactory.createTypeWithNullability(baseType, operand.getType().isNullable());
        return rexBuilder.makeCast(targetType, operand);
    }

    private static RexNode tryMakeTimeOnlyTimestampLiteral(String value, RexBuilder rexBuilder) {
        if (value == null || value.length() >= 10) return null;
        try {
            LocalTime time = LocalTime.parse(value);
            TimestampString ts = new TimestampString(1970, 1, 1, time.getHour(), time.getMinute(), time.getSecond());
            if (time.getNano() > 0) {
                ts = ts.withNanos(time.getNano());
            }
            return rexBuilder.makeTimestampLiteral(ts, 9);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }
}
