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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrites PPL {@code DATEDIFF(a, b)} into a DataFusion-native expression. The PPL UDF has no
 * Substrait binding, so isthmus rejects it.
 *
 * <p>MySQL/PPL semantics: {@code DATEDIFF} is the whole-day difference between the <em>calendar
 * dates</em> of the two arguments — {@code DATEDIFF('2000-01-02 00:00:00', '2000-01-01 23:59:59')}
 * is {@code 1}, not {@code 0}, because the time-of-day is discarded. Argument order is
 * {@code arg1 - arg2}.
 *
 * <p>Lowering (per operand {@code x}, {@code dayNumber(x) = FLOOR(to_unixtime(x) / 86400)}):
 * <pre>{@code   DATEDIFF(a, b)  →  CAST(dayNumber(a) - dayNumber(b) AS <retType>)}</pre>
 * {@code to_unixtime} yields whole epoch seconds (UTC), and the {@code / 86400} floor-divide maps
 * any instant within a UTC day to that day's epoch-day index — so the floor-divide <em>is</em> the
 * day truncation and the subtraction is an exact calendar-day delta. Both primitives
 * ({@code to_unixtime}, integer arithmetic) lower through the local {@code to_unixtime} UDF (see
 * {@link UnixTimestampAdapter}) and the Substrait default catalog. (A {@code date_trunc('day', x)}
 * wrapper is unnecessary and, on this stack, isthmus can't bind {@code DATE_TRUNC(char, timestamp)}.)
 *
 * @opensearch.internal
 */
class DateDiffAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode left = dayNumber(original.getOperands().get(0), cluster);
        RexNode right = dayNumber(original.getOperands().get(1), cluster);
        RexNode diff = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, left, right);
        if (diff.getType().equals(original.getType())) {
            return diff;
        }
        return rexBuilder.makeCast(original.getType(), diff, true);
    }

    /**
     * Epoch-day index of {@code x}: floor-divide {@code to_unixtime(x)} by 86400. SQL {@code /}
     * truncates toward zero, which differs from floor for negative epoch seconds (pre-1970) — and
     * isthmus has no {@code FLOOR(i64)} in the bound catalog, so the floor is expressed in pure
     * integer arithmetic:
     * <pre>{@code   floor(x / d) = (x - ((x MOD d + d) MOD d)) / d}</pre>
     * Bare {@code TIME} operands are anchored to today's TIMESTAMP first ({@code to_unixtime}
     * rejects {@code Time64}); both operands anchor to the same date so their day-numbers cancel,
     * matching MySQL's {@code DATEDIFF}-on-times returning 0.
     */
    private static RexNode dayNumber(RexNode operand, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode anchored = DatePartAdapters.coerceCharacterOperandToTimestamp(operand, cluster);
        RexNode epochSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, anchored);
        RexNode secondsPerDay = rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(TimeOfDayLowering.SECONDS_PER_DAY),
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)
        );
        RexNode mod1 = rexBuilder.makeCall(SqlStdOperatorTable.MOD, epochSeconds, secondsPerDay);
        RexNode plusDay = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, mod1, secondsPerDay);
        RexNode floorMod = rexBuilder.makeCall(SqlStdOperatorTable.MOD, plusDay, secondsPerDay);
        RexNode numerator = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, epochSeconds, floorMod);
        return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, numerator, secondsPerDay);
    }
}
