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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.NumericToDoubleAdapter;

import java.math.BigDecimal;

/**
 * Shared lowering helpers for PPL time-of-day arithmetic, used by {@link SecToTimeAdapter},
 * {@link AddSubTimeAdapter}, and {@link TimeDiffAdapter}.
 *
 * <p>The key building block is {@link #secondsToTime}: it turns a BIGINT seconds expression into a
 * {@code TIME} value via the {@code maketime(h, m, s)} Rust UDF, because a direct
 * {@code CAST(timestamp AS TIME)} is rejected by DataFusion's optimizer
 * ({@code Unsupported CAST from Timestamp to Time}). The seconds are first normalized into a single
 * day {@code [0, 86400)} with {@code ((s MOD 86400) + 86400) MOD 86400} so negative differences and
 * past-midnight sums wrap the MySQL way.
 *
 * @opensearch.internal
 */
final class TimeOfDayLowering {

    private TimeOfDayLowering() {}

    static final long SECONDS_PER_DAY = 86_400L;

    static RexNode bigintLit(RexBuilder rexBuilder, long value) {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value), rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    }

    /**
     * The seconds-of-day {@code [0, 86400)} of a temporal value:
     * {@code ((to_unixtime(anchor(x)) MOD 86400) + 86400) MOD 86400}. The floor-mod is required
     * because pre-1970 values have a negative epoch and SQL {@code MOD} truncates toward zero (so a
     * bare {@code MOD 86400} would yield a negative seconds-of-day).
     */
    static RexNode secondsOfDay(RexNode operand, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode anchored = DatePartAdapters.coerceCharacterOperandToTimestamp(operand, cluster);
        RexNode epoch = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, anchored);
        RexNode mod1 = rexBuilder.makeCall(SqlStdOperatorTable.MOD, epoch, bigintLit(rexBuilder, SECONDS_PER_DAY));
        RexNode plusDay = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, mod1, bigintLit(rexBuilder, SECONDS_PER_DAY));
        return rexBuilder.makeCall(SqlStdOperatorTable.MOD, plusDay, bigintLit(rexBuilder, SECONDS_PER_DAY));
    }

    /**
     * Build a {@code TIME}-typed value from a BIGINT seconds expression: normalize into
     * {@code [0, 86400)}, decompose into hour/minute/second, and call {@code maketime(h, m, s)}.
     */
    static RexNode secondsToTime(RexNode secondsExpr, RelDataType timeType, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // norm = ((s MOD 86400) + 86400) MOD 86400 — wraps negatives and >1-day sums into a day.
        RexNode mod1 = rexBuilder.makeCall(SqlStdOperatorTable.MOD, secondsExpr, bigintLit(rexBuilder, SECONDS_PER_DAY));
        RexNode plusDay = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, mod1, bigintLit(rexBuilder, SECONDS_PER_DAY));
        RexNode norm = rexBuilder.makeCall(SqlStdOperatorTable.MOD, plusDay, bigintLit(rexBuilder, SECONDS_PER_DAY));

        RexNode hour = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, norm, bigintLit(rexBuilder, 3600));
        RexNode minute = rexBuilder.makeCall(
            SqlStdOperatorTable.DIVIDE,
            rexBuilder.makeCall(SqlStdOperatorTable.MOD, norm, bigintLit(rexBuilder, 3600)),
            bigintLit(rexBuilder, 60)
        );
        RexNode second = rexBuilder.makeCall(SqlStdOperatorTable.MOD, norm, bigintLit(rexBuilder, 60));

        return rexBuilder.makeCall(
            timeType,
            RustUdfDateTimeAdapters.LOCAL_MAKETIME_OP,
            java.util.List.of(
                NumericToDoubleAdapter.widenToDoubleIfNumeric(hour, cluster),
                NumericToDoubleAdapter.widenToDoubleIfNumeric(minute, cluster),
                NumericToDoubleAdapter.widenToDoubleIfNumeric(second, cluster)
            )
        );
    }
}
