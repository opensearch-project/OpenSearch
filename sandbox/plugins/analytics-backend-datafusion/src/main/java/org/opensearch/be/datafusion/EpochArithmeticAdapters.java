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
import org.opensearch.analytics.spi.NumericToDoubleAdapter;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Lowerings for PPL datetime scalars that reduce to epoch arithmetic over the local
 * {@code to_unixtime} / {@code from_unixtime} UDFs (see {@link UnixTimestampAdapter},
 * {@link RustUdfDateTimeAdapters}). The raw PPL UDFs have no Substrait binding, so isthmus rejects
 * them; each adapter rewrites to {@code to_unixtime(...)} plus integer arithmetic that lowers
 * through the Substrait default catalog.
 *
 * <p>Constants:
 * <ul>
 *   <li>{@code DAYS_YEAR0_TO_EPOCH = 719528} — days from {@code 0000-01-01} to {@code 1970-01-01}
 *       (MySQL's day 1 is {@code 0000-01-01}, so {@code TO_DAYS('1970-01-01') = 719528}).</li>
 *   <li>{@code SECONDS_YEAR0_TO_EPOCH = 62167219200} — seconds for the same span
 *       ({@code 719528 * 86400}).</li>
 * </ul>
 *
 * @opensearch.internal
 */
final class EpochArithmeticAdapters {

    private EpochArithmeticAdapters() {}

    private static final long SECONDS_PER_DAY = 86_400L;
    private static final long DAYS_YEAR0_TO_EPOCH = 719_528L;
    private static final long SECONDS_YEAR0_TO_EPOCH = DAYS_YEAR0_TO_EPOCH * SECONDS_PER_DAY;

    private static RexNode bigintLit(RexBuilder rexBuilder, long value) {
        return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value), rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT));
    }

    private static RexNode toUnixtime(RexBuilder rexBuilder, RexNode operand) {
        return rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, operand);
    }

    private static RexNode pinReturnType(RexBuilder rexBuilder, RexNode expr, RexCall original) {
        if (expr.getType().equals(original.getType())) {
            return expr;
        }
        return rexBuilder.makeCast(original.getType(), expr, true);
    }

    /**
     * PPL {@code TO_DAYS(x)} — days since {@code 0000-01-01} (BIGINT):
     * {@code to_unixtime(x) / 86400 + 719528}.
     */
    static final class ToDaysAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().size() != 1) {
                return original;
            }
            // Reject a malformed string literal at plan time with the PPL format-hint message
            // (HTTP 400) rather than letting to_unixtime fail opaquely at runtime (HTTP 500).
            DatetimeLiteralValidator.validate(original.getOperands().get(0), DatetimeLiteralValidator.Kind.DATE);
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode epochDays = rexBuilder.makeCall(
                SqlStdOperatorTable.DIVIDE,
                toUnixtime(rexBuilder, original.getOperands().get(0)),
                bigintLit(rexBuilder, SECONDS_PER_DAY)
            );
            RexNode totalDays = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, epochDays, bigintLit(rexBuilder, DAYS_YEAR0_TO_EPOCH));
            return pinReturnType(rexBuilder, totalDays, original);
        }
    }

    /**
     * PPL {@code TO_SECONDS(x)} — seconds since {@code 0000-01-01} (BIGINT):
     * {@code to_unixtime(x) + 62167219200}.
     */
    static final class ToSecondsAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().size() != 1) {
                return original;
            }
            // Reject a malformed string literal at plan time (HTTP 400 + format hint) instead of an
            // opaque runtime to_unixtime parse error. Kind.DATE accepts both bare dates and full
            // datetimes, so valid TO_SECONDS inputs like '2020-09-16 07:40:00' still pass.
            DatetimeLiteralValidator.validate(original.getOperands().get(0), DatetimeLiteralValidator.Kind.DATE);
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode total = rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                toUnixtime(rexBuilder, original.getOperands().get(0)),
                bigintLit(rexBuilder, SECONDS_YEAR0_TO_EPOCH)
            );
            return pinReturnType(rexBuilder, total, original);
        }
    }

    /**
     * PPL {@code FROM_DAYS(n)} — inverse of {@link ToDaysAdapter}, returns DATE:
     * {@code from_unixtime((n - 719528) * 86400)}.
     */
    static final class FromDaysAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().size() != 1) {
                return original;
            }
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode days = rexBuilder.makeCast(
                rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
                original.getOperands().get(0),
                true
            );
            RexNode epochDays = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, days, bigintLit(rexBuilder, DAYS_YEAR0_TO_EPOCH));
            RexNode epochSeconds = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, epochDays, bigintLit(rexBuilder, SECONDS_PER_DAY));
            // from_unixtime's Substrait signature is fp64; widen the i64 seconds to DOUBLE or
            // isthmus rejects the call with "Unable to convert call from_unixtime(i64)".
            RexNode epochSecondsDouble = NumericToDoubleAdapter.widenToDoubleIfNumeric(epochSeconds, cluster);
            RelDataType tsType = rexBuilder.getTypeFactory()
                .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP), true);
            RexNode ts = rexBuilder.makeCall(tsType, RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, List.of(epochSecondsDouble));
            return pinReturnType(rexBuilder, ts, original);
        }
    }

    /**
     * PPL {@code TIME_TO_SEC(x)} — seconds-of-day of a TIME / TIMESTAMP (BIGINT):
     * {@code to_unixtime(x) MOD 86400}. The modulus discards the date, so anchoring is irrelevant.
     * {@code to_unixtime} rejects an Arrow {@code Time64} operand, and a direct
     * {@code CAST(time AS timestamp)} is also unsupported by DataFusion's optimizer
     * ({@code Unsupported CAST from Time64(ns) to Timestamp}), so a bare {@code TIME} is anchored to
     * today UTC via the shared {@link DatePartAdapters#coerceCharacterOperandToTimestamp} CONCAT path
     * (cast to VARCHAR, prefix today's date, parse as TIMESTAMP) — the date is modded away anyway.
     */
    static final class TimeToSecAdapter implements ScalarFunctionAdapter {
        @Override
        public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
            if (original.getOperands().size() != 1) {
                return original;
            }
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RexNode operand = DatePartAdapters.coerceCharacterOperandToTimestamp(original.getOperands().get(0), cluster);
            RexNode secOfDay = rexBuilder.makeCall(
                SqlStdOperatorTable.MOD,
                toUnixtime(rexBuilder, operand),
                bigintLit(rexBuilder, SECONDS_PER_DAY)
            );
            return pinReturnType(rexBuilder, secOfDay, original);
        }
    }
}
