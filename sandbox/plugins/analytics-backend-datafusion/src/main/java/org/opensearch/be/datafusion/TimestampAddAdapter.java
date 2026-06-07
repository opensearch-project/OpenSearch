/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Adapter for standalone PPL {@code TIMESTAMPADD(unit, n, ts)}: rewrites to
 * {@code DATETIME_PLUS(CAST(ts AS TIMESTAMP), INTERVAL n_in_base_unit base_unit)} so it lowers
 * via Substrait's {@code add(timestamp, interval)} that DataFusion executes natively.
 *
 * <p>Mirrors {@link DateAddSubAdapter}'s base-unit normalisation (millis for day-time, months for
 * year-month). Sub-millisecond units (MICROSECOND) and any unrecognised unit fall through unchanged
 * so the substrait conversion failure surfaces as the original "Unable to convert call" message.
 *
 * @opensearch.internal
 */
class TimestampAddAdapter implements ScalarFunctionAdapter {

    private static final long MILLIS_PER_SECOND = 1_000L;
    private static final long MILLIS_PER_MINUTE = 60_000L;
    private static final long MILLIS_PER_HOUR = 3_600_000L;
    private static final long MILLIS_PER_DAY = 86_400_000L;
    private static final long MILLIS_PER_WEEK = 7L * MILLIS_PER_DAY;

    private static final Map<String, long[]> UNIT_TO_BASE = Map.ofEntries(
        Map.entry("MILLISECOND", new long[] { 1L, 0L }),
        Map.entry("SECOND", new long[] { MILLIS_PER_SECOND, 0L }),
        Map.entry("MINUTE", new long[] { MILLIS_PER_MINUTE, 0L }),
        Map.entry("HOUR", new long[] { MILLIS_PER_HOUR, 0L }),
        Map.entry("DAY", new long[] { MILLIS_PER_DAY, 0L }),
        Map.entry("WEEK", new long[] { MILLIS_PER_WEEK, 0L }),
        Map.entry("MONTH", new long[] { 1L, 1L }),
        Map.entry("QUARTER", new long[] { 3L, 1L }),
        Map.entry("YEAR", new long[] { 12L, 1L })
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (!original.getOperator().getName().equalsIgnoreCase("TIMESTAMPADD") || original.getOperands().size() != 3) {
            return original;
        }
        RexNode unitOp = original.getOperands().get(0);
        RexNode countOp = original.getOperands().get(1);
        RexNode tsOp = original.getOperands().get(2);
        if (!(unitOp instanceof RexLiteral unitLit) || !(countOp instanceof RexLiteral countLit)) {
            return original;
        }
        String unit = unitLit.getValueAs(String.class);
        if (unit == null) {
            return original;
        }
        long[] baseSpec = UNIT_TO_BASE.get(unit.toUpperCase(Locale.ROOT));
        if (baseSpec == null) {
            return original;
        }
        Long n;
        try {
            n = countLit.getValueAs(BigDecimal.class) == null ? null : countLit.getValueAs(BigDecimal.class).longValueExact();
        } catch (ArithmeticException unused) {
            return original;
        }
        if (n == null) {
            return original;
        }
        long baseValue = Math.multiplyExact(n, baseSpec[0]);
        TimeUnit baseUnit = baseSpec[1] == 1L ? TimeUnit.MONTH : TimeUnit.DAY;

        RexBuilder rb = cluster.getRexBuilder();
        // Lift ts to the call's TIMESTAMP type (handles VARCHAR / DATE / TIME / TIMESTAMP).
        RelDataType targetType = original.getType();
        RexNode tsTimestamp;
        SqlTypeName tsKind = tsOp.getType().getSqlTypeName();
        if (tsKind == SqlTypeName.TIME) {
            // anchor TIME to today UTC matching DateAddSubAdapter behavior
            RelDataType varchar = rb.getTypeFactory().createSqlType(SqlTypeName.VARCHAR);
            RelDataType nullableVarchar = rb.getTypeFactory().createTypeWithNullability(varchar, tsOp.getType().isNullable());
            RexNode timeAsVarchar = rb.makeCast(nullableVarchar, tsOp);
            String prefix = java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString() + " ";
            RexNode prefixLit = rb.makeLiteral(prefix, varchar, false);
            RexNode concat = rb.makeCall(nullableVarchar, SqlStdOperatorTable.CONCAT, List.of(prefixLit, timeAsVarchar));
            tsTimestamp = rb.makeAbstractCast(targetType, concat);
        } else {
            tsTimestamp = rb.makeAbstractCast(targetType, tsOp);
        }

        RexNode interval = rb.makeIntervalLiteral(
            BigDecimal.valueOf(baseValue),
            new SqlIntervalQualifier(baseUnit, null, SqlParserPos.ZERO)
        );
        RexNode shifted = rb.makeCall(SqlStdOperatorTable.DATETIME_PLUS, tsTimestamp, interval);
        if (shifted.getType().equals(targetType)) {
            return shifted;
        }
        return rb.makeAbstractCast(targetType, shifted);
    }
}
