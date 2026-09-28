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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

/**
 * Rewrites PPL's {@code UNIX_TIMESTAMP(ts)} to {@code to_unixtime} (DataFusion's native name; no
 * Rust UDF registration). PPL declares the result {@code DOUBLE_FORCE_NULLABLE} but DataFusion
 * returns {@code Int64} — return type is preserved on the rewritten call so {@code Project.isValid}
 * holds; the substrait consumer re-resolves and re-coerces by name.
 *
 * <p>Numeric literal in {@code YYYYMMDDhhmmss} shape is rewritten to {@code 'YYYY-MM-DD hh:mm:ss'}
 * so {@code to_unixtime} (which only accepts strings/timestamps) can parse it (MySQL semantics).
 *
 * @opensearch.internal
 */
class UnixTimestampAdapter implements ScalarFunctionAdapter {

    /** Locally-declared rewrite target; name matches DataFusion's native {@code to_unixtime}. */
    static final SqlOperator LOCAL_TO_UNIXTIME_OP = new SqlFunction(
        "to_unixtime",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.TIMEDATE
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> operands = original.getOperands();
        if (operands.size() == 1) {
            String dateLiteral = numericYyyyMmDdHhMmSsToString(operands.get(0));
            if (dateLiteral != null) {
                RexNode strLit = rexBuilder.makeLiteral(dateLiteral, rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR), true);
                return rexBuilder.makeCall(original.getType(), LOCAL_TO_UNIXTIME_OP, List.of(strLit));
            }
        }
        return rexBuilder.makeCall(original.getType(), LOCAL_TO_UNIXTIME_OP, operands);
    }

    /** Detect 14-digit numeric literal {@code YYYYMMDDhhmmss}; returns the formatted string or null. */
    private static String numericYyyyMmDdHhMmSsToString(RexNode operand) {
        if (!(operand instanceof RexLiteral literal)) return null;
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (!SqlTypeName.NUMERIC_TYPES.contains(typeName)) return null;
        BigDecimal raw = literal.getValueAs(BigDecimal.class);
        if (raw == null || raw.scale() > 0) return null;
        String digits = raw.toBigInteger().toString();
        if (digits.length() != 14) return null;
        try {
            int year = Integer.parseInt(digits.substring(0, 4));
            int month = Integer.parseInt(digits.substring(4, 6));
            int day = Integer.parseInt(digits.substring(6, 8));
            int hour = Integer.parseInt(digits.substring(8, 10));
            int minute = Integer.parseInt(digits.substring(10, 12));
            int second = Integer.parseInt(digits.substring(12, 14));
            LocalDateTime.of(year, month, day, hour, minute, second); // calendar validity check
            return String.format(Locale.ROOT, "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
        } catch (NumberFormatException | DateTimeException invalid) {
            return null;
        }
    }
}
