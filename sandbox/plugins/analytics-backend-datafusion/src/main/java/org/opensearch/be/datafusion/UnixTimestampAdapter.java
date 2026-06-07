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
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

/**
 * Rename adapter for PPL's {@code UNIX_TIMESTAMP(ts)}. Rewrites to a
 * locally-declared {@link SqlFunction} named {@code to_unixtime} — the name
 * DataFusion's substrait consumer recognizes for its native
 * {@code ToUnixtimeFunc} (no UDF registration required on the Rust side).
 *
 * <p>Same machinery as {@link ConvertTzAdapter}: locally-declared operator is
 * the referent of the {@link io.substrait.isthmus.expression.FunctionMappings.Sig}
 * in {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS}.
 *
 * <p><b>Type note.</b> PPL's {@code UNIX_TIMESTAMP} returns
 * {@code DOUBLE_FORCE_NULLABLE}; DataFusion's {@code to_unixtime} returns
 * {@code Int64}. {@link AbstractNameMappingAdapter} preserves the PPL-declared
 * return type on the rewritten call so Calcite's {@code Project.isValid}
 * assertion holds. The downstream substrait consumer (DataFusion) re-resolves
 * {@code to_unixtime} by name and applies its own {@code coerce_types}, so the
 * Calcite-inferred type is purely plan-validity bookkeeping.
 *
 * <p><b>Cluster F case 5.</b> MySQL's {@code UNIX_TIMESTAMP} accepts a numeric
 * literal in the {@code YYYYMMDDhhmmss} shape (14 digits) and reads it as a
 * UTC datetime. {@code to_unixtime} only accepts strings/timestamps, so we
 * detect that literal at plan time and rewrite it to its string-datetime form.
 *
 * @opensearch.internal
 */
class UnixTimestampAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator. Name matches DataFusion's native
     * {@code to_unixtime}. Return-type inference is irrelevant — the adapter
     * clones with the original PPL return type.
     */
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

    /**
     * Cluster F case 5: detect a numeric literal of shape {@code YYYYMMDDhhmmss}
     * and convert to {@code 'YYYY-MM-DD hh:mm:ss'}. Returns null when the
     * operand isn't a numeric literal, isn't 14 digits, or doesn't decode to a
     * valid calendar datetime.
     */
    private static String numericYyyyMmDdHhMmSsToString(RexNode operand) {
        if (!(operand instanceof RexLiteral literal)) return null;
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (!SqlTypeName.NUMERIC_TYPES.contains(typeName)) return null;
        BigDecimal raw = literal.getValueAs(BigDecimal.class);
        if (raw == null || raw.scale() > 0) return null;
        // 14 digits exactly: 4 year + 2 month + 2 day + 2 hour + 2 min + 2 sec.
        String digits = raw.toBigInteger().toString();
        if (digits.length() != 14) return null;
        try {
            int year = Integer.parseInt(digits.substring(0, 4));
            int month = Integer.parseInt(digits.substring(4, 6));
            int day = Integer.parseInt(digits.substring(6, 8));
            int hour = Integer.parseInt(digits.substring(8, 10));
            int minute = Integer.parseInt(digits.substring(10, 12));
            int second = Integer.parseInt(digits.substring(12, 14));
            // LocalDateTime validates calendar correctness (e.g. month 13, day 31 in Feb).
            LocalDateTime.of(year, month, day, hour, minute, second);
            return String.format(Locale.ROOT, "%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second);
        } catch (NumberFormatException | DateTimeException invalid) {
            return null;
        }
    }
}
