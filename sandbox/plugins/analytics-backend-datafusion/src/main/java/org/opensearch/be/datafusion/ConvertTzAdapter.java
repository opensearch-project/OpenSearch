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

import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Adapter for PPL's {@code CONVERT_TZ(ts, from_tz, to_tz)}. Three jobs in priority order:
 * identity short-circuit when both tz literals canonicalize to the same value, typed-NULL
 * fallback when a literal tz fails validation (MySQL semantics), and rewrite to
 * {@link #LOCAL_CONVERT_TZ_OP} otherwise. Literal tz operands are canonicalized at plan time
 * via {@link #canonicalizeTz(String)} so the UDF sees a stable form.
 *
 * <p>No offset+offset interval fold: avatica's {@code TimeUnit} is a {@code runtimeOnly} dep
 * and IANA pairs dominate real usage anyway. Return type is preserved on the rewritten call so
 * enclosing {@code Project}/{@code Filter} rowType cache stays consistent
 * (see {@link AbstractNameMappingAdapter}).
 *
 * @opensearch.internal
 */
class ConvertTzAdapter implements ScalarFunctionAdapter {

    /** Locally-declared rewrite target; permissive operand types — UDF does the real vetting. */
    static final SqlOperator LOCAL_CONVERT_TZ_OP = new SqlFunction(
        "convert_tz",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY_STRING_STRING,
        SqlFunctionCategory.TIMEDATE
    );

    /** Matches {@code ±H:MM} / {@code ±HH:MM} with hours [0,14] and minutes [0,59]. */
    private static final Pattern OFFSET_PATTERN = Pattern.compile("^([+-])(\\d{1,2}):(\\d{2})$");

    /** invalid timestamp literal -> typed NULL (peels CAST wrapping that PPL adds for non-TIMESTAMP args). */
    private static RexNode foldInvalidTimestampLiteralToNull(
        RexNode operand,
        org.apache.calcite.rel.type.RelDataType resultType,
        RexBuilder rexBuilder
    ) {
        RexNode unwrapped = operand;
        while (unwrapped instanceof RexCall call
            && (call.getKind() == org.apache.calcite.sql.SqlKind.CAST || call.getKind() == org.apache.calcite.sql.SqlKind.SAFE_CAST)
            && call.getOperands().size() == 1) {
            unwrapped = call.getOperands().get(0);
        }
        if (!(unwrapped instanceof RexLiteral literal)) return null;
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) return null;
        String value = literal.getValueAs(String.class);
        if (value == null) return null;
        try {
            java.time.LocalDateTime.parse(value.replace(' ', 'T'));
            return null;
        } catch (java.time.format.DateTimeParseException ignored) {}
        return rexBuilder.makeNullLiteral(resultType);
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> operands = new ArrayList<>(original.getOperands());
        // invalid timestamp literal -> typed NULL (MySQL CONVERT_TZ semantics)
        RexNode tsFolded = foldInvalidTimestampLiteralToNull(operands.get(0), original.getType(), rexBuilder);
        if (tsFolded != null) {
            return tsFolded;
        }
        // invalid tz literal -> typed NULL; column-valued tz routes through the UDF
        for (int slot : new int[] { 1, 2 }) {
            try {
                operands.set(slot, canonicalizeTzOperand(operands.get(slot), rexBuilder));
            } catch (IllegalArgumentException badTz) {
                return rexBuilder.makeNullLiteral(original.getType());
            }
        }

        // Same from/to tz → no-op. SAFE-cast a VARCHAR operand to TIMESTAMP so the parent
        // Project rowType stays consistent (bare VARCHAR trips RexUtil.compatibleTypes).
        String fromLiteral = tzLiteralValue(operands.get(1));
        String toLiteral = tzLiteralValue(operands.get(2));
        if (fromLiteral != null && toLiteral != null && fromLiteral.equals(toLiteral)) {
            RexNode operand = operands.get(0);
            if (operand.getType().equals(original.getType())) {
                return operand;
            }
            return rexBuilder.makeCast(original.getType(), operand, true, true);
        }

        // Substrait declares convert_tz(precision_timestamp, string, string); SAFE-cast
        // a VARCHAR timestamp slot so it binds (NULL on parse failure).
        if (!operands.get(0).getType().equals(original.getType())) {
            operands.set(0, rexBuilder.makeCast(original.getType(), operands.get(0), true, true));
        }
        return rexBuilder.makeCall(original.getType(), LOCAL_CONVERT_TZ_OP, operands);
    }

    /** String value of a canonicalized tz literal, or null for non-literal/non-string operands. */
    private static String tzLiteralValue(RexNode operand) {
        if (!(operand instanceof RexLiteral literal)) return null;
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) return null;
        return literal.getValueAs(String.class);
    }

    /**
     * Canonicalize a string literal tz operand; non-literals and non-strings pass through.
     * Throws {@link IllegalArgumentException} for unrecognized literals.
     */
    private static RexNode canonicalizeTzOperand(RexNode operand, RexBuilder rexBuilder) {
        if (!(operand instanceof RexLiteral literal)) {
            return operand;
        }
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
            return operand;
        }
        String raw = literal.getValueAs(String.class);
        if (raw == null) {
            // NULL literal — UDF handles null operand at runtime.
            return operand;
        }
        String canonical = canonicalizeTz(raw);
        if (canonical.equals(raw)) {
            return operand;
        }
        return rexBuilder.makeLiteral(
            canonical,
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
            literal.getType().isNullable()
        );
    }

    /**
     * Canonicalize a tz string: {@code ±H:MM}/{@code ±HH:MM} (hours 0-14, minutes 0-59,
     * matches Rust {@code parse_offset_seconds}) or an IANA id resolvable by {@link ZoneId#of}.
     * Throws {@link IllegalArgumentException} for unrecognized inputs.
     */
    static String canonicalizeTz(String raw) {
        Matcher offset = OFFSET_PATTERN.matcher(raw);
        if (offset.matches()) {
            String sign = offset.group(1);
            int hours = Integer.parseInt(offset.group(2));
            int minutes = Integer.parseInt(offset.group(3));
            if (hours > 14 || minutes > 59) {
                throw new IllegalArgumentException(
                    "convert_tz: invalid offset [" + raw + "] — hours must be in [0, 14] and minutes in [0, 59]"
                );
            }
            return String.format(Locale.ROOT, "%s%02d:%02d", sign, hours, minutes);
        }
        try {
            // ZoneId.of() throws for unknown ids; the returned ZoneId.getId()
            // is the JDK's canonical form (same id for equivalent inputs).
            return ZoneId.of(raw).getId();
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("convert_tz: invalid timezone [" + raw + "] — expected IANA zone id or ±HH:MM offset", e);
        }
    }
}
