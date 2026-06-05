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
import org.apache.calcite.util.TimestampString;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Adapter for PPL's {@code CONVERT_TZ(ts, from_tz, to_tz)}. Two jobs in
 * priority order:
 *
 * <ol>
 *   <li><b>Identity short-circuit</b>: when both tz operands are string
 *       literals and canonicalize to the same value, the call reduces to its
 *       timestamp operand. No UDF invocation, no wire traffic.</li>
 *   <li><b>UDF fallback with canonicalized literal operands</b>: every other
 *       case rewrites to {@link #LOCAL_CONVERT_TZ_OP} whose
 *       {@code FunctionMappings.Sig} in {@link DataFusionFragmentConvertor}
 *       resolves to the {@code convert_tz} Rust UDF. Literal tz operands are
 *       validated + canonicalized via {@link #canonicalizeTz(String)} at plan
 *       time so bad literals surface with a clear error rather than silent
 *       per-row NULL at runtime.</li>
 * </ol>
 *
 * <p>Why no offset+offset → interval fold: building an interval literal at
 * Calcite's level requires {@code org.apache.calcite.avatica.util.TimeUnit},
 * which lives in avatica and is a {@code runtimeOnly} dep of this module.
 * Pulling it in just for the fixed-offset case doesn't pay for itself; IANA
 * pairs dominate real-world {@code CONVERT_TZ} usage and must go through the
 * UDF anyway (per-row DST lookup).
 *
 * <p>The fallback preserves the original call's return type via
 * {@code rexBuilder.makeCall(original.getType(), ...)} so the enclosing
 * {@code Project} / {@code Filter} rowType cache stays consistent (see
 * {@link AbstractNameMappingAdapter} javadoc for background).
 *
 * @opensearch.internal
 */
class ConvertTzAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator for the rewrite. {@link SqlKind#OTHER_FUNCTION}
     * so it doesn't collide with any Calcite built-in.
     * {@link OperandTypes#ANY_STRING_STRING} keeps validation permissive on the
     * timestamp slot — real argument vetting happens inside the UDF's
     * {@code coerce_types} and {@code invoke_with_args}.
     */
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

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> operands = new ArrayList<>(original.getOperands());
        // Slot 0 is the timestamp; slots 1 and 2 are from_tz / to_tz.
        for (int slot : new int[] { 1, 2 }) {
            operands.set(slot, canonicalizeTzOperand(operands.get(slot), rexBuilder));
        }

        // Identity short-circuit: both operands resolve to the same canonical
        // string → the conversion is a no-op.
        String fromLiteral = tzLiteralValue(operands.get(1));
        String toLiteral = tzLiteralValue(operands.get(2));
        if (fromLiteral != null && toLiteral != null && fromLiteral.equals(toLiteral)) {
            return operands.get(0);
        }

        // String-first variant: when slot 0 is a VARCHAR literal (PPL
        // `convert_tz('<lit>', '<from>', '<to>')`), fold to a TIMESTAMP literal
        // at plan time. The Rust UDF only accepts precision_timestamp inputs so
        // a non-folded string-first call would fail isthmus's signature match
        // ("Unable to convert call convert_tz(string, string, string)"). All-
        // literal folds avoid both the missing YAML overload and a runtime cast.
        if (fromLiteral != null && toLiteral != null) {
            RexNode folded = tryFoldLiteralTimestampOperand(operands.get(0), fromLiteral, toLiteral, original, rexBuilder);
            if (folded != null) {
                return folded;
            }
        }

        // UDF fallback. Preserve the original call's return type — see
        // AbstractNameMappingAdapter for why (Project.isValid compatibleTypes check).
        return rexBuilder.makeCall(original.getType(), LOCAL_CONVERT_TZ_OP, operands);
    }

    /**
     * If operand 0 is a VARCHAR literal (timestamp string) and both tz operands
     * are canonical literals, parse + convert at plan time and return a TIMESTAMP
     * literal. Returns null when slot 0 isn't a VARCHAR literal.
     */
    private static RexNode tryFoldLiteralTimestampOperand(
        RexNode tsOperand,
        String fromTz,
        String toTz,
        RexCall original,
        RexBuilder rexBuilder
    ) {
        if (!(tsOperand instanceof RexLiteral lit)) return null;
        SqlTypeName typeName = lit.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) return null;
        String tsValue = lit.getValueAs(String.class);
        if (tsValue == null) return null;

        ZoneId fromZone;
        ZoneId toZone;
        try {
            fromZone = ZoneId.of(fromTz);
            toZone = ZoneId.of(toTz);
        } catch (DateTimeException e) {
            return null;
        }

        ZonedDateTime resultZdt = parseToZoned(tsValue, fromZone, toZone);
        if (resultZdt == null) {
            return null;
        }

        LocalDateTime ldt = resultZdt.toLocalDateTime();
        TimestampString tsStr = new TimestampString(
            ldt.getYear(),
            ldt.getMonthValue(),
            ldt.getDayOfMonth(),
            ldt.getHour(),
            ldt.getMinute(),
            ldt.getSecond()
        );
        if (ldt.getNano() > 0) {
            tsStr = tsStr.withNanos(ldt.getNano());
        }

        // Use precision 3 (milliseconds) to match the rest of the adapter chain
        // and avoid i64-ns overflow at far-future timestamps.
        int precision = 3;
        RexNode literal = rexBuilder.makeTimestampLiteral(tsStr, precision);
        if (!literal.getType().equals(original.getType())) {
            literal = rexBuilder.makeAbstractCast(original.getType(), literal);
        }
        return literal;
    }

    /**
     * Parse a timestamp literal as in {@code fromZone}, then convert to
     * {@code toZone}. Accepts:
     * <ul>
     *   <li>{@code yyyy-MM-dd HH:mm:ss[.frac]} (space-separated, naive)</li>
     *   <li>ISO {@code yyyy-MM-ddTHH:mm:ss[.frac]} (naive)</li>
     *   <li>any format with embedded offset (used in preference to {@code fromZone})</li>
     * </ul>
     * Returns null when no format matches.
     */
    private static ZonedDateTime parseToZoned(String input, ZoneId fromZone, ZoneId toZone) {
        // Embedded offset takes precedence over the explicit fromZone.
        try {
            OffsetDateTime odt = OffsetDateTime.parse(input);
            return odt.atZoneSameInstant(toZone);
        } catch (DateTimeParseException ignored) {}
        if (input.contains(" ") && !input.contains("T")) {
            try {
                OffsetDateTime odt = OffsetDateTime.parse(input.replace(' ', 'T'));
                return odt.atZoneSameInstant(toZone);
            } catch (DateTimeParseException ignored) {}
        }

        // Naive timestamp: parse as in fromZone.
        try {
            LocalDateTime ldt = LocalDateTime.parse(input, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT));
            return inFromZoneTo(ldt, fromZone, toZone);
        } catch (DateTimeParseException ignored) {}

        try {
            LocalDateTime ldt = LocalDateTime.parse(input);
            return inFromZoneTo(ldt, fromZone, toZone);
        } catch (DateTimeParseException ignored) {}

        return null;
    }

    /**
     * Resolve {@code ldt} as an instant in {@code fromZone} and convert to {@code toZone}.
     * Uses {@link java.time.zone.ZoneRules#getValidOffsets} to pick a deterministic offset
     * (the first valid one — for ambiguous DST fall-back times this is the pre-transition
     * offset, matching java.time.LocalDateTime#atZone earlier-offset rule).
     */
    private static ZonedDateTime inFromZoneTo(LocalDateTime ldt, ZoneId fromZone, ZoneId toZone) {
        java.util.List<java.time.ZoneOffset> offsets = fromZone.getRules().getValidOffsets(ldt);
        java.time.ZoneOffset chosen;
        if (!offsets.isEmpty()) {
            chosen = offsets.get(0);
        } else {
            // Gap (DST spring-forward) — pick the offset after the transition.
            java.time.zone.ZoneOffsetTransition transition = fromZone.getRules().getTransition(ldt);
            chosen = transition.getOffsetAfter();
            ldt = ldt.plus(transition.getDuration());
        }
        java.time.Instant instant = ldt.toInstant(chosen);
        return instant.atZone(toZone);
    }

    /**
     * Returns the string value of a canonicalized tz literal operand, or null
     * when the operand is not a VARCHAR/CHAR {@link RexLiteral} (column refs,
     * NULL literals, other expressions).
     */
    private static String tzLiteralValue(RexNode operand) {
        if (!(operand instanceof RexLiteral literal)) return null;
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) return null;
        return literal.getValueAs(String.class);
    }

    /**
     * If {@code operand} is a string {@link RexLiteral}, canonicalize it and
     * return a new literal with the canonical form (or the original if already
     * canonical). Non-literal operands (column references, function results)
     * pass through untouched — their runtime values can't be validated until
     * the UDF runs.
     *
     * <p>Throws {@link IllegalArgumentException} for literals that don't match
     * either the {@code ±HH:MM} offset pattern or a known IANA zone id.
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
     * Canonicalize a timezone string. Accepts either:
     * <ul>
     *   <li>{@code ±H:MM} / {@code ±HH:MM} where hours ∈ [0,14] and minutes ∈ [0,59];
     *       returned zero-padded as {@code ±HH:MM}.</li>
     *   <li>IANA zone id recognized by {@link ZoneId#of(String)}; returned as the
     *       JDK-normalized form. {@code ZoneId.of} rejects unknown ids, so invalid
     *       IANA names surface here as {@link IllegalArgumentException}.</li>
     * </ul>
     *
     * <p>The {@code ±HH:MM} bounds match the Rust UDF's {@code parse_offset_seconds}
     * (rust/src/udf/convert_tz.rs) — `+14:59` is the maximum offset anywhere on
     * Earth (Kiribati is +14:00; the extra minute tolerance matches existing
     * UDF behavior).
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
