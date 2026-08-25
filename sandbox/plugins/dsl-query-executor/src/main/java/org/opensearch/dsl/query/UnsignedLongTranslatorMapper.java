/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.dsl.query.range.RangeBoundMath;

import java.math.BigDecimal;
import java.util.Optional;

/**
 * Translator mapper for {@code unsigned_long} fields. Implements bound translation logic
 * directly (negative clamping, decimal truncation, overflow guards) and term parsing inline.
 *
 * <p>This mapper is a stateless singleton shared across every {@code unsigned_long} field in
 * the schema. No per-field state is held; all parameters are derived from the
 * {@code RelDataType} on each call.
 */
final class UnsignedLongTranslatorMapper extends BaseTranslatorMapper {

    /** Singleton instance. */
    static final UnsignedLongTranslatorMapper INSTANCE = new UnsignedLongTranslatorMapper();

    private UnsignedLongTranslatorMapper() {}

    /**
     * Translates a single range bound for an unsigned_long field applying legacy
     * {@code NumberFieldMapper.unsignedLongRangeQuery} semantics: negative clamping,
     * decimal truncation, and overflow guards.
     */
    @Override
    protected RexNode translateBound(Object value, boolean isLower, boolean inclusive, RelDataTypeField field, ConversionContext ctx)
        throws ConversionException {
        if (value == null) {
            return null;
        }

        // Parse the value to a double for sign/decimal checks.
        double doubleValue = parseUnsignedLongBound(value, field.getName());

        // Negative bounds: per NumberFieldMapper.objectToUnsignedLong(lenientBound=true),
        // values below 0 clamp to 0 (lower) or match-none (upper).
        if (doubleValue < 0) {
            if (isLower) {
                return null;
            } else {
                return ctx.getRexBuilder().makeLiteral(false);
            }
        }

        // (a) Value in [0, Long.MAX_VALUE]: apply legacy decimal truncate+adjust.
        long longValue = truncateToLong(value, doubleValue);
        boolean hasDecimal = hasDecimalPartForUnsignedLong(value, doubleValue);
        double sign = Math.signum(doubleValue);

        // Legacy unsignedLongRangeQuery: increment/decrement for exclusive or decimal bounds.
        if (isLower) {
            if ((!hasDecimal && !inclusive) || (hasDecimal && sign > 0)) {
                if (longValue == Long.MAX_VALUE) {
                    return ctx.getRexBuilder().makeLiteral(false);
                }
                longValue++;
            }
        } else {
            if ((!hasDecimal && !inclusive) || (hasDecimal && sign < 0)) {
                if (longValue == 0) {
                    return ctx.getRexBuilder().makeLiteral(false);
                }
                longValue--;
            }
        }

        // Emit inclusive comparison (adjustments above made the bound inclusive).
        RexNode literal = ctx.getRexBuilder().makeLiteral(longValue, field.getType(), true);
        RexNode fieldRef = ctx.getRexBuilder().makeInputRef(field.getType(), field.getIndex());
        SqlOperator op = isLower ? SqlStdOperatorTable.GREATER_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        return ctx.getRexBuilder().makeCall(op, fieldRef, literal);
    }

    /**
     * Parses an unsigned_long bound value to double for sign and magnitude checks.
     * Mirrors {@code NumberFieldMapper.objectToDouble} pathway used by objectToUnsignedLong.
     * For values that may be above Long.MAX_VALUE, uses BigDecimal comparison to avoid
     * double precision loss (Long.MAX_VALUE and Long.MAX_VALUE+1 are indistinguishable as doubles).
     *
     * @throws ConversionException if non-numeric, non-finite, or above Long.MAX_VALUE
     */
    private static double parseUnsignedLongBound(Object value, String fieldName) throws ConversionException {
        if (value instanceof Number num) {
            // WHY: NaN/Infinity on unsigned_long would pass the < 0 check (NaN comparisons are false)
            // and produce a garbage truncation via (long) NaN = 0. Legacy throws IAE for non-finite.
            double d = num.doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                throw new ConversionException(
                    "Non-finite value (" + value + ") is not supported for unsigned_long field '" + fieldName + "'"
                );
            }
            checkAboveLongMax(num, value, fieldName);
            return d;
        }
        // String value: parse and check via BigDecimal for precision
        String str = value.toString();
        // WHY: Strings like "NaN", "Infinity" are accepted by Double.parseDouble but not BigDecimal;
        // reject them explicitly with a clear message matching the Number path.
        if (RangeBoundMath.isNonFinite(str)) {
            throw new ConversionException("Non-finite value (" + value + ") is not supported for unsigned_long field '" + fieldName + "'");
        }
        try {
            BigDecimal bd = new BigDecimal(str);
            if (bd.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                throw new ConversionException(
                    "Unsigned long range bound above Long.MAX_VALUE is not representable on the DSL path "
                        + "(schema_coerce.rs UInt64→Int64 narrowing) for field '"
                        + fieldName
                        + "': "
                        + value
                );
            }
            return bd.doubleValue();
        } catch (NumberFormatException e) {
            throw new ConversionException("Non-numeric range bound for unsigned_long field '" + fieldName + "': " + value);
        }
    }

    /** Checks if a Number value exceeds Long.MAX_VALUE using BigDecimal precision. */
    private static void checkAboveLongMax(Number num, Object originalValue, String fieldName) throws ConversionException {
        // For integers types already known to fit in long, skip
        if (num instanceof Long || num instanceof Integer || num instanceof Short || num instanceof Byte) {
            return;
        }
        // For Double/Float, compare against Long.MAX_VALUE with tolerance
        BigDecimal bd = BigDecimal.valueOf(num.doubleValue());
        if (bd.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
            throw new ConversionException(
                "Unsigned long range bound above Long.MAX_VALUE is not representable on the DSL path "
                    + "(schema_coerce.rs UInt64→Int64 narrowing) for field '"
                    + fieldName
                    + "': "
                    + originalValue
            );
        }
    }

    /**
     * Truncates to long for unsigned_long bound handling. Uses BigDecimal for string values
     * to avoid double precision loss on large longs; Number values use longValue() (floor
     * toward zero) matching legacy BigInteger truncation.
     */
    private static long truncateToLong(Object value, double doubleValue) {
        if (value instanceof Number) {
            if (value instanceof Double || value instanceof Float) {
                return (long) doubleValue;
            }
            return ((Number) value).longValue();
        }
        // String: use BigDecimal to preserve precision for large values
        try {
            return new BigDecimal(value.toString()).toBigInteger().longValue();
        } catch (NumberFormatException e) {
            return (long) doubleValue;
        }
    }

    /**
     * Checks if the value has a non-zero fractional part, for unsigned_long bound adjustment.
     * Mirrors {@code NumberFieldMapper.hasDecimalPart}.
     */
    private static boolean hasDecimalPartForUnsignedLong(Object value, double doubleValue) {
        return doubleValue % 1 != 0;
    }

    /**
     * Converts one value to a typed literal for unsigned_long term/terms queries.
     * Mirrors {@code NumberFieldMapper.NumberType.UNSIGNED_LONG.termQuery}: fractional values
     * can never match a whole-number document value, so return {@code Optional.empty()} for them
     * (match-none semantics). Negative values also return empty.
     */
    @Override
    public Optional<RexNode> toTermLiteral(Object value, RelDataTypeField field, ConversionContext ctx) throws ConversionException {
        Long unsignedValue = parseUnsignedLongTerm(value, field.getName());
        if (unsignedValue == null) {
            return Optional.empty();
        }
        long longVal = unsignedValue;
        RexNode literal = ctx.getRexBuilder().makeLiteral(longVal, field.getType(), true);
        return Optional.of(literal);
    }

    // ========== UNSIGNED_LONG TERM HELPERS ==========

    /**
     * Parses and validates a term value for an unsigned_long field.
     * Values in [0, Long.MAX_VALUE] return the long; negative values and decimals signal
     * match-none (returned as null); values above Long.MAX_VALUE throw ConversionException.
     *
     * <p>Legacy behavior: {@code NumberFieldMapper.NumberType.UNSIGNED_LONG.termQuery} returns
     * MatchNoDocsQuery for values with a decimal part (2.5 cannot equal any whole-number doc),
     * and {@code termsQuery} skips such values. We mirror this by returning null.
     *
     * @return the long value, or null to signal match-none (negative or decimal input)
     * @throws ConversionException on non-numeric or above Long.MAX_VALUE
     */
    private static Long parseUnsignedLongTerm(Object value, String fieldName) throws ConversionException {
        if (value instanceof Number num) {
            // Check above Long.MAX first using BigDecimal
            if (!(num instanceof Long || num instanceof Integer || num instanceof Short || num instanceof Byte)) {
                BigDecimal bd = BigDecimal.valueOf(num.doubleValue());
                if (bd.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                    throw new ConversionException(
                        "Unsigned long term value above Long.MAX_VALUE is not representable on the DSL path "
                            + "(schema_coerce.rs UInt64→Int64 narrowing) for field '"
                            + fieldName
                            + "': "
                            + value
                    );
                }
            }
            double doubleValue = num.doubleValue();
            if (doubleValue < 0) {
                return null;
            }
            // Decimal part → match-none per legacy NumberFieldMapper.UNSIGNED_LONG.termQuery
            if (doubleValue % 1 != 0) {
                return null;
            }
            // For whole numbers, preserve exact long value
            if (num instanceof Long || num instanceof Integer || num instanceof Short || num instanceof Byte) {
                return num.longValue();
            }
            return (long) doubleValue;
        }

        // String value
        String str = value.toString();
        BigDecimal bd;
        try {
            bd = new BigDecimal(str);
        } catch (NumberFormatException e) {
            throw new ConversionException("Non-numeric term value for unsigned_long field '" + fieldName + "': " + value);
        }

        if (bd.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
            throw new ConversionException(
                "Unsigned long term value above Long.MAX_VALUE is not representable on the DSL path "
                    + "(schema_coerce.rs UInt64→Int64 narrowing) for field '"
                    + fieldName
                    + "': "
                    + value
            );
        }

        if (bd.signum() < 0) {
            return null;
        }

        // Decimal part → match-none per legacy NumberFieldMapper.UNSIGNED_LONG.termsQuery
        if (bd.stripTrailingZeros().scale() > 0) {
            return null;
        }

        return bd.longValue();
    }
}
