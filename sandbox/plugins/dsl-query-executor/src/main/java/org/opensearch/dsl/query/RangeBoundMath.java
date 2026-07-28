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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.ScaledFloatType;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;

import java.math.BigDecimal;

/**
 * Math helpers for decimal truncation, overflow guards, and integer-type narrowing
 * used by {@link RangeQueryTranslator} when processing range bounds on integer-typed fields.
 * <p>
 * Replicates {@code NumberFieldMapper.NumberType.INTEGER.rangeQuery} truncate+adjust semantics.
 */
final class RangeBoundMath {

    private RangeBoundMath() {}

    /** Returns true if the SqlTypeName represents an integer-family type (not float/double/decimal). */
    static boolean isIntegerType(SqlTypeName typeName) {
        return typeName == SqlTypeName.INTEGER
            || typeName == SqlTypeName.BIGINT
            || typeName == SqlTypeName.SMALLINT
            || typeName == SqlTypeName.TINYINT;
    }

    /**
     * Returns true if the numeric value has a non-zero fractional part.
     * Mirrors legacy NumberFieldMapper.hasDecimalPart.
     * Accepts raw Number instances and CoercedNumber wrappers (from string-to-number coercion).
     */
    static boolean hasDecimalPart(Object value) {
        if (value instanceof RangeQueryTranslator.CoercedNumber) {
            double d = ((RangeQueryTranslator.CoercedNumber) value).value.doubleValue();
            return d % 1 != 0;
        }
        if (value instanceof Number) {
            double d = ((Number) value).doubleValue();
            return d % 1 != 0;
        }
        return false;
    }

    /**
     * Returns the signum (-1, 0, or 1) of a numeric value.
     * Mirrors legacy NumberFieldMapper.signum.
     * Accepts raw Number instances and CoercedNumber wrappers.
     */
    static double signum(Object value) {
        if (value instanceof RangeQueryTranslator.CoercedNumber) {
            return Math.signum(((RangeQueryTranslator.CoercedNumber) value).value.doubleValue());
        }
        if (value instanceof Number) {
            return Math.signum(((Number) value).doubleValue());
        }
        return 0;
    }

    /**
     * Truncates a numeric value to long (floor toward zero), supporting all integer family widths.
     * Used as the base truncation before narrowing to the specific integer type.
     * Accepts raw Number instances and CoercedNumber wrappers.
     */
    static long toLongValue(Object value) {
        if (value instanceof RangeQueryTranslator.CoercedNumber) {
            return ((RangeQueryTranslator.CoercedNumber) value).value.longValue();
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0;
    }

    /**
     * Narrows a long value to the appropriate Java type for the given SqlTypeName.
     * INTEGER/SMALLINT/TINYINT produce Integer; BIGINT produces Long.
     */
    static Number narrowToFieldType(long value, SqlTypeName typeName) {
        if (typeName == SqlTypeName.BIGINT) {
            return value;
        }
        return (int) value;
    }

    /**
     * Returns the maximum value for the given integer-family SqlTypeName.
     * Used for overflow guard checks before incrementing truncated values.
     */
    static long getMaxValueForType(SqlTypeName typeName) {
        switch (typeName) {
            case BIGINT:
                return Long.MAX_VALUE;
            case INTEGER:
                return Integer.MAX_VALUE;
            case SMALLINT:
                return Short.MAX_VALUE;
            case TINYINT:
                return Byte.MAX_VALUE;
            default:
                return Long.MAX_VALUE;
        }
    }

    /**
     * Returns the minimum value for the given integer-family SqlTypeName.
     * Used for overflow guard checks before decrementing truncated values.
     */
    static long getMinValueForType(SqlTypeName typeName) {
        switch (typeName) {
            case BIGINT:
                return Long.MIN_VALUE;
            case INTEGER:
                return Integer.MIN_VALUE;
            case SMALLINT:
                return Short.MIN_VALUE;
            case TINYINT:
                return Byte.MIN_VALUE;
            default:
                return Long.MIN_VALUE;
        }
    }

    /** Returns true if the SqlTypeName represents a numeric type. */
    static boolean isNumericType(SqlTypeName typeName) {
        return typeName == SqlTypeName.INTEGER
            || typeName == SqlTypeName.BIGINT
            || typeName == SqlTypeName.SMALLINT
            || typeName == SqlTypeName.TINYINT
            || typeName == SqlTypeName.DOUBLE
            || typeName == SqlTypeName.FLOAT
            || typeName == SqlTypeName.REAL
            || typeName == SqlTypeName.DECIMAL;
    }

    /**
     * Scales a bound value for a scaled_float field per
     * {@code ScaledFloatFieldMapper.ScaledFloatFieldType.rangeQuery} semantics:
     * {@code Math.round(doubleValue * scalingFactor)}.
     *
     * @throws ConversionException if the value is non-numeric, non-finite, or the scaled result overflows Long
     */
    static long scaleBound(Object bound, ScaledFloatType sft, String fieldName) throws ConversionException {
        double doubleValue = parseFiniteDouble(bound, fieldName);
        double scaled = doubleValue * sft.getScalingFactor();
        if (Double.isNaN(scaled) || Double.isInfinite(scaled) || scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
            throw new ConversionException(
                "Scaled value overflows Long range for field '" + fieldName + "': " + bound + " * " + sft.getScalingFactor()
            );
        }
        return Math.round(scaled);
    }

    /**
     * Scales a term/terms value to a long for scaled_float equality queries.
     * Shared by {@link TermQueryTranslator} and {@link TermsQueryTranslator}.
     *
     * @throws ConversionException if the value is non-numeric, non-finite, or overflows Long after scaling
     */
    static long scaleToLong(Object value, double factor, String fieldName) throws ConversionException {
        double doubleValue = parseFiniteDouble(value, fieldName);
        double scaled = doubleValue * factor;
        if (Double.isNaN(scaled) || Double.isInfinite(scaled) || scaled > Long.MAX_VALUE || scaled < Long.MIN_VALUE) {
            throw new ConversionException("Scaled value overflows Long range for field '" + fieldName + "': " + value + " * " + factor);
        }
        return Math.round(scaled);
    }

    /**
     * Parses an object to a finite double, rejecting NaN and Infinity.
     *
     * @throws ConversionException if the value is non-numeric or non-finite
     */
    private static double parseFiniteDouble(Object value, String fieldName) throws ConversionException {
        double doubleValue;
        if (value instanceof RangeQueryTranslator.CoercedNumber) {
            doubleValue = ((RangeQueryTranslator.CoercedNumber) value).value.doubleValue();
        } else if (value instanceof Number) {
            doubleValue = ((Number) value).doubleValue();
        } else {
            try {
                doubleValue = Double.parseDouble(value.toString());
            } catch (NumberFormatException e) {
                throw new ConversionException("Non-numeric term value for scaled_float field '" + fieldName + "': " + value);
            }
        }
        if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
            throw new ConversionException("Non-finite value for scaled_float field '" + fieldName + "': " + value);
        }
        return doubleValue;
    }

    // ========== UNSIGNED_LONG BOUND TRANSLATION ==========

    /**
     * Translates a single range bound for an unsigned_long field into a comparison RexNode.
     * Mirrors legacy {@code NumberFieldMapper.unsignedLongRangeQuery} semantics.
     *
     * @return RexNode comparison, literal false for match-none, or null if value is null
     * @throws ConversionException on non-numeric values or values above Long.MAX_VALUE
     */
    static RexNode translateUnsignedLongBound(
        Object value,
        boolean isLower,
        boolean inclusive,
        RelDataTypeField field,
        ConversionContext ctx
    ) throws ConversionException {
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
     * @throws ConversionException if non-numeric or above Long.MAX_VALUE
     */
    static double parseUnsignedLongBound(Object value, String fieldName) throws ConversionException {
        if (value instanceof RangeQueryTranslator.CoercedNumber cn) {
            Number num = cn.value;
            checkAboveLongMax(num, value, fieldName);
            return num.doubleValue();
        }
        if (value instanceof Number num) {
            checkAboveLongMax(num, value, fieldName);
            return num.doubleValue();
        }
        // String value: parse and check via BigDecimal for precision
        String str = value.toString();
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
        if (value instanceof RangeQueryTranslator.CoercedNumber cn) {
            if (cn.value instanceof Double || cn.value instanceof Float) {
                return (long) cn.value.doubleValue();
            }
            return cn.value.longValue();
        }
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
    static Long parseUnsignedLongTerm(Object value, String fieldName) throws ConversionException {
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
