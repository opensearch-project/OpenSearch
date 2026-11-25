/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query.range;

import org.apache.calcite.sql.type.SqlTypeName;

import org.opensearch.dsl.converter.ConversionException;

/**
 * Numeric type-narrowing and overflow-guard helpers shared by the range bound translation path.
 */
public final class RangeBoundMath {

    private RangeBoundMath() {}

    /**
     * Returns true if the value represents a non-finite IEEE-754 value (NaN, +Infinity, -Infinity),
     * whether arriving as a raw Number (Double/Float) or as a coerced String like "NaN" or "Infinity".
     * WHY: Double.longValue() on NaN silently returns 0, producing a wrong bound of 0 on integer
     * fields; Infinity similarly truncates to Long.MIN_VALUE/MAX_VALUE. Callers must reject these
     * before entering the truncation path.
     */
    public static boolean isNonFinite(Object value) {
        if (value instanceof Double d) {
            return Double.isNaN(d) || Double.isInfinite(d);
        }
        if (value instanceof Float f) {
            return Float.isNaN(f) || Float.isInfinite(f);
        }
        if (value instanceof String s) {
            // Matches Double.parseDouble conventions: "NaN", "Infinity", "-Infinity", "+Infinity"
            return s.equalsIgnoreCase("NaN")
                || s.equalsIgnoreCase("Infinity")
                || s.equalsIgnoreCase("-Infinity")
                || s.equalsIgnoreCase("+Infinity");
        }
        return false;
    }

    /**
     * Result of a range-checked narrowing: either a narrowed Number, a match-none sentinel,
     * or a no-constraint sentinel (meaning the bound is vacuously satisfied).
     */
    public enum NarrowResult {
        /** The value was narrowed successfully; use {@code narrowedValue}. */
        OK,
        /** The bound is impossible (lower above max or upper below min); emit FALSE literal. */
        MATCH_NONE,
        /** The bound is vacuous (lower below min or upper above max); return null (no constraint). */
        NO_CONSTRAINT
    }

    /**
     * Holds the result of a checked narrowing: the disposition and (if OK) the narrowed value.
     */
    public record CheckedNarrow(NarrowResult result, Number value) {

        static CheckedNarrow ok(Number value) {
            return new CheckedNarrow(NarrowResult.OK, value);
        }

        static CheckedNarrow matchNone() {
            return new CheckedNarrow(NarrowResult.MATCH_NONE, null);
        }

        static CheckedNarrow noConstraint() {
            return new CheckedNarrow(NarrowResult.NO_CONSTRAINT, null);
        }
    }

    /**
     * Range-checked narrowing for whole-number bounds on integer-family fields.
     * For INTEGER and BIGINT, values outside the type range throw ConversionException (matching
     * legacy NumberFieldMapper IllegalArgumentException "out of range for an integer/long").
     * For TINYINT and SMALLINT, out-of-range values produce match-none or no-constraint instead
     * of throwing, because legacy produces a real query that returns zero hits rather than 400.
     *
     * <p>WHY: unchecked {@code (int) longValue} silently truncates via Java narrowing primitive
     * conversion (JLS 5.1.3), turning e.g. 2147483648L into -2147483648 and matching everything.
     *
     * @param value the long value to narrow
     * @param typeName the target SqlTypeName (TINYINT, SMALLINT, INTEGER, BIGINT)
     * @param isLower true if this is a lower bound, false if upper
     * @param fieldName field name for error messages
     * @return CheckedNarrow with disposition and narrowed value if OK
     * @throws ConversionException if INTEGER/BIGINT and value is out of range
     */
    public static CheckedNarrow narrowChecked(long value, SqlTypeName typeName, boolean isLower, String fieldName)
        throws ConversionException {
        long min = getMinValueForType(typeName);
        long max = getMaxValueForType(typeName);

        if (value > max) {
            if (typeName == SqlTypeName.INTEGER || typeName == SqlTypeName.BIGINT) {
                throw new ConversionException("Value " + value + " is out of range for " + typeName + " field '" + fieldName + "'");
            }
            // TINYINT/SMALLINT: lower above max -> match-none; upper above max -> no constraint
            return isLower ? CheckedNarrow.matchNone() : CheckedNarrow.noConstraint();
        }
        if (value < min) {
            if (typeName == SqlTypeName.INTEGER || typeName == SqlTypeName.BIGINT) {
                throw new ConversionException("Value " + value + " is out of range for " + typeName + " field '" + fieldName + "'");
            }
            // TINYINT/SMALLINT: upper below min -> match-none; lower below min -> no constraint
            return isLower ? CheckedNarrow.noConstraint() : CheckedNarrow.matchNone();
        }

        // Value is in range; narrow safely
        if (typeName == SqlTypeName.BIGINT) {
            return CheckedNarrow.ok(value);
        }
        return CheckedNarrow.ok((int) value);
    }

    /** Returns true if the SqlTypeName represents an integer-family type (not float/double/decimal). */
    public static boolean isIntegerType(SqlTypeName typeName) {
        return typeName == SqlTypeName.INTEGER
            || typeName == SqlTypeName.BIGINT
            || typeName == SqlTypeName.SMALLINT
            || typeName == SqlTypeName.TINYINT;
    }

    /**
     * Truncates a numeric value to long (floor toward zero), supporting all integer family widths.
     * Used as the base truncation before narrowing to the specific integer type.
     */
    public static long toLongValue(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0;
    }

    /**
     * Narrows a long value to Integer (for TINYINT, SMALLINT, INTEGER) or Long (for BIGINT)
     * WITHOUT range checking. Callers on the whole-number path must use
     * {@link #narrowChecked(long, SqlTypeName, boolean, String)} instead to avoid silent
     * truncation via Java narrowing primitive conversion (JLS 5.1.3).
     * Retained only for the decimal-adjust branch where overflow is already guarded.
     */
    public static Number narrowToFieldType(long value, SqlTypeName typeName) {
        if (typeName == SqlTypeName.BIGINT) {
            return value;
        }
        return (int) value;
    }

    /**
     * Returns the maximum value for the given integer-family SqlTypeName.
     * Used for overflow guard checks before incrementing truncated values.
     */
    public static long getMaxValueForType(SqlTypeName typeName) {
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
    public static long getMinValueForType(SqlTypeName typeName) {
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
}
