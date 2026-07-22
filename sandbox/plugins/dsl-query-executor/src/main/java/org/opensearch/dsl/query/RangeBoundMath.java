/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.sql.type.SqlTypeName;

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
}
