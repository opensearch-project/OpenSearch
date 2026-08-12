/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query.range;

import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Generic numeric predicates and type-narrowing helpers shared by the range bound translation path.
 */
public final class RangeBoundMath {

    private RangeBoundMath() {}

    /** Returns true if the SqlTypeName represents an integer-family type (not float/double/decimal). */
    public static boolean isIntegerType(SqlTypeName typeName) {
        return typeName == SqlTypeName.INTEGER
            || typeName == SqlTypeName.BIGINT
            || typeName == SqlTypeName.SMALLINT
            || typeName == SqlTypeName.TINYINT;
    }

    /**
     * Returns true if the numeric value has a non-zero fractional part.
     * Mirrors legacy NumberFieldMapper.hasDecimalPart.
     */
    public static boolean hasDecimalPart(Object value) {
        if (value instanceof Number) {
            double d = ((Number) value).doubleValue();
            return d % 1 != 0;
        }
        return false;
    }

    /**
     * Returns the signum (-1, 0, or 1) of a numeric value.
     * Mirrors legacy NumberFieldMapper.signum.
     */
    public static double signum(Object value) {
        if (value instanceof Number) {
            return Math.signum(((Number) value).doubleValue());
        }
        return 0;
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
     * Narrows a long value to the appropriate Java type for the given SqlTypeName.
     * INTEGER/SMALLINT/TINYINT produce Integer; BIGINT produces Long.
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
