/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;

/**
 * Scalar functions that a backend may support in projections and expressions.
 * Used by the project rule to verify the backend can evaluate each expression
 * in the SELECT clause.
 *
 * @opensearch.internal
 */
public enum ScalarFunction {
    // String
    UPPER(SqlKind.OTHER),
    LOWER(SqlKind.OTHER),
    TRIM(SqlKind.TRIM),
    SUBSTRING(SqlKind.OTHER),
    CONCAT(SqlKind.OTHER),
    CHAR_LENGTH(SqlKind.OTHER),

    // Math
    PLUS(SqlKind.PLUS),
    MINUS(SqlKind.MINUS),
    TIMES(SqlKind.TIMES),
    DIVIDE(SqlKind.DIVIDE),
    MOD(SqlKind.MOD),
    ABS(SqlKind.OTHER),
    CEIL(SqlKind.CEIL),
    FLOOR(SqlKind.FLOOR),

    // Cast / type
    CAST(SqlKind.CAST),

    // Conditional
    CASE(SqlKind.CASE),
    COALESCE(SqlKind.COALESCE),
    NULLIF(SqlKind.NULLIF),

    // Date/time
    EXTRACT(SqlKind.EXTRACT);

    private final SqlKind sqlKind;

    ScalarFunction(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Maps a Calcite SqlKind to a ScalarFunction, or null if not recognized. Skips OTHER. */
    public static ScalarFunction fromSqlKind(SqlKind kind) {
        for (ScalarFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER) {
                return func;
            }
        }
        return null;
    }

    /** Maps a function name to a ScalarFunction. Throws if not recognized. */
    public static ScalarFunction fromNameOrError(String name) {
        try {
            return valueOf(name);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unrecognized scalar function [" + name + "]", e);
        }
    }
}
